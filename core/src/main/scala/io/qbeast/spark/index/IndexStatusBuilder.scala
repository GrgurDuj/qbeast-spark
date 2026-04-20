/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.spark.index

import io.qbeast.core.model._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset

import scala.collection.immutable.SortedMap

/**
 * Builds the index status from a given snapshot and revision
 *
 * @param qbeastSnapshot
 *   the QbeastSnapshot
 * @param revision
 *   the revision
 */
class IndexStatusBuilder(qbeastSnapshot: QbeastSnapshot, revision: Revision)
    extends Serializable
    with StagingUtils {

  def build(): IndexStatus = {
    val cubeStatus =
      if (isStaging(revision)) stagingCubeStatuses
      else indexCubeStatuses
    IndexStatus(revision = revision, cubesStatuses = cubeStatus)
  }

  /**
   * Builds the index status by invalidating only the cubes affected by removed files,
   * then applying new appends incrementally.
   *
   * @param base
   *   the baseline IndexStatus from a Parquet snapshot
   * @param deltaAppends
   *   files added since the snapshot
   * @param deltaRemoves
   *   paths of files removed since the snapshot
   * @return
   *   the updated IndexStatus
   */
  def buildTargeted(
      base: IndexStatus,
      deltaAppends: Dataset[IndexFile],
      deltaRemoves: Seq[String]): IndexStatus = {
    if (deltaRemoves.isEmpty) return buildIncremental(base, deltaAppends)

    import deltaAppends.sparkSession.implicits._
    val removedPaths = deltaRemoves.toSet

    // Identify cubes touched by the removed files
    val oldIndexFiles = qbeastSnapshot.loadIndexFiles(revision.revisionID)
    val invalidatedCubeStrs = oldIndexFiles
      .filter(row => removedPaths.contains(row.path))
      .flatMap(_.blocks.map(_.cubeId.string))
      .distinct()
      .collect()
      .toSet

    val invalidatedCubes = invalidatedCubeStrs.map(revision.createCubeId)

    // Fall back to full rebuild when too many cubes are affected
    if (invalidatedCubes.isEmpty ||
      invalidatedCubes.size > Math.max(10, base.cubesStatuses.size * 0.4)) {
      return build()
    }

    // Drop invalidated cubes and keep only non-removed files that overlap them
    var activeStatuses = base.cubesStatuses -- invalidatedCubes
    val activeFiles = oldIndexFiles
      .filter(row => !removedPaths.contains(row.path))
      .filter(row => row.blocks.exists(b => invalidatedCubeStrs.contains(b.cubeId.string)))

    // Recalculate only the affected cubes and merge back
    val recalculatedCubes = cubeStatusesFromFiles(activeFiles)
      .filter { case (cubeId, _) => invalidatedCubes.contains(cubeId) }
    activeStatuses = activeStatuses ++ recalculatedCubes

    // Apply new appends on top
    buildIncremental(IndexStatus(revision, activeStatuses), deltaAppends)
  }

  private def cubeStatusesFromFiles(files: Dataset[IndexFile]): Map[CubeId, CubeStatus] = {
    val desiredCubeSize = revision.desiredCubeSize
    import files.sparkSession.implicits._
    
    files
      .flatMap(_.blocks)
      .groupBy($"cubeId")
      .agg(
        min($"maxWeight.value").as("maxWeightInt"), 
        sum($"elementCount").as("elementCount")
      )
      .withColumn(
        "normalizedWeight",
        when(
          $"maxWeightInt" < Weight.MaxValueColumn,
          NormalizedWeight.fromWeightColumn($"maxWeightInt")
        ).otherwise(NormalizedWeight.fromColumns(lit(desiredCubeSize), $"elementCount"))
      )
      .withColumn("maxWeight", struct($"maxWeightInt".as("value")))
      .drop($"maxWeightInt")
      .as[CubeStatus]
      .collect()
      .map(cs => (cs.cubeId, cs))
      .toMap
  }

  /**
   * Merges a base IndexStatus with index files added after the base snapshot.
   *
   * @param base
   *   the baseline IndexStatus
   * @param deltaFiles
   *   files added since the baseline
   * @return
   *   the merged IndexStatus
   */
  def buildIncremental(
      base: IndexStatus,
      deltaFiles: Dataset[IndexFile]): IndexStatus = {
    val deltaCubeStatuses = cubeStatusesFromFiles(deltaFiles)
    if (deltaCubeStatuses.isEmpty) return base

    val desiredCubeSize = revision.desiredCubeSize
    var activeStatuses = base.cubesStatuses
    
    deltaCubeStatuses.foreach { case (cubeId, deltaStatus) =>
      val baseStatusOpt = activeStatuses.get(cubeId)
      
      val mergedCount = baseStatusOpt.map(_.elementCount).getOrElse(0L) + deltaStatus.elementCount
      
      val mergedWeight = Weight.min(
        baseStatusOpt.map(_.maxWeight).getOrElse(Weight.MaxValue),
        deltaStatus.maxWeight
      )
        
      val normalizedWeight =
        if (mergedWeight < Weight.MaxValue) mergedWeight.fraction
        else NormalizedWeight(desiredCubeSize, mergedCount)
        
      activeStatuses = activeStatuses.updated(
        cubeId, 
        CubeStatus(cubeId, mergedWeight, normalizedWeight, mergedCount)
      )
    }
    
    IndexStatus(revision, activeStatuses)
  }

  def stagingCubeStatuses: SortedMap[CubeId, CubeStatus] = {
    // All staging files belong to the root.
    // All staging blocks have elementCount=0 as no qbeast tags are present.
    val root = revision.createCubeIdRoot()
    SortedMap(root -> CubeStatus(root, Weight.MaxValue, Weight.MaxValue.fraction, 0L))
  }

  /**
   * Returns the index state for the given space revision
   * @return
   *   Dataset containing cube information
   */
  def indexCubeStatuses: SortedMap[CubeId, CubeStatus] = {
    val builder = SortedMap.newBuilder[CubeId, CubeStatus]
    val desiredCubeSize = revision.desiredCubeSize
    val revisionAddFiles: Dataset[IndexFile] =
      qbeastSnapshot.loadIndexFiles(revision.revisionID)

    import revisionAddFiles.sparkSession.implicits._
    val cubeStatuses = revisionAddFiles
      .flatMap(_.blocks)
      .groupBy($"cubeId")
      .agg(min($"maxWeight.value").as("maxWeightInt"), sum($"elementCount").as("elementCount"))
      .withColumn(
        "normalizedWeight",
        when(
          $"maxWeightInt" < Weight.MaxValueColumn,
          NormalizedWeight.fromWeightColumn($"maxWeightInt"))
          .otherwise(NormalizedWeight.fromColumns(lit(desiredCubeSize), $"elementCount")))
      .withColumn("maxWeight", struct($"maxWeightInt".as("value")))
      .drop($"maxWeightInt")
      .as[CubeStatus]
      .collect()

    cubeStatuses.foreach(cs => builder += (cs.cubeId -> cs))

    builder.result()
  }

}
