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
package io.qbeast.spark.delta

import io.qbeast.core.model.CubeId
import io.qbeast.core.model.CubeStatus
import io.qbeast.core.model.Revision
import io.qbeast.core.model.RevisionID
import io.qbeast.core.model.Weight
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.SortedMap
import scala.util.matching.Regex

/**
 * Loads an IndexStatus snapshot previously written by IndexStatusDumper.
 */
object IndexStatusLoader {

  private val VersionDirPattern: Regex = "version=(\\d+)".r

  private[delta] def revisionDir(tableBasePath: String, revisionID: RevisionID): String =
    s"$tableBasePath/_qbeast/index_snapshots/revision=$revisionID"

  /**
   * Returns the highest persisted snapshot version <= targetVersion, or None.
   */
  def latestSnapshotVersion(
      tableBasePath: String,
      revisionID: RevisionID,
      targetVersion: Long)(implicit spark: SparkSession): Option[Long] = {

    val hadoopConf = spark.sessionState.newHadoopConf()
    val revPath = new Path(revisionDir(tableBasePath, revisionID))
    val fs = revPath.getFileSystem(hadoopConf)

    if (!fs.exists(revPath)) return None

    val statuses =
      try fs.listStatus(revPath)
      catch { case _: Exception => return None }

    val versions = statuses
      .flatMap { s =>
        VersionDirPattern.findFirstMatchIn(s.getPath.getName).map(_.group(1).toLong)
      }
      .filter(_ <= targetVersion)

    if (versions.isEmpty) None else Some(versions.max)
  }

  /**
   * Loads cube statuses from a Parquet snapshot.
   */
  def load(
      tableBasePath: String,
      revisionID: RevisionID,
      snapshotVersion: Long,
      revision: Revision)(implicit spark: SparkSession): SortedMap[CubeId, CubeStatus] = {

    import spark.implicits._

    val path = IndexStatusDumper.snapshotPath(tableBasePath, revisionID, snapshotVersion)

    val rows = spark.read.parquet(path).as[(String, Int, Double, Long)].collect()

    val entries = rows.map { case (cubeIdStr, maxWeightInt, normalizedWeight, elementCount) =>
      val cubeId = revision.createCubeId(cubeIdStr)
      val status = CubeStatus(cubeId, Weight(maxWeightInt), normalizedWeight, elementCount)
      cubeId -> status
    }

    SortedMap(entries: _*)
  }
}
