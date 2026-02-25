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
import io.qbeast.core.model.RevisionID
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.SortedMap

/**
 * Persists an IndexStatus snapshot to Parquet under
 * `<tableBasePath>/_qbeast/index_snapshots/revision=<id>/version=<ver>/`.
 * Writes are idempotent (SaveMode.Overwrite).
 */
object IndexStatusDumper {

  private[delta] def snapshotPath(
      tableBasePath: String,
      revisionID: RevisionID,
      deltaVersion: Long): String =
    s"$tableBasePath/_qbeast/index_snapshots/revision=$revisionID/version=$deltaVersion"

  /**
   * Writes cube statuses for the given revision and Delta version to Parquet.
   */
  def dump(
      cubesStatuses: SortedMap[CubeId, CubeStatus],
      tableBasePath: String,
      revisionID: RevisionID,
      deltaVersion: Long)(implicit spark: SparkSession): Unit = {

    import spark.implicits._

    val rows = cubesStatuses.toSeq.map { case (cubeId, status) =>
      (
        cubeId.string,
        status.maxWeight.value,
        status.normalizedWeight,
        status.elementCount)
    }

    val df = rows
      .toDF("cubeId", "maxWeightInt", "normalizedWeight", "elementCount")

    df.write
      .mode(SaveMode.Overwrite)
      .parquet(snapshotPath(tableBasePath, revisionID, deltaVersion))
  }
}
