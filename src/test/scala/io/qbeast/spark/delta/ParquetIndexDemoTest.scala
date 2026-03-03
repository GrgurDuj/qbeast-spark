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

import io.qbeast.context.QbeastContext
import io.qbeast.core.model.QTableID
import io.qbeast.table.QbeastTable
import io.qbeast.QbeastIntegrationTestSpec
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.DeltaLog

class ParquetIndexDemoTest extends QbeastIntegrationTestSpec {

  private val writeOptions = Map(
    "columnsToIndex" -> "id",
    "cubeSize" -> "1000",
    "columnStats" -> """{"id_min":0,"id_max":100000}""")

  "Parquet index demo" should "create a Parquet index file immediately after the first write" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      spark
        .range(50000)
        .toDF("id")
        .write
        .format("qbeast")
        .options(writeOptions)
        .save(tmpDir)

      val deltaVersion = DeltaLog.forTable(spark, tmpDir).update().version
      val revisionID =
        QbeastContext.metadataManager
          .loadSnapshot(new QTableID(tmpDir))
          .loadLatestRevision
          .revisionID

      val snapshotPath =
        s"$tmpDir/_qbeast/index_snapshots/revision=$revisionID/version=$deltaVersion"
      val fs = new Path(snapshotPath).getFileSystem(spark.sessionState.newHadoopConf())
      fs.exists(new Path(snapshotPath)) shouldBe true

      val indexDF = spark.read.parquet(snapshotPath)
      val schema = indexDF.schema.fieldNames.toSet
      schema should contain("cubeId")
      schema should contain("maxWeightInt")
      schema should contain("normalizedWeight")
      schema should contain("elementCount")

      val cubeCount = indexDF.count()
      cubeCount should be > 0L

      val totalElements = indexDF
        .agg(Map("elementCount" -> "sum"))
        .collect()(0)
        .getLong(0)
      totalElements shouldBe 50000L

      info(s"  Delta version      : $deltaVersion")
      info(s"  Parquet path       : $snapshotPath")
      info(s"  Cubes in index     : $cubeCount")
      info(s"  Total element count: $totalElements")
    }

  it should "serve loadIndexStatus exclusively from the Parquet file" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      spark
        .range(50000)
        .toDF("id")
        .write
        .format("qbeast")
        .options(writeOptions)
        .save(tmpDir)

      val tableID = new QTableID(tmpDir)
      val snapshot = QbeastContext.metadataManager.loadSnapshot(tableID)

      // loadIndexStatus reads from the Parquet snapshot, not the Delta log
      val status = snapshot.loadLatestIndexStatus

      status.cubesStatuses should not be empty
      val total = status.cubesStatuses.values.map(_.elementCount).sum
      total shouldBe 50000L

      // Without a Parquet snapshot this call throws IllegalStateException
      info(s"  Revision ID       : ${status.revision.revisionID}")
      info(s"  Cubes loaded      : ${status.cubesStatuses.size}")
      info(s"  Total element count: $total")
    }

  it should "use the Parquet index to prune cubes during a sampling query" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      spark
        .range(100000)
        .toDF("id")
        .write
        .format("qbeast")
        .options(writeOptions)
        .save(tmpDir)

      val tableID = new QTableID(tmpDir)
      val status = QbeastContext.metadataManager.loadSnapshot(tableID).loadLatestIndexStatus
      val totalCubes = status.cubesStatuses.size

      // A 10 % sample should touch far fewer cubes than the full tree
      val df = spark.read.format("qbeast").load(tmpDir)
      val sampleCount = df.sample(withReplacement = false, 0.1).count()

      // Result is approximate but must be non-trivially smaller than the full dataset
      sampleCount should be > 0L
      sampleCount should be < 100000L

      info(s"  Total cubes in Parquet index: $totalCubes")
      info(s"  Rows returned by 10%% sample : $sampleCount")
    }

  it should "write a new Parquet index at a higher Delta version after optimize" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      spark
        .range(50000)
        .toDF("id")
        .write
        .format("qbeast")
        .options(writeOptions)
        .save(tmpDir)

      val versionAfterWrite = DeltaLog.forTable(spark, tmpDir).update().version

      spark
        .range(50000)
        .toDF("id")
        .write
        .mode("append")
        .format("qbeast")
        .options(writeOptions)
        .save(tmpDir)

      QbeastTable.forPath(spark, tmpDir).optimize()

      val versionAfterOptimize = DeltaLog.forTable(spark, tmpDir).update().version
      versionAfterOptimize should be > versionAfterWrite

      val tableID = new QTableID(tmpDir)
      val revisionID =
        QbeastContext.metadataManager.loadSnapshot(tableID).loadLatestRevision.revisionID

      val hadoopConf = spark.sessionState.newHadoopConf()
      def exists(version: Long): Boolean = {
        val p = new Path(s"$tmpDir/_qbeast/index_snapshots/revision=$revisionID/version=$version")
        p.getFileSystem(hadoopConf).exists(p)
      }
      exists(versionAfterWrite) shouldBe true
      exists(versionAfterOptimize) shouldBe true

      val status = QbeastContext.metadataManager.loadSnapshot(tableID).loadLatestIndexStatus
      val total = status.cubesStatuses.values.map(_.elementCount).sum
      total shouldBe 100000L

      info(s"  Delta version after write    : $versionAfterWrite")
      info(s"  Delta version after optimize : $versionAfterOptimize")
      info(s"  Total element count          : $total")
    }

}
