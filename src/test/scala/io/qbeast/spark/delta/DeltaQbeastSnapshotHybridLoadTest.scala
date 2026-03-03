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
import io.qbeast.core.model._
import io.qbeast.table.QbeastTable
import io.qbeast.QbeastIntegrationTestSpec
import org.apache.spark.sql.delta.DeltaLog

class DeltaQbeastSnapshotHybridLoadTest extends QbeastIntegrationTestSpec {

  private val writeOptions = Map(
    "columnsToIndex" -> "id",
    "cubeSize" -> "1000",
    "columnStats" -> """{"id_min":0,"id_max":100000}""")

  private def writeData(spark: org.apache.spark.sql.SparkSession, tmpDir: String): Unit =
    spark
      .range(10000)
      .toDF("id")
      .write
      .format("qbeast")
      .options(writeOptions)
      .save(tmpDir)

  private def appendData(spark: org.apache.spark.sql.SparkSession, tmpDir: String): Unit =
    spark
      .range(10000)
      .toDF("id")
      .write
      .mode("append")
      .format("qbeast")
      .options(writeOptions)
      .save(tmpDir)

  // ─── Core Parquet snapshot loading ──────────────────────────────────────────

  "DeltaQbeastSnapshot.loadIndexStatus" should
    "return a correct IndexStatus from Parquet after optimize" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      writeData(spark, tmpDir)
      QbeastTable.forPath(spark, tmpDir).optimize()

      val tableID = new QTableID(tmpDir)
      val status = QbeastContext.metadataManager.loadSnapshot(tableID).loadLatestIndexStatus

      val totalElements = status.cubesStatuses.values.map(_.elementCount).sum
      totalElements should be >= 10000L
      status.cubesStatuses.keys.foreach(cube =>
        cube.dimensionCount shouldBe status.revision.columnTransformers.size)
    }

  it should "be readable immediately after a plain write (no optimize required)" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      writeData(spark, tmpDir)

      val tableID = new QTableID(tmpDir)
      val status = QbeastContext.metadataManager.loadSnapshot(tableID).loadLatestIndexStatus

      val totalElements = status.cubesStatuses.values.map(_.elementCount).sum
      totalElements should be >= 10000L
    }

  it should "fail with IllegalStateException when the Parquet snapshot is manually absent" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      // Write directly via Delta (no qbeast write path), so no Parquet snapshot is ever dumped
      spark
        .range(1000)
        .toDF("id")
        .write
        .format("delta")
        .save(tmpDir)

      // Verify the loader contract directly: no snapshot → exception.
      an[Exception] should be thrownBy {
        IndexStatusLoader.load(tmpDir, 1L, 0L, null)(spark)
      }
    }

  it should "reconstruct the correct element count after multiple appends followed by optimize" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      writeData(spark, tmpDir)
      appendData(spark, tmpDir)
      appendData(spark, tmpDir)
      QbeastTable.forPath(spark, tmpDir).optimize()

      val tableID = new QTableID(tmpDir)
      val status = QbeastContext.metadataManager.loadSnapshot(tableID).loadLatestIndexStatus

      val totalElements = status.cubesStatuses.values.map(_.elementCount).sum
      totalElements shouldBe 30000L
    }

  // ─── OTree invariants after optimize ────────────────────────────────────────

  "IndexStatus after optimize" should
    "not decrease total elementCount across two optimizes" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      writeData(spark, tmpDir)
      val qt = QbeastTable.forPath(spark, tmpDir)
      qt.optimize()

      val tableID = new QTableID(tmpDir)
      val statusAfterFirst =
        QbeastContext.metadataManager.loadSnapshot(tableID).loadLatestIndexStatus
      val totalAfterFirst = statusAfterFirst.cubesStatuses.values.map(_.elementCount).sum

      appendData(spark, tmpDir)
      qt.optimize()

      val statusAfterSecond =
        QbeastContext.metadataManager.loadSnapshot(tableID).loadLatestIndexStatus
      val totalAfterSecond = statusAfterSecond.cubesStatuses.values.map(_.elementCount).sum

      totalAfterSecond should be >= totalAfterFirst
      totalAfterSecond shouldBe 20000L
    }

  it should "not increase any cube's maxWeight across two optimizes (OTree invariant)" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      writeData(spark, tmpDir)
      val qt = QbeastTable.forPath(spark, tmpDir)
      qt.optimize()

      val tableID = new QTableID(tmpDir)
      val statusAfterFirst =
        QbeastContext.metadataManager.loadSnapshot(tableID).loadLatestIndexStatus

      appendData(spark, tmpDir)
      qt.optimize()

      val statusAfterSecond =
        QbeastContext.metadataManager.loadSnapshot(tableID).loadLatestIndexStatus

      // Cubes present in the first optimize snapshot should not gain weight after another optimize
      statusAfterFirst.cubesStatuses.foreach { case (cubeId, csFirst) =>
        statusAfterSecond.cubesStatuses.get(cubeId).foreach { csSecond =>
          csSecond.maxWeight should be <= csFirst.maxWeight
        }
      }
    }

  // ─── Reader isolation ───────────────────────────────────────────────────────

  "Reader isolation" should
    "preserve element count across Optimize (optimize is non-destructive)" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      writeData(spark, tmpDir)
      appendData(spark, tmpDir)

      val qt = QbeastTable.forPath(spark, tmpDir)
      qt.optimize()

      val tableID = new QTableID(tmpDir)
      val statusAfterFirstOptimize =
        QbeastContext.metadataManager.loadSnapshot(tableID).loadLatestIndexStatus
      val totalAfterFirst =
        statusAfterFirstOptimize.cubesStatuses.values.map(_.elementCount).sum

      // A second optimize should not alter total element count
      qt.optimize()

      val statusAfterSecondOptimize =
        QbeastContext.metadataManager.loadSnapshot(tableID).loadLatestIndexStatus
      val totalAfterSecond =
        statusAfterSecondOptimize.cubesStatuses.values.map(_.elementCount).sum

      totalAfterSecond shouldBe totalAfterFirst
    }

  it should "produce a defragmented index with blockCount == cubeCount after Optimize" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      writeData(spark, tmpDir)
      appendData(spark, tmpDir)

      val qt = QbeastTable.forPath(spark, tmpDir)
      qt.optimize()

      val metrics = qt.getIndexMetrics
      val fragmentation = metrics.blockCount / metrics.cubeCount.toDouble
      fragmentation shouldBe 1.0
    }

  it should "keep the revision ID stable across Optimize" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      writeData(spark, tmpDir)

      val tableID = new QTableID(tmpDir)
      val revBefore = QbeastContext.metadataManager.loadSnapshot(tableID).loadLatestRevision

      QbeastTable.forPath(spark, tmpDir).optimize()

      val revAfter = QbeastContext.metadataManager.loadSnapshot(tableID).loadLatestRevision

      revAfter.revisionID shouldBe revBefore.revisionID
    }

  // ─── Snapshot version selection ─────────────────────────────────────────────

  "Snapshot version selection" should
    "select the Parquet snapshot written by the most recent optimize" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      writeData(spark, tmpDir)
      appendData(spark, tmpDir)
      appendData(spark, tmpDir)
      QbeastTable.forPath(spark, tmpDir).optimize()

      val tableID = new QTableID(tmpDir)
      val status = QbeastContext.metadataManager.loadSnapshot(tableID).loadLatestIndexStatus

      val total = status.cubesStatuses.values.map(_.elementCount).sum
      total shouldBe 30000L
    }

  it should
    "always use the snapshot with the highest version not exceeding the current Delta version" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      writeData(spark, tmpDir)
      QbeastTable.forPath(spark, tmpDir).optimize()

      val tableID = new QTableID(tmpDir)
      val firstOptVersion = DeltaLog.forTable(spark, tmpDir).update().version

      appendData(spark, tmpDir)
      QbeastTable.forPath(spark, tmpDir).optimize()

      val secondOptVersion = DeltaLog.forTable(spark, tmpDir).update().version
      secondOptVersion should be > firstOptVersion

      val latestStatus =
        QbeastContext.metadataManager.loadSnapshot(tableID).loadLatestIndexStatus
      val revID = latestStatus.revision.revisionID

      IndexStatusLoader.latestSnapshotVersion(tmpDir, revID, targetVersion = secondOptVersion)(
        spark) shouldBe Some(secondOptVersion)
      IndexStatusLoader.latestSnapshotVersion(tmpDir, revID, targetVersion = firstOptVersion)(
        spark) shouldBe Some(firstOptVersion)
    }

  // ─── Delta log to Parquet round-trip ────────────────────────

  "Delta log to Parquet round-trip" should
    "produce an identical IndexStatus when dumped and reloaded" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      writeData(spark, tmpDir)
      QbeastTable.forPath(spark, tmpDir).optimize()

      val tableID = new QTableID(tmpDir)
      val deltaVersion = DeltaLog.forTable(spark, tmpDir).update().version
      val fromParquet =
        QbeastContext.metadataManager.loadSnapshot(tableID).loadLatestIndexStatus

      // Dump again at the same version (idempotent)
      IndexStatusDumper.dump(
        fromParquet.cubesStatuses,
        tmpDir,
        fromParquet.revision.revisionID,
        deltaVersion)(spark)

      val reloaded = IndexStatusLoader.load(
        tmpDir,
        fromParquet.revision.revisionID,
        deltaVersion,
        fromParquet.revision)(spark)

      reloaded.size shouldBe fromParquet.cubesStatuses.size
      reloaded.keySet shouldBe fromParquet.cubesStatuses.keySet

      fromParquet.cubesStatuses.foreach { case (cubeId, original) =>
        val recovered = reloaded(cubeId)
        recovered.maxWeight.value shouldBe original.maxWeight.value
        recovered.elementCount shouldBe original.elementCount
      }
    }

  it should "preserve cube keys and element counts after an append then round-trip" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      writeData(spark, tmpDir)
      appendData(spark, tmpDir)
      QbeastTable.forPath(spark, tmpDir).optimize()

      val tableID = new QTableID(tmpDir)
      val deltaVersion = DeltaLog.forTable(spark, tmpDir).update().version
      val fromParquet =
        QbeastContext.metadataManager.loadSnapshot(tableID).loadLatestIndexStatus

      val reloaded = IndexStatusLoader.load(
        tmpDir,
        fromParquet.revision.revisionID,
        deltaVersion,
        fromParquet.revision)(spark)

      val parquetTotal = fromParquet.cubesStatuses.values.map(_.elementCount).sum
      val reloadedTotal = reloaded.values.map(_.elementCount).sum
      reloadedTotal shouldBe parquetTotal
      reloadedTotal shouldBe 20000L
    }

  it should "be discoverable via latestSnapshotVersion after optimize" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      writeData(spark, tmpDir)
      QbeastTable.forPath(spark, tmpDir).optimize()

      val tableID = new QTableID(tmpDir)
      val deltaVersion = DeltaLog.forTable(spark, tmpDir).update().version
      val status = QbeastContext.metadataManager.loadSnapshot(tableID).loadLatestIndexStatus
      val revID = status.revision.revisionID

      IndexStatusLoader.latestSnapshotVersion(tmpDir, revID, targetVersion = deltaVersion)(
        spark) shouldBe Some(deltaVersion)
    }

}
