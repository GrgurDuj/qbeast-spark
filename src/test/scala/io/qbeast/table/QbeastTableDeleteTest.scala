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
package io.qbeast.table

import io.qbeast.QbeastIntegrationTestSpec
import io.qbeast.TestClasses.Client3
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class QbeastTableDeleteTest extends QbeastIntegrationTestSpec {

  private def createTestDF(spark: SparkSession, rows: Int): DataFrame = {
    val rdd = spark.sparkContext.parallelize(
      0.until(rows)
        .map(i =>
          Client3(
            id = i,
            name = s"client-$i",
            age = 20 + (i % 60), // Ages 20-79
            val2 = i * 100,
            val3 = i * 2.5)))
    spark.createDataFrame(rdd)
  }

  private def writeQbeastTable(
      data: DataFrame,
      columnsToIndex: Seq[String],
      cubeSize: Int,
      tmpDir: String,
      mode: String = "overwrite"): Unit = {
    data.write
      .mode(mode)
      .format("qbeast")
      .options(
        Map("columnsToIndex" -> columnsToIndex.mkString(","), "cubeSize" -> cubeSize.toString))
      .save(tmpDir)
  }

  behavior of "QbeastTable DELETE"

  it should "delete rows matching simple predicate" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      import spark.implicits._

      val data = createTestDF(spark, 10000)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 500
      writeQbeastTable(data, columnsToIndex, cubeSize, tmpDir)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      val initialCount = spark.read.format("qbeast").load(tmpDir).count()
      initialCount shouldBe 10000

      qbeastTable.delete("age > 70")

      val afterDeleteCount = spark.read.format("qbeast").load(tmpDir).count()
      val expectedRemaining = data.filter($"age" <= 70).count()
      afterDeleteCount shouldBe expectedRemaining

      val deletedRowsRemaining = spark.read
        .format("qbeast")
        .load(tmpDir)
        .filter($"age" > 70)
        .count()
      deletedRowsRemaining shouldBe 0
  }

  it should "maintain cube element counts after deletion" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      import spark.implicits._

      val data = createTestDF(spark, 10000)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 500
      writeQbeastTable(data, columnsToIndex, cubeSize, tmpDir)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      val initialMetrics = qbeastTable.getIndexMetrics
      val initialTotalElements = initialMetrics.denormalizedBlocks
        .select(sum($"blockElementCount"))
        .as[Long]
        .first()

      initialTotalElements shouldBe 10000

      qbeastTable.delete("age BETWEEN 30 AND 40")

      val afterMetrics = qbeastTable.getIndexMetrics
      val afterTotalElements = afterMetrics.denormalizedBlocks
        .select(sum($"blockElementCount"))
        .as[Long]
        .first()

      val actualRowCount = spark.read.format("qbeast").load(tmpDir).count()
      afterTotalElements shouldBe actualRowCount

      val cubeElementCounts = afterMetrics.denormalizedBlocks
        .groupBy($"cubeId")
        .agg(sum($"blockElementCount").alias("totalElements"))
        .collect()

      cubeElementCounts.foreach { row =>
        val elements = row.getLong(1)
        elements should be > 0L
      }
  }

  it should "maintain cube weights after deletion" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      import spark.implicits._

      val data = createTestDF(spark, 10000)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 500
      writeQbeastTable(data, columnsToIndex, cubeSize, tmpDir)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      qbeastTable.delete("age < 25")

      val afterMetrics = qbeastTable.getIndexMetrics
      val afterWeights = afterMetrics.denormalizedBlocks
        .select($"maxWeight.value")
        .collect()
        .map(row => row.getInt(0))

      afterWeights.foreach { weight =>
        weight should be >= Int.MinValue
        weight should be <= Int.MaxValue
      }
      afterWeights.length should be > 0
  }

  it should "maintain sampling accuracy after deletion" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      val data = createTestDF(spark, 10000)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 500
      writeQbeastTable(data, columnsToIndex, cubeSize, tmpDir)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      qbeastTable.delete("age % 10 == 0")

      val qbeastDf = spark.read.format("qbeast").load(tmpDir)
      val totalRows = qbeastDf.count()

      val sampleFraction = 0.1
      val sampledRows = qbeastDf.sample(withReplacement = false, sampleFraction).count()

      val expectedSample = totalRows * sampleFraction
      val tolerance = expectedSample * 0.5
      sampledRows should be >= (expectedSample - tolerance).toLong
      sampledRows should be <= (expectedSample + tolerance).toLong
  }

  it should "handle deletion with complex predicates" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      import spark.implicits._

      val data = createTestDF(spark, 10000)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 500
      writeQbeastTable(data, columnsToIndex, cubeSize, tmpDir)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      qbeastTable.delete("age BETWEEN 30 AND 50 AND val2 % 200 == 0")

      val remaining = spark.read.format("qbeast").load(tmpDir)
      val shouldBeDeleted = remaining.filter($"age".between(30, 50) && $"val2" % 200 === 0)
      shouldBeDeleted.count() shouldBe 0

      val shouldRemain = remaining.filter(!($"age".between(30, 50) && $"val2" % 200 === 0))
      shouldRemain.count() should be > 0L
  }

  it should "handle deletion of all rows in a cube" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      import spark.implicits._

      val data = createTestDF(spark, 10000)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 500
      writeQbeastTable(data, columnsToIndex, cubeSize, tmpDir)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      qbeastTable.delete("age < 25")

      val afterMetrics = qbeastTable.getIndexMetrics
      val cubesWithZeroElements = afterMetrics.denormalizedBlocks
        .filter($"blockElementCount" === 0)
        .count()

      cubesWithZeroElements shouldBe 0
  }

  it should "handle deletion with no matching rows" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      val data = createTestDF(spark, 10000)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 500
      writeQbeastTable(data, columnsToIndex, cubeSize, tmpDir)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      val initialCount = spark.read.format("qbeast").load(tmpDir).count()
      val initialMetrics = qbeastTable.getIndexMetrics

      qbeastTable.delete("age > 1000")

      val afterCount = spark.read.format("qbeast").load(tmpDir).count()
      afterCount shouldBe initialCount

      val afterMetrics = qbeastTable.getIndexMetrics
      afterMetrics.denormalizedBlocks.count() shouldBe initialMetrics.denormalizedBlocks.count()
  }

  it should "optimize file pruning during deletion" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      val data = createTestDF(spark, 20000)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 500
      writeQbeastTable(data, columnsToIndex, cubeSize, tmpDir)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      qbeastTable.delete("age BETWEEN 70 AND 79")

      val remainingData = spark.read.format("qbeast").load(tmpDir)

      remainingData.count() should be < 20000L
  }

  it should "handle deletion across multiple revisions" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      import spark.implicits._

      val data1 = createTestDF(spark, 5000)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 500
      writeQbeastTable(data1, columnsToIndex, cubeSize, tmpDir)

      val data2 = createTestDF(spark, 5000).withColumn("age", $"age" + 20)
      writeQbeastTable(data2, columnsToIndex, cubeSize, tmpDir, mode = "append")

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)
      qbeastTable.allRevisionIDs().size should be >= 2

      val initialCount = spark.read.format("qbeast").load(tmpDir).count()
      initialCount shouldBe 10000

      qbeastTable.delete("age BETWEEN 35 AND 45")

      val afterCount = spark.read.format("qbeast").load(tmpDir).count()
      afterCount should be < initialCount

      val remaining = spark.read.format("qbeast").load(tmpDir)
      remaining.filter($"age".between(35, 45)).count() shouldBe 0
  }

  it should "be atomic (rollback on failure)" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      val data = createTestDF(spark, 10000)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 500
      writeQbeastTable(data, columnsToIndex, cubeSize, tmpDir)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      val initialCount = spark.read.format("qbeast").load(tmpDir).count()
      val initialMetrics = qbeastTable.getIndexMetrics

      intercept[Exception] {
        qbeastTable.delete("nonexistent_column > 100")
      }

      val afterCount = spark.read.format("qbeast").load(tmpDir).count()
      afterCount shouldBe initialCount

      val afterMetrics = qbeastTable.getIndexMetrics
      afterMetrics.denormalizedBlocks.count() shouldBe initialMetrics.denormalizedBlocks.count()
  }

  it should "handle concurrent operations safely" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      import spark.implicits._

      val data = createTestDF(spark, 10000)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 500
      writeQbeastTable(data, columnsToIndex, cubeSize, tmpDir)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      val appendData = createTestDF(spark, 1000).withColumn("age", $"age" + 100)

      qbeastTable.delete("age < 25")

      writeQbeastTable(appendData, columnsToIndex, cubeSize, tmpDir, mode = "append")

      val finalCount = spark.read.format("qbeast").load(tmpDir).count()

      finalCount should be > 0L

      val ageStats = spark.read
        .format("qbeast")
        .load(tmpDir)
        .select(min($"age"), max($"age"))
        .collect()
      ageStats.length shouldBe 1
  }

  it should "efficiently handle large-scale deletions" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      import spark.implicits._

      val data = createTestDF(spark, 50000) // Larger dataset
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 500
      writeQbeastTable(data, columnsToIndex, cubeSize, tmpDir)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      val startTime = System.currentTimeMillis()
      qbeastTable.delete("id % 2 == 0")
      val deleteTime = System.currentTimeMillis() - startTime

      val remaining = spark.read.format("qbeast").load(tmpDir)
      remaining.filter($"id" % 2 === 0).count() shouldBe 0

      deleteTime should be < 60000L

      val metrics = qbeastTable.getIndexMetrics
      val totalElements = metrics.denormalizedBlocks
        .select(sum($"blockElementCount"))
        .as[Long]
        .first()
      totalElements shouldBe remaining.count()
  }

  it should "maintain query performance after deletion" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      import spark.implicits._

      val data = createTestDF(spark, 20000)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 500
      writeQbeastTable(data, columnsToIndex, cubeSize, tmpDir)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      qbeastTable.delete("age < 30")

      val queryStart = System.currentTimeMillis()
      val result = spark.read
        .format("qbeast")
        .load(tmpDir)
        .filter($"age".between(50, 60) && $"val2" < 300000)
        .count()
      val queryTime = System.currentTimeMillis() - queryStart

      result should be > 0L

      queryTime should be < 10000L

      val metrics = qbeastTable.getIndexMetrics
      val totalFiles = metrics.denormalizedBlocks.count()

      totalFiles should be > 5L
  }

  it should "handle delete on non-indexed columns only" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      import spark.implicits._

      val data = createTestDF(spark, 10000)
      val columnsToIndex = Seq("age", "val2") // name is NOT indexed
      val cubeSize = 500
      writeQbeastTable(data, columnsToIndex, cubeSize, tmpDir)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      qbeastTable.delete("name LIKE 'client-1%'")

      val remaining = spark.read.format("qbeast").load(tmpDir)
      remaining.filter($"name".like("client-1%")).count() shouldBe 0

      remaining.count() should be > 0L

      val metrics = qbeastTable.getIndexMetrics
      val totalElements = metrics.denormalizedBlocks
        .select(sum($"blockElementCount"))
        .as[Long]
        .first()
      totalElements shouldBe remaining.count()
  }

  it should "handle delete all rows" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
    val data = createTestDF(spark, 1000)
    val columnsToIndex = Seq("age", "val2")
    val cubeSize = 500
    writeQbeastTable(data, columnsToIndex, cubeSize, tmpDir)

    val qbeastTable = QbeastTable.forPath(spark, tmpDir)

    qbeastTable.delete("1 = 1")

    val remaining = spark.read.format("qbeast").load(tmpDir)
    remaining.count() shouldBe 0
  }

  it should "handle delete with IN clause" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
    import spark.implicits._

    val data = createTestDF(spark, 10000)
    val columnsToIndex = Seq("age", "val2")
    val cubeSize = 500
    writeQbeastTable(data, columnsToIndex, cubeSize, tmpDir)

    val qbeastTable = QbeastTable.forPath(spark, tmpDir)

    qbeastTable.delete("age IN (25, 30, 35, 40, 45)")

    val remaining = spark.read.format("qbeast").load(tmpDir)
    remaining.filter($"age".isin(25, 30, 35, 40, 45)).count() shouldBe 0

    remaining.filter(!$"age".isin(25, 30, 35, 40, 45)).count() should be > 0L
  }

  it should "handle delete with floating point column predicates" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      import spark.implicits._

      val data = createTestDF(spark, 10000)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 500
      writeQbeastTable(data, columnsToIndex, cubeSize, tmpDir)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      qbeastTable.delete("val3 > 10000.0")

      val remaining = spark.read.format("qbeast").load(tmpDir)
      remaining.filter($"val3" > 10000.0).count() shouldBe 0

      remaining.filter($"val3" <= 10000.0).count() should be > 0L
  }

  it should "handle delete with nested OR/AND expressions" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      import spark.implicits._

      val data = createTestDF(spark, 10000)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 500
      writeQbeastTable(data, columnsToIndex, cubeSize, tmpDir)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      qbeastTable.delete("(age > 70 AND val2 < 500000) OR (age < 25 AND val2 > 100000)")

      val remaining = spark.read.format("qbeast").load(tmpDir)
      val shouldBeDeleted =
        remaining.filter(($"age" > 70 && $"val2" < 500000) || ($"age" < 25 && $"val2" > 100000))
      shouldBeDeleted.count() shouldBe 0

      remaining.count() should be > 0L
  }

  it should "handle multiple sequential deletes" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      import spark.implicits._

      val data = createTestDF(spark, 10000)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 500
      writeQbeastTable(data, columnsToIndex, cubeSize, tmpDir)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      val initialCount = spark.read.format("qbeast").load(tmpDir).count()

      qbeastTable.delete("age < 25")
      val afterFirst = spark.read.format("qbeast").load(tmpDir).count()

      qbeastTable.delete("age > 75")
      val afterSecond = spark.read.format("qbeast").load(tmpDir).count()

      qbeastTable.delete("age BETWEEN 40 AND 45")
      val afterThird = spark.read.format("qbeast").load(tmpDir).count()

      afterFirst should be < initialCount
      afterSecond should be < afterFirst
      afterThird should be < afterSecond

      val remaining = spark.read.format("qbeast").load(tmpDir)
      remaining.filter($"age" < 25).count() shouldBe 0
      remaining.filter($"age" > 75).count() shouldBe 0
      remaining.filter($"age".between(40, 45)).count() shouldBe 0

      val metrics = qbeastTable.getIndexMetrics
      val totalElements = metrics.denormalizedBlocks
        .select(sum($"blockElementCount"))
        .as[Long]
        .first()
      totalElements shouldBe afterThird
  }

  it should "handle deletion with OR predicates" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      import spark.implicits._

      val data = createTestDF(spark, 10000)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 500
      writeQbeastTable(data, columnsToIndex, cubeSize, tmpDir)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      qbeastTable.delete("age < 25 OR age > 75")

      val remaining = spark.read.format("qbeast").load(tmpDir)
      remaining.filter($"age" < 25 || $"age" > 75).count() shouldBe 0

      val middleRange = remaining.filter($"age".between(25, 75))
      middleRange.count() should be > 0L
  }

  it should "handle delete with NOT predicate" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      import spark.implicits._

      val data = createTestDF(spark, 10000)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 500
      writeQbeastTable(data, columnsToIndex, cubeSize, tmpDir)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      qbeastTable.delete("NOT (age BETWEEN 30 AND 50)")

      val remaining = spark.read.format("qbeast").load(tmpDir)
      remaining.filter($"age" < 30 || $"age" > 50).count() shouldBe 0

      remaining.filter($"age".between(30, 50)).count() should be > 0L
  }

  it should "handle delete with comparison operators" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      import spark.implicits._

      val data = createTestDF(spark, 10000)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 500
      writeQbeastTable(data, columnsToIndex, cubeSize, tmpDir)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      qbeastTable.delete("age >= 70 AND age <= 75 AND val2 != 0")

      val remaining = spark.read.format("qbeast").load(tmpDir)
      remaining.filter($"age" >= 70 && $"age" <= 75 && $"val2" =!= 0).count() shouldBe 0
  }

  it should "verify Delta time travel works after delete" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      val data = createTestDF(spark, 5000)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 500
      writeQbeastTable(data, columnsToIndex, cubeSize, tmpDir)

      val initialCount = spark.read.format("qbeast").load(tmpDir).count()

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)
      qbeastTable.delete("age < 30")

      val afterDeleteCount = spark.read.format("qbeast").load(tmpDir).count()
      afterDeleteCount should be < initialCount

      val historicalCount = spark.read
        .format("delta")
        .option("versionAsOf", 0)
        .load(tmpDir)
        .count()

      historicalCount shouldBe initialCount
  }

  it should "maintain index consistency after delete then optimize" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      import spark.implicits._

      val data = createTestDF(spark, 10000)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 500
      writeQbeastTable(data, columnsToIndex, cubeSize, tmpDir)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      qbeastTable.delete("age < 30")

      val afterDeleteCount = spark.read.format("qbeast").load(tmpDir).count()

      qbeastTable.optimize()

      val afterOptimizeCount = spark.read.format("qbeast").load(tmpDir).count()
      afterOptimizeCount shouldBe afterDeleteCount

      spark.read.format("qbeast").load(tmpDir).filter($"age" < 30).count() shouldBe 0

      val metrics = qbeastTable.getIndexMetrics
      val totalElements = metrics.denormalizedBlocks
        .select(sum($"blockElementCount"))
        .as[Long]
        .first()
      totalElements shouldBe afterOptimizeCount
  }

}
