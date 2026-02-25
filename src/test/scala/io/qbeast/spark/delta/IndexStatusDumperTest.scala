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

import io.qbeast.core.model._
import io.qbeast.core.transform.LinearTransformer
import io.qbeast.QbeastIntegrationTestSpec

import scala.collection.immutable.SortedMap

class IndexStatusDumperTest extends QbeastIntegrationTestSpec {

  private val tableID = QTableID("dumper-test")

  private val revision = Revision.firstRevision(
    tableID,
    1000,
    Vector(LinearTransformer("x", DoubleDataType), LinearTransformer("y", DoubleDataType)),
    Vector.empty)

  private val revID = revision.revisionID

  private val root = CubeId.root(2)
  private val Seq(c1, c2) = root.children.take(2).toList
  private val c3 = c1.children.next

  private def cs(cube: CubeId, fraction: Double, count: Long): CubeStatus =
    CubeStatus(cube, Weight(fraction), fraction, count)

  private def snapshotPath(base: String, rev: RevisionID, version: Long): String =
    s"$base/_qbeast/index_snapshots/revision=$rev/version=$version"

  "IndexStatusDumper.dump" should "create a Parquet file at the expected path" in
    withSparkAndTmpDir { (spark, tmpDir) =>
      val statuses = SortedMap(root -> cs(root, 0.1, 500L))
      IndexStatusDumper.dump(statuses, tmpDir, revID, deltaVersion = 3L)(spark)

      val expectedPath = snapshotPath(tmpDir, revID, 3L)
      val fs = new org.apache.hadoop.fs.Path(expectedPath)
        .getFileSystem(spark.sessionState.newHadoopConf())
      fs.exists(new org.apache.hadoop.fs.Path(expectedPath)) shouldBe true
    }

  it should "write exactly one row per cube" in withSparkAndTmpDir { (spark, tmpDir) =>
    val statuses = SortedMap(
      root -> cs(root, 0.1, 1000L),
      c1 -> cs(c1, 0.6, 500L),
      c2 -> cs(c2, 1.0, 400L),
      c3 -> cs(c3, 1.0, 100L))

    IndexStatusDumper.dump(statuses, tmpDir, revID, deltaVersion = 1L)(spark)

    val df = spark.read.parquet(snapshotPath(tmpDir, revID, 1L))
    df.count() shouldBe 4L
  }

  it should "write zero rows for an empty SortedMap" in withSparkAndTmpDir { (spark, tmpDir) =>
    IndexStatusDumper.dump(SortedMap.empty[CubeId, CubeStatus], tmpDir, revID, deltaVersion = 0L)(
      spark)

    val df = spark.read.parquet(snapshotPath(tmpDir, revID, 0L))
    df.count() shouldBe 0L
  }

  it should "write the correct cubeId string for each cube" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      val statuses = SortedMap(root -> cs(root, 0.5, 99L))
      IndexStatusDumper.dump(statuses, tmpDir, revID, deltaVersion = 1L)(spark)

      import spark.implicits._
      val ids = spark.read
        .parquet(snapshotPath(tmpDir, revID, 1L))
        .select("cubeId")
        .as[String]
        .collect()
        .toSet

      ids shouldBe Set(root.string)
  }

  it should "write the raw Int maxWeightInt value without loss" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      val w = Weight(0.42)
      val statuses = SortedMap(root -> CubeStatus(root, w, w.fraction, 1L))
      IndexStatusDumper.dump(statuses, tmpDir, revID, deltaVersion = 1L)(spark)

      import spark.implicits._
      val stored = spark.read
        .parquet(snapshotPath(tmpDir, revID, 1L))
        .select("maxWeightInt")
        .as[Int]
        .collect()
        .head

      stored shouldBe w.value
  }

  it should "write normalizedWeight as a Double consistent with maxWeight.fraction" in
    withSparkAndTmpDir { (spark, tmpDir) =>
      val w = Weight(0.75)
      val statuses = SortedMap(root -> CubeStatus(root, w, w.fraction, 200L))
      IndexStatusDumper.dump(statuses, tmpDir, revID, deltaVersion = 1L)(spark)

      import spark.implicits._
      val stored = spark.read
        .parquet(snapshotPath(tmpDir, revID, 1L))
        .select("normalizedWeight")
        .as[Double]
        .collect()
        .head

      stored shouldBe (w.fraction +- 1e-9)
    }

  it should "write elementCount as a Long value exactly" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      val count = 987654321L
      val statuses = SortedMap(root -> cs(root, 0.3, count))
      IndexStatusDumper.dump(statuses, tmpDir, revID, deltaVersion = 1L)(spark)

      import spark.implicits._
      val stored = spark.read
        .parquet(snapshotPath(tmpDir, revID, 1L))
        .select("elementCount")
        .as[Long]
        .collect()
        .head

      stored shouldBe count
  }

  it should "write Weight.MaxValue as Int.MaxValue without overflow" in
    withSparkAndTmpDir { (spark, tmpDir) =>
      val statuses =
        SortedMap(c1 -> CubeStatus(c1, Weight.MaxValue, Weight.MaxValue.fraction, 42L))
      IndexStatusDumper.dump(statuses, tmpDir, revID, deltaVersion = 1L)(spark)

      import spark.implicits._
      val stored = spark.read
        .parquet(snapshotPath(tmpDir, revID, 1L))
        .select("maxWeightInt")
        .as[Int]
        .collect()
        .head

      stored shouldBe Int.MaxValue
    }

  it should "overwrite an existing snapshot for the same version (idempotent)" in
    withSparkAndTmpDir { (spark, tmpDir) =>
      val first = SortedMap(root -> cs(root, 0.5, 100L))
      val second = SortedMap(root -> cs(root, 0.5, 100L), c1 -> cs(c1, 0.9, 50L))

      IndexStatusDumper.dump(first, tmpDir, revID, deltaVersion = 5L)(spark)
      IndexStatusDumper.dump(second, tmpDir, revID, deltaVersion = 5L)(spark) // same version

      // Second dump wins — two rows expected
      val df = spark.read.parquet(snapshotPath(tmpDir, revID, 5L))
      df.count() shouldBe 2L
    }

  it should "write to separate paths for different revisionIDs" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      val rev1 = 1L
      val rev2 = 2L

      IndexStatusDumper.dump(
        SortedMap(root -> cs(root, 0.1, 10L)),
        tmpDir,
        rev1,
        deltaVersion = 0L)(spark)
      IndexStatusDumper.dump(
        SortedMap(root -> cs(root, 0.2, 20L), c1 -> cs(c1, 0.8, 5L)),
        tmpDir,
        rev2,
        deltaVersion = 0L)(spark)

      spark.read.parquet(snapshotPath(tmpDir, rev1, 0L)).count() shouldBe 1L
      spark.read.parquet(snapshotPath(tmpDir, rev2, 0L)).count() shouldBe 2L
  }

  it should "write a Parquet file with the expected column names" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      val statuses = SortedMap(root -> cs(root, 0.5, 1L))
      IndexStatusDumper.dump(statuses, tmpDir, revID, deltaVersion = 1L)(spark)

      val columns = spark.read.parquet(snapshotPath(tmpDir, revID, 1L)).columns.toSet
      columns should contain allOf ("cubeId", "maxWeightInt", "normalizedWeight", "elementCount")
  }

}
