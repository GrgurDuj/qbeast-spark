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

class IndexStatusLoaderTest extends QbeastIntegrationTestSpec {

  private val tableID = QTableID("loader-test")

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

  private def writeSnapshot(
      spark: org.apache.spark.sql.SparkSession,
      tmpDir: String,
      statuses: SortedMap[CubeId, CubeStatus],
      rev: RevisionID,
      version: Long): Unit =
    IndexStatusDumper.dump(statuses, tmpDir, rev, version)(spark)

  "IndexStatusLoader.latestSnapshotVersion" should
    "return None when no snapshot directory exists at all" in withSparkAndTmpDir {
      (spark, tmpDir) =>
        val result =
          IndexStatusLoader.latestSnapshotVersion(tmpDir, revID, targetVersion = 10L)(spark)
        result shouldBe None
    }

  it should "return None when every written version exceeds targetVersion" in
    withSparkAndTmpDir { (spark, tmpDir) =>
      writeSnapshot(spark, tmpDir, SortedMap(root -> cs(root, 0.5, 1L)), revID, version = 5L)
      writeSnapshot(spark, tmpDir, SortedMap(root -> cs(root, 0.5, 2L)), revID, version = 8L)

      val result =
        IndexStatusLoader.latestSnapshotVersion(tmpDir, revID, targetVersion = 3L)(spark)
      result shouldBe None
    }

  it should "return the exact version when only one snapshot exists and it equals targetVersion" in
    withSparkAndTmpDir { (spark, tmpDir) =>
      writeSnapshot(spark, tmpDir, SortedMap(root -> cs(root, 0.5, 1L)), revID, version = 4L)

      IndexStatusLoader.latestSnapshotVersion(tmpDir, revID, targetVersion = 4L)(
        spark) shouldBe Some(4L)
    }

  it should "return the highest version that does not exceed targetVersion" in
    withSparkAndTmpDir { (spark, tmpDir) =>
      writeSnapshot(spark, tmpDir, SortedMap(root -> cs(root, 0.1, 1L)), revID, version = 2L)
      writeSnapshot(spark, tmpDir, SortedMap(root -> cs(root, 0.1, 2L)), revID, version = 5L)
      writeSnapshot(spark, tmpDir, SortedMap(root -> cs(root, 0.1, 3L)), revID, version = 9L)

      IndexStatusLoader.latestSnapshotVersion(tmpDir, revID, targetVersion = 6L)(
        spark) shouldBe Some(5L)
    }

  it should "return the latest version when targetVersion >= all available versions" in
    withSparkAndTmpDir { (spark, tmpDir) =>
      writeSnapshot(spark, tmpDir, SortedMap(root -> cs(root, 0.1, 1L)), revID, version = 1L)
      writeSnapshot(spark, tmpDir, SortedMap(root -> cs(root, 0.1, 2L)), revID, version = 3L)

      IndexStatusLoader.latestSnapshotVersion(tmpDir, revID, targetVersion = 100L)(
        spark) shouldBe Some(3L)
    }

  it should "return None for an unknown revisionID even when other revisions have snapshots" in
    withSparkAndTmpDir { (spark, tmpDir) =>
      writeSnapshot(spark, tmpDir, SortedMap(root -> cs(root, 0.5, 1L)), 1L, 0L)

      IndexStatusLoader.latestSnapshotVersion(tmpDir, 99L, 10L)(spark) shouldBe None
    }

  "IndexStatusLoader.load" should "reconstruct the same number of cubes that were dumped" in
    withSparkAndTmpDir { (spark, tmpDir) =>
      val statuses =
        SortedMap(root -> cs(root, 0.1, 1000L), c1 -> cs(c1, 0.6, 500L), c2 -> cs(c2, 1.0, 400L))

      writeSnapshot(spark, tmpDir, statuses, revID, version = 1L)

      val recovered = IndexStatusLoader.load(tmpDir, revID, snapshotVersion = 1L, revision)(spark)
      recovered.size shouldBe 3
    }

  it should "recover identical CubeId keys after a round-trip" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      val statuses = SortedMap(root -> cs(root, 0.2, 10L), c3 -> cs(c3, 1.0, 5L))
      writeSnapshot(spark, tmpDir, statuses, revID, version = 1L)

      val recovered = IndexStatusLoader.load(tmpDir, revID, snapshotVersion = 1L, revision)(spark)
      recovered.keySet shouldBe statuses.keySet
  }

  it should "recover exact elementCount for every cube" in withSparkAndTmpDir { (spark, tmpDir) =>
    val count = 123456789L
    val statuses = SortedMap(root -> cs(root, 0.5, count))
    writeSnapshot(spark, tmpDir, statuses, revID, version = 1L)

    val recovered = IndexStatusLoader.load(tmpDir, revID, snapshotVersion = 1L, revision)(spark)
    recovered(root).elementCount shouldBe count
  }

  it should "recover the exact maxWeight Int value" in withSparkAndTmpDir { (spark, tmpDir) =>
    val w = Weight(0.333)
    val statuses = SortedMap(root -> CubeStatus(root, w, w.fraction, 1L))
    writeSnapshot(spark, tmpDir, statuses, revID, version = 1L)

    val recovered = IndexStatusLoader.load(tmpDir, revID, snapshotVersion = 1L, revision)(spark)
    recovered(root).maxWeight.value shouldBe w.value
  }

  it should "recover normalizedWeight consistent with maxWeight.fraction" in
    withSparkAndTmpDir { (spark, tmpDir) =>
      val statuses =
        SortedMap(root -> cs(root, 0.1, 2000L), c1 -> cs(c1, 0.7, 1000L), c2 -> cs(c2, 0.9, 500L))

      writeSnapshot(spark, tmpDir, statuses, revID, version = 1L)

      val recovered = IndexStatusLoader.load(tmpDir, revID, snapshotVersion = 1L, revision)(spark)
      recovered.foreach { case (_, cubeStatus) =>
        cubeStatus.normalizedWeight shouldBe (cubeStatus.maxWeight.fraction +- 1e-9)
      }
    }

  it should "load Weight.MaxValue without Int overflow" in withSparkAndTmpDir { (spark, tmpDir) =>
    val statuses = SortedMap(c2 -> CubeStatus(c2, Weight.MaxValue, Weight.MaxValue.fraction, 7L))
    writeSnapshot(spark, tmpDir, statuses, revID, version = 1L)

    val recovered = IndexStatusLoader.load(tmpDir, revID, snapshotVersion = 1L, revision)(spark)
    recovered(c2).maxWeight shouldBe Weight.MaxValue
    recovered(c2).maxWeight.value shouldBe Int.MaxValue
  }

  it should "return an empty SortedMap for an empty snapshot" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      writeSnapshot(spark, tmpDir, SortedMap.empty[CubeId, CubeStatus], revID, version = 1L)

      val recovered = IndexStatusLoader.load(tmpDir, revID, snapshotVersion = 1L, revision)(spark)
      recovered shouldBe empty
  }

  it should "perform a complete lossless round-trip for a multi-level tree" in
    withSparkAndTmpDir { (spark, tmpDir) =>
      val statuses = SortedMap(
        root -> cs(root, 0.1, 2000L),
        c1 -> cs(c1, 0.7, 1000L),
        c2 -> cs(c2, 1.0, 800L),
        c3 -> cs(c3, 1.0, 200L))

      writeSnapshot(spark, tmpDir, statuses, revID, version = 7L)

      val recovered = IndexStatusLoader.load(tmpDir, revID, snapshotVersion = 7L, revision)(spark)

      recovered.size shouldBe statuses.size

      statuses.foreach { case (cubeId, original) =>
        val r = recovered(cubeId)
        r.elementCount shouldBe original.elementCount
        r.maxWeight.value shouldBe original.maxWeight.value
      }
    }

}
