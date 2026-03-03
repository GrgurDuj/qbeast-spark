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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.SortedMap

class IndexStatusBuilderIncrementalTest extends AnyFlatSpec with Matchers {

  private val tableID = QTableID("test-table")
  private val revision = Revision.firstRevision(tableID, 1000, Vector.empty, Vector.empty)
  private val root = CubeId.root(2)
  private val Seq(c1, c2) = root.children.take(2).toList
  private val c3 = c1.children.next

  private def cubeStatus(cube: CubeId, maxWeightFraction: Double, count: Long): CubeStatus =
    CubeStatus(cube, Weight(maxWeightFraction), maxWeightFraction, count)

  private def indexFile(
      path: String,
      cube: CubeId,
      minFraction: Double,
      maxFraction: Double,
      elementCount: Long): IndexFile = {
    val block = Block(path, cube, Weight(minFraction), Weight(maxFraction), elementCount)
    IndexFile(
      path = path,
      size = 1L,
      dataChange = true,
      modificationTime = 0L,
      revisionId = revision.revisionID,
      blocks = Vector(block))
  }

  private def buildIncremental(base: IndexStatus, deltaFiles: Seq[IndexFile]): IndexStatus = {
    val builder = SortedMap.newBuilder[CubeId, CubeStatus]
    builder ++= base.cubesStatuses

    val deltaByCube: Map[CubeId, Seq[Block]] =
      deltaFiles.flatMap(_.blocks).groupBy(_.cubeId)

    deltaByCube.foreach { case (cubeId, blocks) =>
      val deltaMaxWeight = blocks.map(_.maxWeight).min
      val deltaElementCount = blocks.map(_.elementCount).sum

      base.cubesStatuses.get(cubeId) match {
        case Some(existing) =>
          val mergedMaxWeight = Weight.min(existing.maxWeight, deltaMaxWeight)
          val mergedElementCount = existing.elementCount + deltaElementCount
          val mergedNormWeight = mergedMaxWeight.fraction
          builder += cubeId -> CubeStatus(
            cubeId,
            mergedMaxWeight,
            mergedNormWeight,
            mergedElementCount)

        case None =>
          val normWeight = deltaMaxWeight.fraction
          builder += cubeId -> CubeStatus(cubeId, deltaMaxWeight, normWeight, deltaElementCount)
      }
    }

    IndexStatus(revision, builder.result())
  }

  "buildIncremental" should "return the base IndexStatus unchanged when there are no delta files" in {
    val baseStatuses =
      SortedMap(root -> cubeStatus(root, 0.1, 500L), c1 -> cubeStatus(c1, 0.7, 300L))
    val base = IndexStatus(revision, baseStatuses)

    val result = buildIncremental(base, Seq.empty)

    result.cubesStatuses.size shouldBe 2
    result.cubesStatuses(root).elementCount shouldBe 500L
    result.cubesStatuses(c1).elementCount shouldBe 300L
  }

  it should "add a brand-new cube from delta files that was absent in the base snapshot" in {
    val baseStatuses = SortedMap(root -> cubeStatus(root, 0.1, 500L))
    val base = IndexStatus(revision, baseStatuses)

    // c1 is a new cube introduced by a delta commit
    val delta = Seq(indexFile("file-delta-1.parquet", c1, 0.1, 0.6, 200L))

    val result = buildIncremental(base, delta)

    result.cubesStatuses should contain key c1
    result.cubesStatuses(c1).elementCount shouldBe 200L
    result.cubesStatuses(c1).maxWeight shouldBe Weight(0.6)
  }

  it should "accumulate elementCount when a cube appears in both base and delta" in {
    val baseStatuses =
      SortedMap(root -> cubeStatus(root, 0.2, 1000L), c1 -> cubeStatus(c1, 0.8, 400L))
    val base = IndexStatus(revision, baseStatuses)

    // Another append fills c1 further
    val delta = Seq(indexFile("file-delta-2.parquet", c1, 0.6, 0.9, 150L))

    val result = buildIncremental(base, delta)

    result.cubesStatuses(c1).elementCount shouldBe (400L + 150L)
  }

  it should "take the minimum maxWeight when merging an existing cube with delta blocks" in {
    // Base has c1 with maxWeight = 0.8 (partially filled leaf)
    val baseStatuses = SortedMap(c1 -> cubeStatus(c1, 0.8, 500L))
    val base = IndexStatus(revision, baseStatuses)

    // Delta pushes a block with lower maxWeight = 0.5, indicating more data was added
    val delta = Seq(indexFile("file-delta-3.parquet", c1, 0.3, 0.5, 300L))

    val result = buildIncremental(base, delta)

    result.cubesStatuses(c1).maxWeight shouldBe Weight(0.5)
    result.cubesStatuses(c1).elementCount shouldBe (500L + 300L)
  }

  it should "keep cubes from base that are not touched by delta files" in {
    val baseStatuses =
      SortedMap(root -> cubeStatus(root, 0.1, 100L), c2 -> cubeStatus(c2, 1.0, 50L))
    val base = IndexStatus(revision, baseStatuses)

    // Delta only touches c1 — root and c2 should be untouched
    val delta = Seq(indexFile("file-delta-4.parquet", c1, 0.1, 0.7, 200L))

    val result = buildIncremental(base, delta)

    result.cubesStatuses(root).elementCount shouldBe 100L
    result.cubesStatuses(c2).elementCount shouldBe 50L
  }

  it should "handle multiple delta files touching different cubes" in {
    val baseStatuses = SortedMap(root -> cubeStatus(root, 0.1, 1000L))
    val base = IndexStatus(revision, baseStatuses)

    val delta = Seq(
      indexFile("file-delta-5a.parquet", c1, 0.1, 0.6, 300L),
      indexFile("file-delta-5b.parquet", c2, 0.1, 0.8, 200L),
      indexFile("file-delta-5c.parquet", c3, 0.6, 1.0, 100L))

    val result = buildIncremental(base, delta)

    result.cubesStatuses should contain key c1
    result.cubesStatuses should contain key c2
    result.cubesStatuses should contain key c3
    result.cubesStatuses(c1).elementCount shouldBe 300L
    result.cubesStatuses(c2).elementCount shouldBe 200L
    result.cubesStatuses(c3).elementCount shouldBe 100L
  }

  it should "handle multiple delta files for the same cube by summing counts and taking min weight" in {
    val baseStatuses = SortedMap(root -> cubeStatus(root, 0.1, 500L))
    val base = IndexStatus(revision, baseStatuses)

    // Two separate delta files both contribute to c1
    val delta = Seq(
      indexFile("file-delta-6a.parquet", c1, 0.1, 0.7, 100L),
      indexFile("file-delta-6b.parquet", c1, 0.1, 0.5, 250L))

    val result = buildIncremental(base, delta)

    // Min of 0.7 and 0.5 => 0.5
    result.cubesStatuses(c1).maxWeight shouldBe Weight(0.5)
    result.cubesStatuses(c1).elementCount shouldBe (100L + 250L)
  }

  it should "produce stable NormalizedWeight matching maxWeight.fraction after merge" in {
    val baseStatuses = SortedMap(root -> cubeStatus(root, 0.3, 800L))
    val base = IndexStatus(revision, baseStatuses)

    val delta = Seq(indexFile("file-delta-7.parquet", root, 0.0, 0.2, 400L))

    val result = buildIncremental(base, delta)
    val cs = result.cubesStatuses(root)
    val expected = Weight(0.2).fraction

    cs.normalizedWeight shouldBe (expected +- 1e-9)
    cs.maxWeight.fraction shouldBe (cs.normalizedWeight +- 1e-9)
  }

  it should "preserve the revision metadata in the merged IndexStatus" in {
    val base = IndexStatus(revision, SortedMap(root -> cubeStatus(root, 0.5, 100L)))
    val delta = Seq(indexFile("file-delta-8.parquet", c1, 0.1, 0.9, 50L))

    val result = buildIncremental(base, delta)

    result.revision shouldBe revision
    result.revision.revisionID shouldBe revision.revisionID
  }

  it should "produce identical results to a full rebuild when delta represents the full history" in {
    // Empty base (like a table with no snapshot yet)
    val emptyBase = IndexStatus(revision, SortedMap.empty[CubeId, CubeStatus])

    val allFiles = Seq(
      indexFile("file-a.parquet", root, 0.0, 0.1, 500L),
      indexFile("file-b.parquet", c1, 0.1, 0.7, 300L),
      indexFile("file-c.parquet", c2, 0.1, 1.0, 200L))

    val resultIncremental = buildIncremental(emptyBase, allFiles)

    // A full rebuild using the same files should yield the same element counts
    resultIncremental.cubesStatuses(root).elementCount shouldBe 500L
    resultIncremental.cubesStatuses(c1).elementCount shouldBe 300L
    resultIncremental.cubesStatuses(c2).elementCount shouldBe 200L
  }

}
