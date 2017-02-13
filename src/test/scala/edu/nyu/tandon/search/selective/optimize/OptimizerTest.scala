package edu.nyu.tandon.search.selective.optimize

import edu.nyu.tandon.search.selective.data.Properties
import edu.nyu.tandon.search.selective.{BudgetIndicator, Titles2Map}
import edu.nyu.tandon.test.BaseFunSuite
import edu.nyu.tandon.utils.Lines
import edu.nyu.tandon.utils.Lines._
import edu.nyu.tandon.utils.WriteLineIterator._
import org.scalatest.Matchers._

import scala.collection.mutable

/**
  * @author michal.siedlaczek@nyu.edu
  */
class OptimizerTest extends BaseFunSuite {

  trait PQ {
    val k = 4
    val q = new mutable.PriorityQueue[Result]()(Ordering.by(_.score))
    q.enqueue(
      Result(1.0, hit = true),
      Result(0.9, hit = false),
      Result(0.8, hit = false),
      Result(0.7, hit = false)
    )
  }

  trait S {
    val s1 = new Shard(List(
      Bucket(List(Result(1.0, hit = true)), 1.0), // already selected
      Bucket(List(Result(1.2, hit = false)), 1.0),
      Bucket(List(Result(1.3, hit = true)), 0.001),
      Bucket(List(Result(1.5, hit = true)), 0.001)
    ), numSelected = 1)
    val s2 = new Shard(List(
      Bucket(List(Result(0.9, hit = true)), 1.0), // already selected
      Bucket(List(Result(1.2, hit = true)), 1.0),
      Bucket(List(Result(1.3, hit = false)), 0.001),
      Bucket(List(Result(1.5, hit = false)), 0.001)
    ), numSelected = 1)
  }

  test("calcPrecision") {
    new PQ {
      Optimizer.calcPrecision(q, q.length) shouldBe (0.25 +- 0.0001)
    }
  }

  test("Shard.selectWithBudget: s1") {
    new PQ {
      new S {
        val Some((s, precision, cost)) = s1.selectWithBudget(q, k, 10)
        s.numSelected shouldBe 4
        s.buckets should contain theSameElementsInOrderAs s1.buckets
        precision shouldBe 0.75
        cost shouldBe (1.002 +- 0.0001)
      }
    }
  }

  test("Shard.selectWithBudget: s1, budget=1") {
    new PQ {
      new S {
        val sel = s1.selectWithBudget(q, k, 1)
        sel shouldBe None
      }
    }
  }

  test("Shard.selectWithBudget: s2") {
    new PQ {
      new S {
        val Some((s, precision, cost)) = s2.selectWithBudget(q, k, 10)
        s.numSelected shouldBe 2
        s.buckets should contain theSameElementsInOrderAs s2.buckets
        precision shouldBe 0.5
        cost shouldBe (1.0 +- 0.0001)
      }
    }
  }

  test("Shard.selectWithBudget: s2 budget=0.5") {
    new PQ {
      new S {
        val sel = s2.selectWithBudget(q, k, 0.5)
        sel shouldBe None
      }
    }
  }

  test("PrecisionOptimizer.select") {
    new PQ {
      new S {
        val p = new PrecisionOptimizer(List(s1, s2), 10.0, k, topResults = q)
        val sel = p.select()
        sel.shards.map(_.buckets) should contain theSameElementsInOrderAs p.shards.map(_.buckets)
        sel.shards.map(_.numSelected) should contain theSameElementsInOrderAs List(4, 1)
      }
    }
  }

  test("PrecisionOptimizer.select: None") {
    new PQ {
      new S {
        val p = new PrecisionOptimizer(List(s1, s2), 0.5, k, topResults = q)
        val sel = p.select()
        sel.shards.map(_.buckets) should contain theSameElementsInOrderAs p.shards.map(_.buckets)
        sel.shards.map(_.numSelected) should contain theSameElementsInOrderAs p.shards.map(_.numSelected)
        val budget = sel.budget
        budget shouldBe 0.0
      }
    }
  }

  test("PrecisionOptimizer.optimize: budget=1") {
    new PQ {
      new S {
        val p = new PrecisionOptimizer(List(s1, s2), 1.0, k, topResults = q)
        val sel = p.optimize()
        sel.shards.map(_.buckets) should contain theSameElementsInOrderAs p.shards.map(_.buckets)
        sel.shards.map(_.numSelected) should contain theSameElementsInOrderAs List(1, 2)
      }
    }
  }

  test("PrecisionOptimizer.optimize: budget=2.002") {
    new PQ {
      new S {
        val p = new PrecisionOptimizer(List(s1, s2), 2.002, k, topResults = q)
        val sel = p.optimize()
        sel.shards.map(_.buckets) should contain theSameElementsInOrderAs p.shards.map(_.buckets)
        sel.shards.map(_.numSelected) should contain theSameElementsInOrderAs List(4, 1)
      }
    }
  }

  test("PrecisionOptimizer.optimize: budget=10.0") {
    new PQ {
      new S {
        val p = new PrecisionOptimizer(List(s1, s2), 10.0, k, topResults = q)
        val sel = p.optimize()
        sel.shards.map(_.buckets) should contain theSameElementsInOrderAs p.shards.map(_.buckets)
        sel.shards.map(_.numSelected) should contain theSameElementsInOrderAs List(4, 1)
      }
    }
  }

  test("Optimizer.takeUntilFirstSatisfiedPrecision: Nil") {
    Optimizer.takeUntilFirstSatisfiedPrecision(Nil, 10.0) should contain theSameElementsInOrderAs Nil
  }

  test("Optimizer.takeUntilFirstSatisfiedPrecision: one element, less than target") {
    val l = List((1.0, 1.0))
    Optimizer.takeUntilFirstSatisfiedPrecision(l, 10.0) should contain theSameElementsInOrderAs l
  }

  test("Optimizer.takeUntilFirstSatisfiedPrecision: one element, greater than target") {
    val l = List((1.0, 11.0))
    Optimizer.takeUntilFirstSatisfiedPrecision(l, 10.0) should contain theSameElementsInOrderAs l
  }

  test("Optimizer.takeUntilFirstSatisfiedPrecision: 3 elements, all lower than target") {
    val l = List(
      (1.0, 1.0),
      (1.0, 2.0),
      (1.0, 3.0)
    )
    Optimizer.takeUntilFirstSatisfiedPrecision(l, 10.0) should contain theSameElementsInOrderAs l
  }

  test("Optimizer.takeUntilFirstSatisfiedPrecision: 3 elements, last at target") {
    val l = List(
      (1.0, 1.0),
      (1.0, 2.0),
      (1.0, 10.0)
    )
    Optimizer.takeUntilFirstSatisfiedPrecision(l, 10.0) should contain theSameElementsInOrderAs l
  }

  test("Optimizer.takeUntilFirstSatisfiedPrecision: 3 elements, middle at target") {
    val l = List(
      (1.0, 1.0),
      (1.0, 10.0),
      (1.0, 10.0)
    )
    Optimizer.takeUntilFirstSatisfiedPrecision(l, 10.0) should contain theSameElementsInOrderAs l.take(2)
  }

  test("Optimizer.takeUntilFirstSatisfiedPrecision: 3 elements, first at target") {
    val l = List(
      (1.0, 10.0),
      (1.0, 10.0),
      (1.0, 10.0)
    )
    Optimizer.takeUntilFirstSatisfiedPrecision(l, 10.0) should contain theSameElementsInOrderAs l.take(1)
  }

  test("Shard.selectWithPrecision") {
    new PQ {
      new S {
        val Some((shard, precision, cost)) = s1.selectWithPrecision(q, 4, 0.5)
        precision shouldBe 0.5
        cost shouldBe (1.001 +- 0.0001)
        shard.numSelected shouldBe 3
      }
    }
  }

  test("Shard.selectWithPrecision: target = 1.0") {
    new PQ {
      new S {
        val Some((shard, precision, cost)) = s1.selectWithPrecision(q, 4, 1.0)
        precision shouldBe 0.75
        cost shouldBe (1.002 +- 0.0001)
        shard.numSelected shouldBe 4
      }
    }
  }

  test("Shard.selectWithPrecision: s2, target = 10.0") {
    new PQ {
      new S {
        val Some((shard, precision, cost)) = s2.selectWithPrecision(q, 4, 10.0)
        precision shouldBe 0.5
        cost shouldBe (1.0 +- 0.0001)
        shard.numSelected shouldBe 2

        q.enqueue(s2.buckets(1).results:_*)
        q.enqueue(q.dequeueAll.take(k):_*)
        val Some((shard2, precision2, cost2)) = shard.selectWithPrecision(q, 4, 10.0)
        precision2 shouldBe 0.5
        cost2 shouldBe (0.001 +- 0.0001)
        shard2.numSelected shouldBe 3

        q.enqueue(s2.buckets(2).results:_*)
        q.enqueue(q.dequeueAll.take(k):_*)
        val Some((shard3, precision3, cost3)) = shard2.selectWithPrecision(q, 4, 10.0)
        precision3 shouldBe 0.5
        cost3 shouldBe (0.001 +- 0.0001)
        shard3.numSelected shouldBe 4
      }
    }
  }

  test("BudgetOptimizer.select") {
    new PQ {
      new S {
        val b = new BudgetOptimizer(List(s1, s2), 10.0, k, topResults = q)
        val sel = b.select()
        sel.shards.map(_.buckets) should contain theSameElementsInOrderAs b.shards.map(_.buckets)
        sel.shards.map(_.numSelected) should contain theSameElementsInOrderAs List(4, 1)
      }
    }
  }

  test("BudgetOptimizer.select: target = 0.5") {
    new PQ {
      new S {
        val b = new BudgetOptimizer(List(s1, s2), 0.5, k, topResults = q)
        val sel = b.select()
        sel.shards.map(_.buckets) should contain theSameElementsInOrderAs b.shards.map(_.buckets)
        sel.shards.map(_.numSelected) should contain theSameElementsInOrderAs List(1, 2)
      }
    }
  }

  test("BudgetOptimizer.optimize") {
    new PQ {
      new S {
        val b = new BudgetOptimizer(List(s1, s2), 10.0, k, topResults = q)
        val sel = b.optimize()
        sel.shards.map(_.buckets) should contain theSameElementsInOrderAs b.shards.map(_.buckets)
        sel.shards.map(_.numSelected) should contain theSameElementsInOrderAs List(4, 4)
      }
    }
  }

  trait ShardsOnDisk extends S {
    val tmpDir = createTemporaryDir()
    val basename = "basename"
    val features = "features"

    List("features = features",
         "buckets.count = 4",
         "k = 4").write(s"$tmpDir/$basename.properties")

    List("1 3 4 5 6").write(s"$tmpDir/$features.results.global")
    List("10", "10").write(s"$tmpDir/$features.sizes")

    List(
      "000",
      "001",
      "002",
      "003",
      "004",
      "005",
      "006",
      "007",
      "008",
      "009",
      "010",
      "011"
    ).write(s"$tmpDir/$features.titles")

    List(
      "1 0 000 0",
      "1 0 001 1",
      "1 0 002 0",
      "1 0 003 1",
      "1 0 004 1",
      "1 0 005 1",
      "1 0 006 1",
      "1 0 007 0",
      "1 0 008 0",
      "1 0 009 0",
      "1 0 010 0",
      "1 0 011 0"
    ).write(s"$tmpDir/$features.qrels")


    List("1").write(s"$tmpDir/$basename#0#0.results.global")
    List("2").write(s"$tmpDir/$basename#0#1.results.global")
    List("3").write(s"$tmpDir/$basename#0#2.results.global")
    List("4").write(s"$tmpDir/$basename#0#3.results.global")

    List("1.0").write(s"$tmpDir/$basename#0#0.results.scores")
    List("1.2").write(s"$tmpDir/$basename#0#1.results.scores")
    List("1.3").write(s"$tmpDir/$basename#0#2.results.scores")
    List("1.5").write(s"$tmpDir/$basename#0#3.results.scores")

    List("1.0").write(s"$tmpDir/$basename#0#0.cost")
    List("1.0").write(s"$tmpDir/$basename#0#1.cost")
    List("0.001").write(s"$tmpDir/$basename#0#2.cost")
    List("0.001").write(s"$tmpDir/$basename#0#3.cost")


    List("5").write(s"$tmpDir/$basename#1#0.results.global")
    List("6").write(s"$tmpDir/$basename#1#1.results.global")
    List("7").write(s"$tmpDir/$basename#1#2.results.global")
    List("8").write(s"$tmpDir/$basename#1#3.results.global")

    List("0.9").write(s"$tmpDir/$basename#1#0.results.scores")
    List("1.2").write(s"$tmpDir/$basename#1#1.results.scores")
    List("1.3").write(s"$tmpDir/$basename#1#2.results.scores")
    List("1.5").write(s"$tmpDir/$basename#1#3.results.scores")

    List("1.0").write(s"$tmpDir/$basename#1#0.cost")
    List("1.0").write(s"$tmpDir/$basename#1#1.cost")
    List("0.001").write(s"$tmpDir/$basename#1#2.cost")
    List("0.001").write(s"$tmpDir/$basename#1#3.cost")

  }

  trait ShardsWithMap extends ShardsOnDisk {
    Titles2Map.titles2map(Properties.get(s"$tmpDir/$basename").features)
  }

  test("PrecisionOptimizer.main: overlap, b=10") {
    new ShardsOnDisk {
      PrecisionOptimizer.main(Array(
        "overlap",
        s"$tmpDir/$basename",
        "--budget", "10",
        "-k", "4"
      ))
      Lines.fromFile(s"$tmpDir/$basename$BudgetIndicator[opt].selection").ofSeq[Int]
        .next() should contain theSameElementsInOrderAs List(4, 0)
    }
  }

  test("PrecisionOptimizer.main: overlap, b=2.001") {
    new ShardsOnDisk {
      PrecisionOptimizer.main(Array(
        "overlap",
        s"$tmpDir/$basename",
        "--budget", "2.001",
        "-k", "4"
      ))
      Lines.fromFile(s"$tmpDir/$basename$BudgetIndicator[opt].selection").ofSeq[Int]
        .next() should contain theSameElementsInOrderAs List(1, 1)
    }
  }

  test("PrecisionOptimizer.main: overlap, b=2.002") {
    new ShardsOnDisk {
      PrecisionOptimizer.main(Array(
        "overlap",
        s"$tmpDir/$basename",
        "--budget", "2.002",
        "-k", "4"
      ))
      Lines.fromFile(s"$tmpDir/$basename$BudgetIndicator[opt].selection").ofSeq[Int]
        .next() should contain theSameElementsInOrderAs List(4, 0)
    }
  }

  test("PrecisionOptimizer.main: relevance, b=10") {
    new ShardsWithMap {
      PrecisionOptimizer.main(Array(
        "relevance",
        s"$tmpDir/$basename",
        "--budget", "10",
        "-k", "4"
      ))
      Lines.fromFile(s"$tmpDir/$basename$BudgetIndicator[opt].selection").ofSeq[Int]
        .next() should contain theSameElementsInOrderAs List(4, 0)
    }
  }

  test("PrecisionOptimizer.main: relevance, b=2.001") {
    new ShardsWithMap {
      PrecisionOptimizer.main(Array(
        "relevance",
        s"$tmpDir/$basename",
        "--budget", "2.001",
        "-k", "4"
      ))
      Lines.fromFile(s"$tmpDir/$basename$BudgetIndicator[opt].selection").ofSeq[Int]
        .next() should contain theSameElementsInOrderAs List(1, 1)
    }
  }

  test("PrecisionOptimizer.main: relevance, b=2.002") {
    new ShardsWithMap {
      PrecisionOptimizer.main(Array(
        "relevance",
        s"$tmpDir/$basename",
        "--budget", "2.002",
        "-k", "4"
      ))
      Lines.fromFile(s"$tmpDir/$basename$BudgetIndicator[opt].selection").ofSeq[Int]
        .next() should contain theSameElementsInOrderAs List(4, 0)
    }
  }

  test("PrecisionOptimizer.main: relevance, no map") {
    new ShardsOnDisk {
      PrecisionOptimizer.main(Array(
        "relevance",
        s"$tmpDir/$basename",
        "--budget", "10",
        "-k", "4"
      ))
      Lines.fromFile(s"$tmpDir/$basename$BudgetIndicator[opt].selection").ofSeq[Int]
        .next() should contain theSameElementsInOrderAs List(4, 0)
    }
  }

}
