package edu.nyu.tandon.search.selective.select
import edu.nyu.tandon.search.selective._
import edu.nyu.tandon.test.BaseFunSuite
import edu.nyu.tandon.utils.Lines
import org.scalatest.Matchers._
import edu.nyu.tandon.utils.Lines._
import edu.nyu.tandon.utils.WriteLineIterator._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class SmartSelectorTest extends BaseFunSuite {

  trait S {
    val s1 = new Shard(List(
      Bucket(impact = 1.0, cost = 1.0),
      Bucket(impact = 0.0, cost = 1.0),
      Bucket(impact = 0.1, cost = 1.0),
      Bucket(impact = 1.0, cost = 1.0)
    ), numSelected = 1, costOfSelected = 1.0)
  }

  test("Shard.nextAvailable") {
    new S {
      val Some(shard1) = s1.nextAvailable()
      shard1.numSelected shouldBe 3
      shard1.costOfSelected shouldBe 3.0

      val Some(shard2) = shard1.nextAvailable()
      shard2.numSelected shouldBe 4
      shard2.costOfSelected shouldBe 4.0

      shard2.nextAvailable() shouldBe None

      s1.nextAvailable(threshold = 0.1).get.numSelected shouldBe 4
      s1.nextAvailable(threshold = 0.01).get.numSelected shouldBe 3

    }
  }

  test("Shard.threshold") {
    new S {
      SmartSelector(List(s1), 1.0, 1).threshold shouldBe 1.0
      SmartSelector(List(s1), 1.0, 2).threshold shouldBe 0.1
      SmartSelector(List(s1), 1.0, 3).threshold shouldBe 0.0
      SmartSelector(List(s1), 1.0, 4).threshold shouldBe 0.0
    }
  }

  trait Main {
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


    List("1.0").write(s"$tmpDir/$basename#0#0.cost")
    List("1.0").write(s"$tmpDir/$basename#0#1.cost")
    List("1.0").write(s"$tmpDir/$basename#0#2.cost")
    List("1.0").write(s"$tmpDir/$basename#0#3.cost")

    List("1.0").write(s"$tmpDir/$basename#0#0.payoff")
    List("0.0").write(s"$tmpDir/$basename#0#1.payoff")
    List("0.1").write(s"$tmpDir/$basename#0#2.payoff")
    List("1.0").write(s"$tmpDir/$basename#0#3.payoff")

    List("1").write(s"$tmpDir/$basename#0#0.results.local")
    List("2").write(s"$tmpDir/$basename#0#1.results.local")
    List("3").write(s"$tmpDir/$basename#0#2.results.local")
    List("4").write(s"$tmpDir/$basename#0#3.results.local")

    List("1").write(s"$tmpDir/$basename#0#0.results.global")
    List("2").write(s"$tmpDir/$basename#0#1.results.global")
    List("3").write(s"$tmpDir/$basename#0#2.results.global")
    List("4").write(s"$tmpDir/$basename#0#3.results.global")

    List("1.0").write(s"$tmpDir/$basename#0#0.results.scores")
    List("1.2").write(s"$tmpDir/$basename#0#1.results.scores")
    List("1.3").write(s"$tmpDir/$basename#0#2.results.scores")
    List("1.5").write(s"$tmpDir/$basename#0#3.results.scores")


    List("1.0").write(s"$tmpDir/$basename#1#0.cost")
    List("1.0").write(s"$tmpDir/$basename#1#1.cost")
    List("1.0").write(s"$tmpDir/$basename#1#2.cost")
    List("1.0").write(s"$tmpDir/$basename#1#3.cost")

    List("2.0").write(s"$tmpDir/$basename#1#0.payoff")
    List("0.0").write(s"$tmpDir/$basename#1#1.payoff")
    List("0.2").write(s"$tmpDir/$basename#1#2.payoff")
    List("0.0").write(s"$tmpDir/$basename#1#3.payoff")

    List("5").write(s"$tmpDir/$basename#1#0.results.local")
    List("6").write(s"$tmpDir/$basename#1#1.results.local")
    List("7").write(s"$tmpDir/$basename#1#2.results.local")
    List("8").write(s"$tmpDir/$basename#1#3.results.local")

    List("5").write(s"$tmpDir/$basename#1#0.results.global")
    List("6").write(s"$tmpDir/$basename#1#1.results.global")
    List("7").write(s"$tmpDir/$basename#1#2.results.global")
    List("8").write(s"$tmpDir/$basename#1#3.results.global")

    List("0.9").write(s"$tmpDir/$basename#1#0.results.scores")
    List("1.2").write(s"$tmpDir/$basename#1#1.results.scores")
    List("1.3").write(s"$tmpDir/$basename#1#2.results.scores")
    List("1.5").write(s"$tmpDir/$basename#1#3.results.scores")
  }

  /*
   * Uniform costs
   *
   * Impacts:
   *
   * 0: [ 1 | 0 | 0.1 | 1 ]
   * 1: [ 2 | 0 | 0.2 | 0 ]
   */

  test("main: b1, k4") {
    new Main {
      SmartSelector.main(Array(
        s"$tmpDir/$basename",
        "--budget", "1",
        "-k", "4"
      ))

      Lines.fromFile(s"$tmpDir/$basename$BudgetIndicator[1].selection").ofSeq[Int]
        .next() should contain theSameElementsInOrderAs List(0, 1)
      Lines.fromFile(s"$tmpDir/$basename$BudgetIndicator[1].selected.docs").ofSeq[Int]
        .next() should contain theSameElementsInOrderAs List(5)
    }
  }

  test("main: b3, k3") {
    new Main {
      SmartSelector.main(Array(
        s"$tmpDir/$basename",
        "--budget", "3",
        "-k", "3"
      ))

      Lines.fromFile(s"$tmpDir/$basename$BudgetIndicator[3].selection").ofSeq[Int]
        .next() should contain theSameElementsInOrderAs List(1, 1)
      Lines.fromFile(s"$tmpDir/$basename$BudgetIndicator[3].selected.docs").ofSeq[Int]
        .next() should contain theSameElementsInOrderAs List(1, 5)
    }
  }

  test("main: b10, k1") {
    new Main {
      SmartSelector.main(Array(
        s"$tmpDir/$basename",
        "--budget", "10",
        "-k", "1"
      ))

      Lines.fromFile(s"$tmpDir/$basename$BudgetIndicator[10].selection").ofSeq[Int]
        .next() should contain theSameElementsInOrderAs List(0, 1)
      Lines.fromFile(s"$tmpDir/$basename$BudgetIndicator[10].selected.docs").ofSeq[Int]
        .next() should contain theSameElementsInOrderAs List(5)
    }
  }

}
