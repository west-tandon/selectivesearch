package edu.nyu.tandon.search.selective.learn

import edu.nyu.tandon.search.selective.Spark
import edu.nyu.tandon.search.selective.data.payoff.Payoffs
import org.apache.spark.ml.regression.RandomForestRegressionModel
import scopt.OptionParser

/**
  * @author michal.siedlaczek@nyu.edu
  */
object PredictPayoffs {

  val CommandName = "predict-payoffs"

  val PredictedLabelColumn = "prediction"

  def main(args: Array[String]): Unit = {

    case class Config(basename: String = null,
                      model: String = null)

    val parser = new OptionParser[Config](CommandName) {

      opt[String]('n', "basename")
        .action((x, c) => c.copy(basename = x))
        .text("the prefix of the files")
        .required()

      opt[String]('m', "model")
        .action((x, c) => c.copy(model = x))
        .text("prediction model")
        .required()

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        Spark.session
        val model = RandomForestRegressionModel.load(config.model)
        val payoffs = Payoffs.fromRegressionModel(config.basename, model)
        payoffs.store(config.basename)

      case None =>
    }
  }

}
