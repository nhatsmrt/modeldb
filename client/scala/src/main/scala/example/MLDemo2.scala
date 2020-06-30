package example

import ai.verta.client._
import ai.verta.client.entities._
import ai.verta.repository._
import ai.verta.blobs._
import ai.verta.blobs.dataset._

import scala.concurrent.ExecutionContext
import scala.util.{Try, Success, Failure}

import java.io.File

import org.apache.spark.ml._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.types._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics

object MLDemo2 extends App {
  implicit val ec = ExecutionContext.global

  val client = new Client(ClientConnection.fromEnvironment())

  val sparkSession = SparkSession.builder.master("local")
    .appName("spark session example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("OFF")

  def deleteDirectory(dir: File): Unit = {
    Option(dir.listFiles()).map(_.foreach(deleteDirectory))
    dir.delete()
  }

  try {
    // get the repository
    val repo = client.getOrCreateRepository("IrisDemo").get
    val commit = repo.getCommitByBranch("data").get
    val project = client.getOrCreateProject("Iris Demo").get
    val expRun = project.getOrCreateExperiment("Logistic Regression experiment")
                        .flatMap(_.getOrCreateExperimentRun()).get

    val irisBlob: Dataset = commit.get("iris").get match { case path: Dataset => path }
    irisBlob.download(Some("/Users/nhat/Documents/data/iris/iris.parquet"), Some("iris/iris.parquet"))
    val irisDF = sparkSession.read.parquet("iris/iris.parquet")

    // split data into train and test set:
    val splits = irisDF.randomSplit(Array(0.8, 0.2), 2221999)
    val trainDF = splits(0)
    val testDF = splits(1)

    // declare some hyperparameters:
    val maxIter = 120 // how many iters to train the model
    val regParam = 0.3 // regularizer strength
    val elasticNetParam = 0.8 // control mixing between L1 and L2 regularization

    // train a logistic regression on the train data:
    val model = new LogisticRegression()
      .setMaxIter(maxIter)
      .setRegParam(regParam)
      .setElasticNetParam(elasticNetParam)
      .fit(trainDF)

    // save the model:
    expRun.logArtifactObj("model", model)

    // load the model:
    val loadedModel: LogisticRegressionModel = expRun.getArtifactObj("model").get match {
      case logRegModel: LogisticRegressionModel => logRegModel
    }

    // evaluate on the test set:
    val predictionAndLabels = loadedModel
      .transform(testDF).cache()

    val predictions = predictionAndLabels.select("prediction").rdd.map(_.getDouble(0))
    val labels = predictionAndLabels.select("label").rdd.map(_.getDouble(0))

    val metrics = new MulticlassMetrics(predictions.zip(labels))

    expRun.logHyperparameters(Map(
      "maxIter" -> maxIter,
      "regParam" -> regParam,
      "elasticNetParam" -> elasticNetParam
    ))
    expRun.logMetric("accuracy", metrics.accuracy)
    expRun.logCommit(commit, Some(Map("data" -> "iris")))
  } finally {
    client.close()
    deleteDirectory(new File("iris"))
    sparkSession.stop()
  }
}
