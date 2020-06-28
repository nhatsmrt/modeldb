package example

import ai.verta.client._
import ai.verta.client.entities._
import ai.verta.repository._
import ai.verta.blobs._
import ai.verta.blobs.dataset._

import scala.concurrent.ExecutionContext
import scala.util.{Try, Success, Failure}

import org.apache.spark.ml._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode

object MLDemo extends App {
  def castAll(df: DataFrame, columns: List[String], outputType: DataType): DataFrame = {
    if (columns.isEmpty) df
    else castAll(df.withColumn(columns.head, df(columns.head).cast(outputType)), columns.tail, outputType)
  }

  implicit val ec = ExecutionContext.global

  val client = new Client(ClientConnection.fromEnvironment())

  val sparkSession = SparkSession.builder.master("local")
    .appName("spark session example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("OFF")

  try {
    // Load the data:
    val irisRawDF = sparkSession
      .read
      .format("csv")
      .option("header", "true")
      .load("/Users/nhat/Documents/data/iris/iris.csv")

    // cast columns to float
    val irisFloatDF =
      castAll(irisRawDF, List("sepal_length", "sepal_width", "petal_length", "petal_width"), FloatType)

    // Convert string label to numeric
    val indexer = new StringIndexer()
      .setInputCol("species")
      .setOutputCol("label")
    val irisIndexedDF = indexer.fit(irisFloatDF).transform(irisFloatDF)

    // Assembling the columns:
    val assembler = new VectorAssembler()
      .setInputCols(Array("sepal_length", "sepal_width", "petal_length", "petal_width"))
      .setOutputCol("features")

    val transformedIrisDF = assembler.transform(irisIndexedDF)
      .select("features", "label")
    transformedIrisDF.show()

    // save the data:
    transformedIrisDF
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/Users/nhat/Documents/data/iris/iris.parquet")

    // create a repository
    val repo = client.getOrCreateRepository("IrisDemo").get

    // Create the data blob:
    val irisBlob = PathBlob("/Users/nhat/Documents/data/iris/iris.parquet", true).get

    // Save the blobs in a commit and save the commit to ModelDB:
    repo.getCommitByBranch()
      .flatMap(_.newBranch("data"))
      .flatMap(_.update("iris", irisBlob))
      .flatMap(_.save("upload data")).get
  } finally {
    client.close()
    sparkSession.stop()
  }
}
