package org.jpmml.evaluator.spark

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jpmml.evaluator.Evaluator
import org.jpmml.evaluator.clustering.ClusteringModelEvaluator
import org.jpmml.evaluator.spark.support.EvaluatorUtil
import org.scalatest.FunSuite

object PredictionTest {
  case class Record(sepal_length: Double, sepal_width: Double, petal_length: Double, petal_width: Double)
}

class BaseExamples extends FunSuite {
  import PredictionTest._

  test("Test a regression model is building") {
    val pmmlFile: File = new File(getClass.getClassLoader.getResource("regression.pmml").getFile)

    val evaluator = EvaluatorUtil.createEvaluator(pmmlFile)

    val pmmlTransformerBuilder = new TransformerBuilder(evaluator)
      .withTargetCols
      .withOutputCols
      .exploded(false)

    val pmmlTransformer: PmmlTransformer = pmmlTransformerBuilder.buildTransformer

    assert(pmmlTransformer.uid == "pmml-transformer")
    assert(pmmlTransformer.columnProducers.size == 1)
    assert(pmmlTransformer.outputCol == "pmml")
  }

  test("Test a regression model is built with label col") {
    val pmmlFile: File = new File(getClass.getClassLoader.getResource("regression.pmml").getFile)

    val evaluator = EvaluatorUtil.createEvaluator(pmmlFile)

    val pmmlTransformerBuilder = new TransformerBuilder(evaluator)
      .withLabelCol("MPG") // Double column
      .exploded(true)

    val pmmlTransformer: PmmlTransformer = pmmlTransformerBuilder.buildTransformer

    assert(pmmlTransformer.uid == "pmml-transformer")
    assert(pmmlTransformer.columnProducers.size == 1)
    assert(pmmlTransformer.columnProducers.head.columnName == "MPG")
  }

  test("Test a clustering model is correctly interpreted") {
    val pmmlFile: File = new File(getClass.getClassLoader.getResource("clustering.pmml").getFile)
    val evaluator = EvaluatorUtil.createEvaluator(pmmlFile)
    val pmmlTransformerBuilder = new TransformerBuilder(evaluator)
      .withTargetCols
      .withOutputCols
      .exploded(false)

    implicit val sparkSession: SparkSession = SparkSession
      .builder()
      .config(new SparkConf()
        .setAppName("iris").setMaster("local"))
      .getOrCreate()
    import sparkSession.implicits._

    val pmmlTransformer: PmmlTransformer = pmmlTransformerBuilder.buildTransformer

    assert(pmmlTransformer.uid == "pmml-transformer")
    assert(pmmlTransformer.columnProducers.size == 3)

    val dataSet = sparkSession.sparkContext.parallelize(Seq(
      Record(1, 2, 3, 4),
      Record(5, 6, 7, 8)
    ))

    val frame: DataFrame = sparkSession.createDataFrame(dataSet)
    frame.show()

    val r = pmmlTransformer.transform(frame)
    r.show()
  }
}
