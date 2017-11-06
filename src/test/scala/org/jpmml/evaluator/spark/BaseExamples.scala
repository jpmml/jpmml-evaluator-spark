package org.jpmml.evaluator.spark

import java.io.File

import org.apache.spark.ml.Transformer
import org.jpmml.evaluator.Evaluator
import org.jpmml.evaluator.spark.support.EvaluatorUtil
import org.scalatest.FunSuite

class BaseExamples extends FunSuite {
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
      .withLabelCol("Species") // String column
      .withProbabilityCol("Species_probability", Seq("setosa", "versicolor", "virginica"))
      .withOutputCols
      .exploded(true)

    val pmmlTransformer: PmmlTransformer = pmmlTransformerBuilder.buildTransformer

    assert(pmmlTransformer.uid == "pmml-transformer")
    assert(pmmlTransformer.columnProducers.size == 5)
  }
}
