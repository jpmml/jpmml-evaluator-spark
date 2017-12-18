package org.apache.spark.ml

import org.apache.spark.sql.PMMLTransformer
import org.dmg.pmml.ResultFeature
import org.jpmml.evaluator.spark._
import org.jpmml.evaluator.spark.sql._
import org.jpmml.evaluator.{Evaluator, ResultField}

import scala.collection.mutable.ArrayBuffer

case class TransformerBuilder(evaluator: Evaluator) {

  private val columnProducers: ArrayBuffer[ColumnProducer[_ <: ResultField]] = new ArrayBuffer[ColumnProducer[_ <: ResultField]]

  private var exploded: Boolean = false

  def withTargetCols(): TransformerBuilder = {
    import scala.collection.JavaConverters._
    val targetFields = evaluator.getTargetFields
    for (targetField <- targetFields.asScala) {
      this.columnProducers.append(TargetColumnProducer(targetField,
        if (targetField.isSynthetic) "_target" else targetField.getName.getValue)
      )
    }
    this
  }

  def withLabelCol(columnName: String): TransformerBuilder = {
    val targetFields = evaluator.getTargetFields
    if (targetFields.size != 1) throw new IllegalArgumentException
    val targetField = targetFields.get(0)
    this.columnProducers.append(TargetColumnProducer(targetField, columnName))
    this
  }

  def withProbabilityCol(columnName: String): TransformerBuilder = withProbabilityCol(columnName, null)

  def withProbabilityCol(columnName: String, labels: Array[String]): TransformerBuilder = {
    val targetFields = evaluator.getTargetFields
    if (targetFields.size != 1) throw new IllegalArgumentException
    val targetField = targetFields.get(0)
    val values = new ArrayBuffer[String]
    val outputFields = evaluator.getOutputFields
    import scala.collection.JavaConversions._
    for (outputField <- outputFields) {
      val pmmlOutputField = outputField.getOutputField
      val resultFeature = pmmlOutputField.getResultFeature
      resultFeature match {
        case ResultFeature.PROBABILITY =>
          val value = pmmlOutputField.getValue
          if (value != null) values.append(value)
        case _ =>
      }
    }
    if (values.isEmpty) {
      throw new IllegalArgumentException
    }
    if (labels != null && (labels.length != values.size || !labels.toList.containsAll(values))) throw new IllegalArgumentException

    this.columnProducers.append(ProbabilityColumnProducer(targetField, columnName, if (labels != null) labels else values.toArray))
    this
  }

  def withOutputCols(): TransformerBuilder = {
    val outputFields = evaluator.getOutputFields
    import scala.collection.JavaConversions._
    for (outputField <- outputFields) {
      this.columnProducers.append(OutputColumnProducer(outputField, outputField.getName.getValue))
    }
    this
  }

  def exploded(exploded: Boolean): TransformerBuilder = {
    this.exploded = exploded
    this
  }

  def build: Transformer = {
    val pmmlTransformer = PMMLTransformer(evaluator, this.columnProducers)
    if (this.exploded) {
      val columnExploder = ColumnExploder("pmml")
      val pipelineModel = new PipelineModel(null, Array[Transformer](pmmlTransformer, columnExploder))
      pipelineModel
    } else
      pmmlTransformer
  }
}
