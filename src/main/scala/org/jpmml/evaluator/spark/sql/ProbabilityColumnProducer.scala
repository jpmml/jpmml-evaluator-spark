package org.jpmml.evaluator.spark.sql

import org.apache.spark.mllib.linalg.{DenseVector, Vector, VectorUDT}
import org.apache.spark.sql.types.StructField
import org.jpmml.evaluator.{Evaluator, HasProbability, TargetField}

case class ProbabilityColumnProducer(override val field: TargetField, override val columnName: String, labels: Array[String]) extends ColumnProducer(field, columnName) {
  override def init(evaluator: Evaluator): StructField = StructField(columnName, new VectorUDT, nullable = false)

  override def format(value: Any): Vector = {

    val hasProbability: HasProbability = value.asInstanceOf[HasProbability]

    val probabilities: Array[Double] = new Array[Double](labels.length)

    for (i <- labels.indices) {
      probabilities(i) = hasProbability.getProbability(labels(i))
    }

    new DenseVector(probabilities)
  }
}
