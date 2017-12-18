package org.jpmml.evaluator.spark.sql

import org.apache.spark.sql.types.StructField
import org.jpmml.evaluator.{Evaluator, ResultField}

abstract class ColumnProducer[F <: ResultField](val field: F, val columnName: String) extends Serializable {

  def init(evaluator: Evaluator): StructField

  def format(value: Any): Any

}
