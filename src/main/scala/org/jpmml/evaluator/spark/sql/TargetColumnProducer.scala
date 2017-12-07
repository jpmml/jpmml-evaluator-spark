package org.jpmml.evaluator.spark.sql

import org.apache.spark.sql.types.StructField
import org.jpmml.evaluator.{Evaluator, TargetField}

case class TargetColumnProducer(override val field: TargetField, override val columnName: String) extends ColumnProducer(field, columnName) {
  override def init(evaluator: Evaluator): StructField = {

    val dataType = field.getDataType

    StructField(columnName, SchemaUtil.translateDataType(dataType), nullable = false)
  }

  override def format(value: Any): Any = org.jpmml.evaluator.EvaluatorUtil.decode(value)
}
