package org.jpmml.evaluator.spark.sql

import org.apache.spark.sql.types.StructField
import org.dmg.pmml.{DataType, Model}
import org.jpmml.evaluator._

case class OutputColumnProducer(override val field: OutputField, override val columnName: String) extends ColumnProducer(field, columnName) {
  private var formatString = false

  override def init(evaluator: Evaluator): StructField = {

    var dataType = field.getDataType
    if (dataType == null) try {
      dataType = OutputUtil.getDataType(field.getOutputField, evaluator.asInstanceOf[ModelEvaluator[_ <: Model]])
      this.formatString = false
    } catch {
      case pe: PMMLException =>
        dataType = DataType.STRING
        this.formatString = true
    }

    StructField(columnName, SchemaUtil.translateDataType(dataType), nullable = false)
  }

  override def format(value: Any): Any = {
    if (this.formatString) value.toString else value
  }
}
