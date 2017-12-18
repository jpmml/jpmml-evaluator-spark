package org.apache.spark.sql

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.catalyst.expressions.{CreateStruct, ScalaUDF}
import org.apache.spark.sql.types.{StructField, StructType}
import org.dmg.pmml.FieldName
import org.jpmml.evaluator.spark.sql.{ColumnProducer, DatasetUtil}
import org.jpmml.evaluator.{Evaluator, FieldValue, ResultField}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


case class PMMLTransformer(evaluator: Evaluator, columnProducers: ArrayBuffer[ColumnProducer[_ <: ResultField]]) extends Transformer {
  var outputSchema = new StructType

  for (columnProducer <- columnProducers) {
    val structField = columnProducer.init(evaluator)
    outputSchema = outputSchema.add(structField)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {

    val inputFields = evaluator.getInputFields

    import scala.collection.JavaConverters._
    val expressions = inputFields.asScala.map {
      inputField =>
        dataset(DatasetUtil.escapeColumnName(inputField.getName.getValue)).expr
    }

    val evaluatorFunction = (row: Row) => {
      val arguments = new mutable.LinkedHashMap[FieldName, FieldValue]

      for (i <- 0 until inputFields.size()) {
        val inputField = inputFields.get(i)
        val name = inputField.getName
        val value = row.get(i)
        val preparedValue = inputField.prepare(value)
        arguments.put(name, preparedValue)
      }
      import scala.collection.JavaConversions._
      val result = evaluator.evaluate(arguments)
      val formattedValues = new ArrayBuffer[Any](columnProducers.size)

      for (i <- columnProducers.indices) {
        val columnProducer = columnProducers(i)
        val resultField = columnProducer.field
        val name = resultField.getName
        val value = result.get(name)
        val formattedValue = columnProducer.format(value)
        formattedValues.append(formattedValue)
      }
      Row(formattedValues: _*)
    }


    val evaluateExpression = ScalaUDF(evaluatorFunction, outputSchema, Seq(CreateStruct(expressions)))

    val outputColumn = new Column(evaluateExpression)

    dataset.withColumn(DatasetUtil.escapeColumnName("pmml"), outputColumn)
  }

  override def copy(extra: ParamMap): Transformer = throw new UnsupportedOperationException

  override def transformSchema(schema: StructType): StructType = {
    val outputField = StructField("pmml", outputSchema, nullable = false)
    schema.add(outputField)
  }

  override val uid: String = null
}
