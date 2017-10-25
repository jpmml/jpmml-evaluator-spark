/*
 * Copyright (c) 2016 Villu Ruusmann
 *
 * This file is part of JPMML-Evaluator
 *
 * JPMML-Evaluator is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * JPMML-Evaluator is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with JPMML-Evaluator.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.jpmml.evaluator.spark

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.{Model, PipelineModel, Transformer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{MLWritable, MLWriter}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, RowFactory}
import org.apache.spark.sql.catalyst.expressions.{CreateStruct, Expression, ScalaUDF}
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}
import org.dmg.pmml.FieldName
import org.jpmml.evaluator.spark.support.DatasetUtil
import org.jpmml.evaluator.spark.transformers.PmmlColumnExploder
import org.jpmml.evaluator.{Evaluator, FieldValue, InputField, ResultField}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import java.{util => ju}
/**
  * A pmml transformer builds a pipeline from a JPMML Evaluator
  *
  * @param evaluator       the JMML evaluator we will work with in order to build the final dataset
  * @param columnProducers list of [[ColumnProducer]] we will work with, in order to build the output schema as well as the formatted values
  * @param outputCol       the output column which will be "pmml" by default and will be analyzed by [[PmmlColumnExploder]] and [[org.apache.spark.ml.feature.ColumnPruner]]
  */
case class PmmlTransformer(
                            evaluator: Evaluator,
                            columnProducers: List[ColumnProducer[_ <: ResultField]],
                            outputCol: String = "pmml") extends Transformer {
  if (outputCol == null)
    throw new IllegalArgumentException

  /**
    * The output schema which is derived from the producers
    * @todo Use fold
    */
  private val outputSchema: StructType = {
    var schema = new StructType
    for (columnProducer <- columnProducers) {
      val structField = columnProducer.init(evaluator)
      schema = schema.add(structField)
    }

    schema
  }

  val getInputCols: Array[String] = {
    evaluator.getActiveFields.map(_.getName.getValue).toArray
  }

  override val uid = "pmml"

  override def copy(extra: ParamMap) = throw new UnsupportedOperationException

  override def transformSchema(schema: StructType): StructType = {
    schema.add(DataTypes.createStructField(outputCol, outputSchema, false))
  }

  /** Evaluation function which will be used by the scala udf
    * @todo use zip
    */
  private val evaluationFunction: (Row) => Row = {
    row => {
      val input = evaluator.getInputFields.asScala

      val arguments: Map[FieldName, FieldValue] = input.zipWithIndex.map { case (inputField, index) =>
        val inputField = input(index)
        val rowElement = row(index)

        (inputField.getName, inputField.prepare(rowElement))
      }.toMap

      val result: mutable.Map[FieldName, _] = evaluator.evaluate(arguments.asJava).asScala

      val formattedvalues = columnProducers
        .map(columnProducer => {
          columnProducer.format(result(columnProducer.field.getName))
        }).toArray

      RowFactory.create(formattedvalues)
    }
  }

  /** Transform incoming dataset to a dataset of rows (dataframe) **/
  override def transform(dataset: Dataset[_]): Dataset[Row] = {
    def escapeColumns: (InputField) => Expression = {
      (inputField: InputField) =>
        dataset(
          DatasetUtil.escapeColumnName(inputField.getName.getValue)
        ).expr
    }

    val udf = ScalaUDF(
      function = evaluationFunction,
      dataType = outputSchema,
      children = Seq[Expression](
        CreateStruct(evaluator.getInputFields.asScala.map(escapeColumns))),
      inputTypes = Seq[DataType]())

    dataset.withColumn(
      colName = DatasetUtil.escapeColumnName(outputCol),
      col = new Column(udf))
  }
}


