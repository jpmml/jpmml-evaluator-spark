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
package org.jpmml.evaluator.spark.transformers

import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.StructType
import org.jpmml.evaluator.spark.PmmlPipelineModel
import org.jpmml.evaluator.spark.support.DatasetUtil


case class PmmlColumnExploder(val structCol: String) extends Transformer {
  override val uid: String = "exploder"

  override def copy(extra: ParamMap): PmmlColumnExploder = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    val structSchema = getStructSchema(schema)

    var result = schema
    for (field <- structSchema.fields) {
      result = result.add(field)
    }
    result
  }

  override def transform(dataset: Dataset[_]): Dataset[Row] = {
    val structSchema = getStructSchema(dataset.schema)
    val structColumn = dataset.apply(DatasetUtil.escapeColumnName(structCol))

    var dataFrame = dataset.toDF

    for (field <- structSchema.fields) {
      val name = field.name
      val fieldColumn = structColumn.getField(DatasetUtil.escapeColumnName(name))
      dataFrame = dataFrame.withColumn(DatasetUtil.escapeColumnName(name), fieldColumn)
    }

    dataFrame
  }

  private def getStructSchema(schema: StructType): StructType = {
    schema(structCol)
      .dataType
      .asInstanceOf[StructType]
  }
}
