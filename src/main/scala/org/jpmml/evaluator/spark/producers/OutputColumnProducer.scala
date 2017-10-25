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
package org.jpmml.evaluator.spark.producers

import org.apache.spark.sql.types.{DataTypes, StructField}
import org.dmg.pmml.{DataType, Model}
import org.jpmml.evaluator._
import org.jpmml.evaluator.spark.ColumnProducer
import org.jpmml.evaluator.spark.support.SchemaUtil

object OutputColumnProducer {
  private def getName(field: OutputField) = field.getName.getValue
}

class OutputColumnProducer private[spark](
                                           override val field: OutputField,
                                           override val columnName: String) extends
  ColumnProducer[OutputField](field,
    if (columnName != null)
      columnName
    else
      OutputColumnProducer.getName(field)) {

  private var formatString = false

  override def init(evaluator: Evaluator): StructField = {
    var dataType = field.getDataType

    if (dataType == null)
      try {
        dataType = OutputUtil.getDataType(field.getOutputField, evaluator.asInstanceOf[ModelEvaluator[_ <: Model]])
        formatString = false
      } catch {
        case pe: PMMLException =>
          dataType = DataType.STRING
          formatString = true
      }

    DataTypes.createStructField(columnName, SchemaUtil.translateDataType(dataType), false)
  }

  override def format(value: Any): Any = {
    if (this.formatString)
      value.toString
    else
      value
  }
}