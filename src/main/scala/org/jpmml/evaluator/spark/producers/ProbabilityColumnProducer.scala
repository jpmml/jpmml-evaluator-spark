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

import org.apache.spark.mllib.linalg.{DenseVector, Vector, VectorUDT}
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.jpmml.evaluator.spark.ColumnProducer
import org.jpmml.evaluator.{Evaluator, HasProbability, TargetField}


class ProbabilityColumnProducer private[spark](override val field: TargetField,
                                               override val columnName: String,
                                               val labels: Seq[String]) extends
  ColumnProducer[TargetField](field,
    if (columnName != null)
      columnName else
    "probability") {

  override def init(evaluator: Evaluator): StructField =
    DataTypes.createStructField(columnName, new VectorUDT, false)

  override def format(value: Any): Vector = {
    val hasProbability = value.asInstanceOf[HasProbability]

    new DenseVector(labels.map(l => hasProbability.getProbability(l).doubleValue()).toArray)
  }
}