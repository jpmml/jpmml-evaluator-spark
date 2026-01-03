/*
 * Copyright (c) 2026 Villu Ruusmann
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

import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.jpmml.evaluator.{Evaluator, EvaluatorUtil}

class FlatPMMLTransformer(override val uid: String, override val evaluator: Evaluator) extends PMMLTransformer(uid, evaluator) {

	def this(evaluator: Evaluator) = this(Identifiable.randomUID("flatPmmlTransformer"), evaluator)

	override
	def pmmlTransformerFields(): Seq[StructField] = {
		pmmlFields :+ exceptionField
	}

	override
	protected def buildResultsRow(row: Row, results: java.util.Map[String, _]): Row = {
		val targetValues: Seq[Any] = getTargetFields.map {
			targetField => EvaluatorUtil.decode(results.get(targetField.getName))
		}

		val outputValues: Seq[Any] = getOutputFields.map {
			outputField => results.get(outputField.getName)
		}

		val exceptionValue: String = null

		val rowValues: Seq[Any] = row.toSeq ++ targetValues ++ outputValues :+ exceptionValue

		Row.fromSeq(rowValues)
	}

	override
	protected def buildExceptionRow(row: Row, exception: Exception): Row = {
		val targetValues: Seq[Any] = Seq.fill(getTargetFields.size)(null)

		val outputValues: Seq[Any] = Seq.fill(getOutputFields.size)(null)

		val exceptionValue: String = exception.getClass.getName() + ": " + exception.getMessage

		val rowValues: Seq[Any] = row.toSeq ++ targetValues ++ outputValues :+ exceptionValue

		Row.fromSeq(rowValues)
	}
}