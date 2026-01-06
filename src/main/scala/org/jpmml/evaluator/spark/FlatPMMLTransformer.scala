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

import org.apache.spark.ml.util.{Identifiable, MLReadable, MLReader}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.jpmml.evaluator.Evaluator

class FlatPMMLTransformer(override val uid: String, override val evaluator: Evaluator) extends PMMLTransformer(uid, evaluator) {

	def this(evaluator: Evaluator) = this(Identifiable.randomUID("flatPmmlTransformer"), evaluator)

	override
	def pmmlTransformerFields(): Seq[StructField] = {
		pmmlFields() :+ exceptionField()
	}

	override
	protected def buildResultsRow(row: Row, results: java.util.Map[String, _]): Row = {
		Row.fromSeq(inputValues(row) ++ pmmlValues(results) :+ exceptionValue(null))
	}

	override
	protected def buildExceptionRow(row: Row, exception: Exception): Row = {
		Row.fromSeq(inputValues(row) ++ pmmlValues(null) :+ exceptionValue(exception))
	}
}

object FlatPMMLTransformer extends MLReadable[FlatPMMLTransformer] {

	override
	def read: MLReader[FlatPMMLTransformer] = {
		new PMMLTransformerReader[FlatPMMLTransformer]
	}

	override
	def load(path: String): FlatPMMLTransformer = {
		super.load(path)
	}
}