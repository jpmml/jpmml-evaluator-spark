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

import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLReader}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType, StringType}
import org.jpmml.evaluator.{Evaluator, EvaluatorUtil}

class NestedPMMLTransformer(override val uid: String, override val evaluator: Evaluator) extends PMMLTransformer(uid, evaluator) {

	val resultsCol: Param[String] = new Param[String](this, "resultsCol", "Results column name")


	/**
	 * @group getParam
	 */
	def getResultsCol: String = $(resultsCol)

	/**
	 * @group setParam
	 */
	def setResultsCol(value: String): NestedPMMLTransformer = {
		set(resultsCol, value)
		this
	}


	def this(evaluator: Evaluator) = this(Identifiable.randomUID("nestedPmmlTransformer"), evaluator)

	setDefault(
		resultsCol -> "results"
	)

	override
	def pmmlTransformerFields(): Seq[StructField] = {
		Seq(resultsField, exceptionField)
	}

	def resultsField(): StructField = {
		StructField(getResultsCol, StructType(pmmlFields.toArray), true)
	}

	override
	protected def buildResultsRow(row: Row, results: java.util.Map[String, _]): Row = {
		val exceptionValue: String = null

		Row.fromSeq(row.toSeq :+ Row.fromSeq(pmmlValues(results)) :+ exceptionValue)
	}

	override
	protected def buildExceptionRow(row: Row, exception: Exception): Row = {
		val exceptionValue: String = exception.getClass.getName + ": " + exception.getMessage

		Row.fromSeq(row.toSeq :+ null :+ exceptionValue)
	}
}

object NestedPMMLTransformer extends MLReadable[NestedPMMLTransformer] {

	override
	def read(): MLReader[NestedPMMLTransformer] = {
		new PMMLTransformerReader[NestedPMMLTransformer]
	}

	override
	def load(path: String): NestedPMMLTransformer = {
		super.load(path)
	}
}