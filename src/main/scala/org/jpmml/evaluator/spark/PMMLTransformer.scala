/*
 * Copyright (c) 2018 Interset Software Inc
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its contributors
 *    may be used to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.jpmml.evaluator.spark

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Column, Dataset, Row}
import org.jpmml.evaluator.{Evaluator, InputField, ResultField}

import scala.collection.JavaConverters._

// Use a Java list for the ColumnProducers to make the constructor API easy to use from Java
// We'll just convert to Scala where needed internally
class PMMLTransformer(evaluator: Evaluator, columnProducers: java.util.List[ColumnProducer[_ <: ResultField]]) extends Transformer {

	override val uid: String = "pmml-transformer"
	def getOutputCol = "pmml"

	// Initialize the output schema based on the ColumnProducers
	private val outputSchema: StructType = {
		columnProducers
			.asScala
			.foldLeft(new StructType()){ (schema, cp) =>
				schema.add(cp.init(evaluator))
			}
	}

	override def transformSchema(schema: StructType): StructType = {
		schema.add(
			DataTypes.createStructField(getOutputCol, outputSchema, false)
		)
	}

	// Transformer method that evaluates the PMML against each row
	override def transform(ds: Dataset[_]): Dataset[Row] = {
		// Helper evaluation function, takes in a row and returns a row
		def evaluationFunction: (Row) => Row = {
			row => {
				val input = evaluator.getInputFields.asScala

				val arguments = input.zipWithIndex.map { case (inputField, index) =>
					// The row here is pruned to just be the input fields, so this lines up correctly
					val inputField = input(index)
					val rowElement = row(index)

					(inputField.getName, inputField.prepare(rowElement))
				}

				// Evaluate the arguments
				val resultMap = evaluator.evaluate(arguments.toMap.asJava).asScala

				// Format the values from the map
				val formattedValues = columnProducers.asScala.map{ cp =>
					cp.format( resultMap(cp.getField.getName) )
				}

				// Create a Row from the formatted values
				Row.fromSeq(formattedValues)
			}
		}

		// Map input fields to Dataset Columns, and escape special characters
		val columns: Seq[Column] = evaluator.getInputFields.asScala.map { inputField: InputField =>
			ds(
				DatasetUtil.escapeColumnName(inputField.getName.getValue)
			)
		}

		// Create a UDF to perform the evaluation
		import org.apache.spark.sql.functions._
		val udfFn = udf(evaluationFunction, outputSchema)

		// Wrap the columns in a struct() so the function executes on a Row
		val udfCol = udfFn(struct(columns:_*))

		// Execute the PMML by including the evaluation as a new column (getOutputCol)
		ds.withColumn(
			colName = DatasetUtil.escapeColumnName(getOutputCol),
			col = udfCol
		)
	}

	override def copy(extra: ParamMap): Transformer = throw new UnsupportedOperationException
}
