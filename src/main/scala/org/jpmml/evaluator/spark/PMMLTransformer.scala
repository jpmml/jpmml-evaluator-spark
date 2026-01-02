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

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, FloatType, IntegerType, StringType, StructType}
import org.jpmml.evaluator.{Evaluator, OutputField, TargetField}

import scala.jdk.CollectionConverters._

abstract
class PMMLTransformer(override val uid: String, val evaluator: Evaluator) extends Transformer with DefaultParamsWritable {

	val exceptionCol: Param[String] = new Param[String](this, "exceptionCol", "Exception column name")


	/**
	 * @group getParam
	 */
	def getExceptionCol: String = $(exceptionCol)

	/**
	 * @group setParam
	 */
	def setExceptionCol(value: String): PMMLTransformer = {
		set(exceptionCol, value)
		this
	}


	def this(evaluator: Evaluator) = this(Identifiable.randomUID("pmmlTransformer"), evaluator)

	setDefault(
		exceptionCol -> "exception"
	)

	override
	def transformSchema(schema: StructType): StructType

	protected 
	def buildResultsRow(row: Row, results: java.util.Map[String, _]): Row

	protected
	def buildExceptionRow(row: Row, exception: Exception): Row

	override
	def transform(dataset: Dataset[_]): DataFrame = {
		val df = dataset.toDF()
		val transformedSchema = transformSchema(df.schema)
		val columnIndices = buildColumnIndices(df.schema)

		val resultRdd = df.rdd.mapPartitions(
			partition => partition.map {
				row => {
					val arguments = new LazyRowMap(row, columnIndices)

					try {
						val results = evaluator.evaluate(arguments)

						buildResultsRow(row, results)
					} catch {
						case e: Exception => {
							buildExceptionRow(row, e)
						}
					}
				}
			}
		)

		df.sparkSession.createDataFrame(resultRdd, transformedSchema)
	}

	override
	def copy(extra: ParamMap): PMMLTransformer = {
		defaultCopy(extra)
	}

	private[spark]
	def getTargetFields: Seq[TargetField] = {
		evaluator.getTargetFields.asScala.toSeq
	}

	private[spark]
	def getOutputFields: Seq[OutputField] = {
		evaluator.getOutputFields.asScala.toSeq
	}

	protected
	def toSparkDataType(pmmlDataType: org.dmg.pmml.DataType): DataType = {
		pmmlDataType match {
			case org.dmg.pmml.DataType.STRING => StringType
			case org.dmg.pmml.DataType.INTEGER => IntegerType
			case org.dmg.pmml.DataType.FLOAT => FloatType
			case org.dmg.pmml.DataType.DOUBLE => DoubleType
			case org.dmg.pmml.DataType.BOOLEAN => BooleanType
			case _ => throw new IllegalArgumentException("PMML data type " + pmmlDataType.value() + " is not supported")
		}
	}

	private 
	def buildColumnIndices(schema: StructType): Map[String, Int] = {
		schema.fieldNames.zipWithIndex.toMap
	}
}

class LazyRowMap (
	private val row: Row,
	private val columnIndices: Map[String, Int]
) extends java.util.AbstractMap[String, Object] {

	override
	def get(key: Object): Object = {
		val columnName = key.asInstanceOf[String]

		columnIndices.get(columnName) match {
			case Some(index) => {
				row.get(index).asInstanceOf[Object]
			}
			case None => {
				null
			}
		}
	}

	override
	def entrySet(): java.util.Set[java.util.Map.Entry[String, Object]] = {
		throw new UnsupportedOperationException()
	}
}