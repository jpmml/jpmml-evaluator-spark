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

import java.io.{ObjectInputStream, ObjectOutputStream}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{BooleanParam, Param, ParamMap}
import org.apache.spark.ml.util.{Identifiable, MLReader, MLWritable, MLWriter}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.ml.param.ParamPair
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, FloatType, IntegerType, StringType, StructField, StructType}
import org.jpmml.evaluator.{Evaluator, EvaluatorUtil, OutputField, TargetField}

import scala.io.Source
import scala.jdk.CollectionConverters._

abstract
class PMMLTransformer(override val uid: String, val evaluator: Evaluator) extends Transformer with MLWritable {

	/**
	* @group param
	*/
	val inputs: BooleanParam = new BooleanParam(this, "inputs", "Copy input columns")

	/**
	 * @group param
	 */
	val targets: BooleanParam = new BooleanParam(this, "targets", "Produce columns for PMML target fields")

	/**
	 * @group param
	 */
	val outputs: BooleanParam = new BooleanParam(this, "outputs", "Produce columns for PMML output fields")

	/**
	 * @group param
	 */
	val exceptionCol: Param[String] = new Param[String](this, "exceptionCol", "Exception column name")

	/**
	 * @group param
	 */
	val syntheticTargetName: Param[String] = new Param[String](this, "syntheticTargetName", "Name for a synthetic target field")


	/**
	 * @group getParam
	 */
	def getInputs: Boolean = $(inputs)

	/**
	 * @group setParam
	 */
	def setInputs(value: Boolean): this.type = set(inputs, value)

	/**
	 * @group getParam
	 */
	def getTargets: Boolean = $(targets)

	/**
	 * @group setParam
	 */
	def setTargets(value: Boolean): this.type = set(targets, value)

	/**
	 * @group getParam
	 */
	def getOutputs: Boolean = $(outputs)

	/**
	 * @group setParam
	 */
	def setOutputs(value: Boolean): this.type = set(outputs, value)

	/**
	 * @group getParam
	 */
	def getExceptionCol: String = $(exceptionCol)

	/**
	 * @group setParam
	 */
	def setExceptionCol(value: String): this.type = set(exceptionCol, value)

	/**
	 * @group getParam
	 */
	def getSyntheticTargetName: String = $(syntheticTargetName)

	/**
	 * @group setParam
	 */
	def setSyntheticTargetName(value: String): this.type = set(syntheticTargetName, value)


	def this(evaluator: Evaluator) = this(Identifiable.randomUID("pmmlTransformer"), evaluator)

	setDefault(
		inputs -> true,
		targets -> true,
		outputs -> true,
		exceptionCol -> "exception",
		syntheticTargetName -> "_target"
	)

	override
	def transformSchema(schema: StructType): StructType = {
		StructType(inputFields(schema) ++ pmmlTransformerFields)
	}

	protected
	def inputFields(schema: StructType): Seq[StructField] = {

		if(getInputs){
			schema.fields
		} else

		{
			Seq.empty
		}
	}

	protected
	def pmmlTransformerFields(): Seq[StructField]

	protected
	def pmmlFields(): Seq[StructField] = {
		val targetFields: Seq[StructField] = getTargetFields.map {
			targetField => StructField(if(targetField.isSynthetic) getSyntheticTargetName else targetField.getName, toSparkDataType(targetField.getDataType), true)
		}

		val outputFields: Seq[StructField] = getOutputFields.map {
			outputField => StructField(outputField.getName, toSparkDataType(outputField.getDataType), true)
		}

		targetFields ++ outputFields
	}

	protected
	def exceptionField(): StructField = {
		StructField(getExceptionCol, StringType, true)
	}

	override
	def transform(dataset: Dataset[_]): DataFrame = {
		val df = dataset.toDF
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

	protected 
	def buildResultsRow(row: Row, results: java.util.Map[String, _]): Row

	protected
	def buildExceptionRow(row: Row, exception: Exception): Row

	protected
	def inputValues(row: Row): Seq[Any] = {

		if(getInputs){
			row.toSeq
		} else

		{
			Seq.empty
		}
	}

	protected
	def pmmlValues(results: java.util.Map[String, _]): Seq[Any] = {

		if(results != null){
			val targetValues: Seq[Any] = getTargetFields.map {
				targetField => EvaluatorUtil.decode(results.get(targetField.getName))
			}

			val outputValues: Seq[Any] = getOutputFields.map {
				outputField => results.get(outputField.getName)
			}

			targetValues ++ outputValues
		} else

		{
			Seq.fill(getTargetFields.size + getOutputFields.size)(null)
		}
	}

	protected
	def exceptionValue(exception: Exception): String = {

		if(exception != null){
			exception.getClass.getName + ": " + exception.getMessage
		} else

		{
			null
		}
	}

	override
	def write: MLWriter = {
		new PMMLTransformerWriter(this)
	}

	override
	def copy(extra: ParamMap): PMMLTransformer = {
		defaultCopy(extra)
	}

	private[spark]
	def getTargetFields: Seq[TargetField] = {

		if(getTargets){
			evaluator.getTargetFields.asScala.toSeq	
		} else

		{
			Seq.empty
		}
	}

	private[spark]
	def getOutputFields: Seq[OutputField] = {

		if(getOutputs){
			evaluator.getOutputFields.asScala.toSeq
		} else

		{
			Seq.empty
		}
	}

	protected
	def toSparkDataType(pmmlDataType: org.dmg.pmml.DataType): DataType = {
		pmmlDataType match {
			case org.dmg.pmml.DataType.STRING => StringType
			case org.dmg.pmml.DataType.INTEGER => IntegerType
			case org.dmg.pmml.DataType.FLOAT => FloatType
			case org.dmg.pmml.DataType.DOUBLE => DoubleType
			case org.dmg.pmml.DataType.BOOLEAN => BooleanType
			case _ => throw new IllegalArgumentException("PMML data type " + pmmlDataType.value + " is not supported")
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
		throw new UnsupportedOperationException
	}
}

class PMMLTransformerWriter(instance: PMMLTransformer) extends MLWriter {

	override
	def saveImpl(path: String): Unit = {
		val fs = new Path(path).getFileSystem(sc.hadoopConfiguration)

		val metadataPath = new Path(path, "metadata")
		val evaluatorPath = new Path(path, "evaluator")

		val paramMap = instance.extractParamMap.toSeq.map {
			case ParamPair(p, v) => p.name -> p.jsonEncode(v)
		}.toMap

		val metadata: Map[String, Any] = Map(
			"class" -> instance.getClass.getName,
			"uid" -> instance.uid,
			"timestamp" -> System.currentTimeMillis,
			"sparkVersion" -> sc.version,
			"paramMap" -> paramMap
		)

		val mapper = new ObjectMapper
		mapper.registerModule(DefaultScalaModule)
		val metadataJson = mapper.writeValueAsString(metadata)

		val metadataOs = fs.create(metadataPath)
		try {
			metadataOs.write(metadataJson.getBytes("UTF-8"))
		} finally {
			metadataOs.close
		}

		val evaluatorOs = new ObjectOutputStream(fs.create(evaluatorPath))
		try {
			evaluatorOs.writeObject(instance.evaluator)
		} finally {
			evaluatorOs.close
		}
	}
}

class PMMLTransformerReader[T <: PMMLTransformer] extends MLReader[T] {

	override
	def load(path: String): T = {
		val fs = new Path(path).getFileSystem(sc.hadoopConfiguration)

		val metadataPath = new Path(path, "metadata")
		val evaluatorPath = new Path(path, "evaluator")

		val metadataJson = {
			val metadataIs = fs.open(metadataPath)
			try {
				Source.fromInputStream(metadataIs)("UTF-8").mkString
			} finally {
				metadataIs.close
			}
		}

		val mapper = new ObjectMapper()
		mapper.registerModule(DefaultScalaModule)
		val metadata = mapper.readValue(metadataJson, classOf[Map[String, Any]])

		val evaluator = {
			val evaluatorIs = new ObjectInputStream(fs.open(evaluatorPath))
			try {
				evaluatorIs.readObject().asInstanceOf[Evaluator]
			} finally {
				evaluatorIs.close
			}
		}

		val className = metadata("class").asInstanceOf[String]
		val uid = metadata("uid").asInstanceOf[String]
		val paramMap = metadata("paramMap").asInstanceOf[Map[String, String]]

		val clazz = Class.forName(className)
		val instance = clazz.getConstructor(classOf[String], classOf[Evaluator]).newInstance(uid, evaluator).asInstanceOf[PMMLTransformer]

		paramMap.foreach {
			case (name, jsonValue) => {
				val param = instance.getParam(name)
				val value = param.jsonDecode(jsonValue)
				instance.set(param, value)
			}
		}

		instance.asInstanceOf[T]
	}
}
