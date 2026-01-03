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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, StringType, StructField}
import org.jpmml.evaluator.{Evaluator, LoadingModelEvaluatorBuilder}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

abstract
class PMMLTransformerTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

	var spark: SparkSession = _


	protected
	def createPmmlTransformer(evaluator: Evaluator): PMMLTransformer

	override
	def beforeAll(): Unit = {
		spark = SparkSession.builder
			.master("local[*]")
			.appName("PMMLTransformerTest")
			.getOrCreate
	}

	override
	def afterAll(): Unit = {
		if (spark != null) {
			spark.stop()
		}
	}

	protected
	def loadEvaluator(pmmlPath: String): Evaluator = {
		val inputStream = getClass.getClassLoader.getResourceAsStream(pmmlPath)

		try {
			new LoadingModelEvaluatorBuilder()
				.load(inputStream)
				.build()
		} finally {
			inputStream.close
		}
	}

	protected
	def loadDataFrame(csvPath: String): DataFrame = {
		val resource = getClass.getClassLoader.getResource(csvPath)

		spark.read
			.option("header", "true")
			.option("inferSchema", "true")
			.option("nanValue", "NaN")
			.csv(resource.getPath)
	}

	protected
	def checkPmmlField(pmmlField: StructField, dataType: DataType): Unit = {
		pmmlField.dataType shouldBe dataType
		pmmlField.nullable shouldBe true
	}

	protected
	def checkExceptionField(exceptionField: StructField): Unit = {
		exceptionField.dataType shouldBe StringType
		exceptionField.nullable shouldBe true
	}
}