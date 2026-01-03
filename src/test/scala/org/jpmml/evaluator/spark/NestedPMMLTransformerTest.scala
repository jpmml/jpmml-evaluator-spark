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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}
import org.jpmml.evaluator.Evaluator

class NestedPMMLTransformerTest extends PMMLTransformerTest {

	override
	protected def createPmmlTransformer(evaluator: Evaluator): NestedPMMLTransformer = {
		new NestedPMMLTransformer(evaluator)
	}

	test("DecisionTreeIris with Iris"){
		val evaluator = loadEvaluator("DecisionTreeIris.pmml")
		val pmmlTransformer = createPmmlTransformer(evaluator)

		val df = loadDataFrame("Iris.csv")

		checkDecisionTreeIris(pmmlTransformer, df, 150, 0)
	}

	test("DecisionTreeIris with IrisInvalid"){
		val evaluator = loadEvaluator("DecisionTreeIris.pmml")
		val pmmlTransformer = createPmmlTransformer(evaluator)

		val df = loadDataFrame("IrisInvalid.csv")

		checkDecisionTreeIris(pmmlTransformer, df, 147, 3)
	}

	def checkDecisionTreeIris(pmmlTransformer: NestedPMMLTransformer, df: DataFrame, successCount: Int, failureCount: Int): Unit = {
		val schema = df.schema
		val pmmlSchema = pmmlTransformer.transformSchema(schema)

		checkDecisionTreeIris(pmmlTransformer, schema, pmmlSchema)

		val pmmlDf = pmmlTransformer.transform(df)

		pmmlDf.count() shouldBe df.count()

		checkDecisionTreeIris(pmmlTransformer, df.schema, pmmlDf.schema)

		pmmlDf.filter(pmmlDf(pmmlTransformer.getResultsCol).isNotNull).count() shouldBe successCount
		pmmlDf.filter(pmmlDf(pmmlTransformer.getResultsCol).isNull).count() shouldBe failureCount

		pmmlDf.filter(pmmlDf(pmmlTransformer.getExceptionCol).isNotNull).count() shouldBe failureCount
		pmmlDf.filter(pmmlDf(pmmlTransformer.getExceptionCol).isNull).count() shouldBe successCount
	}

	def checkDecisionTreeIris(pmmlTransformer: NestedPMMLTransformer, schema: StructType, pmmlSchema: StructType): Unit = {
		val columns = schema.fieldNames
		val pmmlColumns = pmmlSchema.fieldNames

		pmmlColumns.size shouldBe (columns.size + 1 + 1)

		columns.foreach {
			colName => pmmlColumns should contain(colName)
		}

		pmmlColumns should contain(pmmlTransformer.getResultsCol)

		val resultsType = pmmlSchema(pmmlTransformer.getResultsCol).dataType.asInstanceOf[StructType]
		resultsType.fieldNames.size shouldBe (1 + 3)

		pmmlTransformer.getTargetFields.foreach {
			targetField => checkPmmlField(resultsType(targetField.getName), StringType)
		}

		pmmlTransformer.getOutputFields.foreach {
			outputField => checkPmmlField(resultsType(outputField.getName), DoubleType)
		}

		pmmlColumns should contain(pmmlTransformer.getExceptionCol)

		checkExceptionField(pmmlSchema(pmmlTransformer.getExceptionCol))
	}
}