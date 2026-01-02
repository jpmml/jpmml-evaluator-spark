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
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.jpmml.evaluator.Evaluator

class FlatPMMLTransformerTest extends PMMLTransformerTest {

	override
	protected def createPmmlTransformer(evaluator: Evaluator): FlatPMMLTransformer = {
		new FlatPMMLTransformer(evaluator)
	}

	test("DecisionTreeIris with Iris"){
		val evaluator = loadEvaluator("DecisionTreeIris.pmml")
		val pmmlTransformer = createPmmlTransformer(evaluator)

		val df = loadDataFrame("Iris.csv")
		val pmmlDf = pmmlTransformer.transform(df)

		checkDecisionTreeIris(pmmlTransformer, df, pmmlDf)

		pmmlDf.filter(pmmlDf("Species").isNotNull).count() shouldBe 150
		pmmlDf.filter(pmmlDf("Species").isNull).count() shouldBe 0

		pmmlDf.filter(pmmlDf(pmmlTransformer.getExceptionCol).isNotNull).count() shouldBe 0
		pmmlDf.filter(pmmlDf(pmmlTransformer.getExceptionCol).isNull).count() shouldBe 150
	}

	test("DecisionTreeIris with IrisInvalid"){
		val evaluator = loadEvaluator("DecisionTreeIris.pmml")
		val pmmlTransformer = createPmmlTransformer(evaluator)

		val df = loadDataFrame("IrisInvalid.csv")
		val pmmlDf = pmmlTransformer.transform(df)

		checkDecisionTreeIris(pmmlTransformer, df, pmmlDf)

		pmmlDf.filter(pmmlDf("Species").isNotNull).count() shouldBe 147
		pmmlDf.filter(pmmlDf("Species").isNull).count() shouldBe 3

		pmmlDf.filter(pmmlDf(pmmlTransformer.getExceptionCol).isNotNull).count() shouldBe 3
		pmmlDf.filter(pmmlDf(pmmlTransformer.getExceptionCol).isNull).count() shouldBe 147
	}

	def checkDecisionTreeIris(pmmlTransformer: FlatPMMLTransformer, df: DataFrame, pmmlDf: DataFrame): Unit = {
		pmmlDf.count() shouldBe df.count()

		val columns = df.schema.fieldNames
		val pmmlColumns = pmmlDf.schema.fieldNames

		pmmlColumns.size shouldBe (columns.size + 1 + 3 + 1)

		columns.foreach {
			colName => pmmlColumns should contain(colName)
		}

		pmmlTransformer.getTargetFields.foreach {
			targetField => {
				pmmlColumns should contain(targetField.getName)
				pmmlDf.schema(targetField.getName).dataType shouldBe StringType
			}
		}

		pmmlTransformer.getOutputFields.foreach {
			outputField => {
				pmmlColumns should contain(outputField.getName)
				pmmlDf.schema(outputField.getName).dataType shouldBe DoubleType
			}
		}

		pmmlColumns should contain(pmmlTransformer.getExceptionCol)
		pmmlDf.schema(pmmlTransformer.getExceptionCol).dataType shouldBe StringType
	}
}