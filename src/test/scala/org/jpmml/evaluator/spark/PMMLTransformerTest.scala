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

import java.util.Arrays

import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession
import org.jpmml.evaluator.LoadingModelEvaluatorBuilder
import org.scalatest.FunSuite

// Helper object for case class for Spark
object IrisHelper {
	case class InputRecord(Sepal_Length: Double, Sepal_Width: Double, Petal_Length: Double, Petal_Width: Double)
	case class DefaultResultRecord(Species: String, Probability_setosa: Double, Probability_versicolor: Double, Probability_virginica: Double, Node_id: String)
	case class CustomResultRecord(label: String, probabilities: Vector)
}

class PMMLTransformerTest extends FunSuite {
	import IrisHelper._

	test("Transformer works as expected") {
		implicit val sparkSession = SparkSession
			.builder()
			.config(
				new SparkConf()
					.setAppName("DecisionTreeIris")
					.setMaster("local")
			).getOrCreate()

		val inputRdd = sparkSession.sparkContext.makeRDD(Seq(
			InputRecord(5.1, 3.5, 1.4, 0.2),
			InputRecord(7, 3.2, 4.7, 1.4),
			InputRecord(6.3, 3.3, 6, 2.5)
		))
		val inputDs = sparkSession.createDataFrame(inputRdd)

		val expectedDefaultResultRdd = sparkSession.sparkContext.makeRDD(Seq(
			DefaultResultRecord("setosa", 1.0, 0.0, 0.0, "2"),
			DefaultResultRecord("versicolor", 0.0, 0.9074074074074074, 0.09259259259259259, "6"),
			DefaultResultRecord("virginica", 0.0, 0.021739130434782608, 0.9782608695652174, "7")
		))
		val expectedDefaultResultDs = sparkSession.createDataFrame(expectedDefaultResultRdd)

		val expectedCustomResultRdd = sparkSession.sparkContext.makeRDD(Seq(
			CustomResultRecord("setosa", Vectors.dense(1.0, 0.0, 0.0)),
			CustomResultRecord("versicolor", Vectors.dense(0.0, 0.9074074074074074, 0.09259259259259259)),
			CustomResultRecord("virginica", Vectors.dense(0.0, 0.021739130434782608, 0.9782608695652174))
		))
		val expectedCustomResultDs = sparkSession.createDataFrame(expectedCustomResultRdd)

		// Load the PMML
		val pmmlIs = getClass.getClassLoader.getResourceAsStream("DecisionTreeIris.pmml")

		// Create the evaluator
		val evaluator = new LoadingModelEvaluatorBuilder()
			.load(pmmlIs)
			.build()

		// Create the transformer
		var pmmlTransformer = new TransformerBuilder(evaluator)
			.withTargetCols
			.withOutputCols
			.exploded(true)
			.build()

		// Verify the transformed results
		var resultDs = pmmlTransformer.transform(inputDs)
		resultDs.show

		resultDs = resultDs.select("Species", "Probability_setosa", "Probability_versicolor", "Probability_virginica", "Node_Id")

		assert(resultDs.rdd.collect.toList == expectedDefaultResultDs.rdd.collect.toList)

		pmmlTransformer = new TransformerBuilder(evaluator)
			.withLabelCol("label")
			.withProbabilityCol("probability", Arrays.asList("setosa", "versicolor", "virginica"))
			.exploded(true)
			.build()

		resultDs = pmmlTransformer.transform(inputDs)
		resultDs.show

		resultDs = resultDs.select("label", "probability")

		assert(resultDs.rdd.collect.toList == expectedCustomResultDs.rdd.collect.toList)
	}
}
