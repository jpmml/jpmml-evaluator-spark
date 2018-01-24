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

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

// Helper object for case class for Spark
object TestHelper {
  case class Record(sepal_length: Double, sepal_width: Double, petal_length: Double, petal_width: Double)
}

class PMMLTransformerTest extends FunSuite {
  import TestHelper._

  test("Transformer works as expected") {
    implicit val sparkSession = SparkSession
      .builder()
      .config(
        new SparkConf()
          .setAppName("iris-test")
          .setMaster("local")
      ).getOrCreate()

    val ds = sparkSession.sparkContext.parallelize(Seq(
      Record(1, 2, 3, 4),
      Record(5, 6, 7, 8)
    ))

    val df = sparkSession.createDataFrame(ds)

    // Load the PMML
    val pmmlFile = new File(getClass.getClassLoader.getResource("clustering.pmml").getFile)

    // Create the evaluator
    val evaluator = EvaluatorUtil.createEvaluator(pmmlFile)

    // Create the transformer
    val pmmlTransformer = new TransformerBuilder(evaluator)
      .withTargetCols
      .withOutputCols
      .exploded(true)
      .build()

    // Verify the transformed results
    val result = pmmlTransformer.transform(df)
    result.show

    import org.apache.spark.sql.functions._
    assert(result.count() == 2)
    assert(result.filter(col("prediction") === 4 && col("sepal_length") === 1).count() == 1)
    assert(result.filter(col("prediction") === 1 && col("sepal_length") === 5).count() == 1)
  }
}
