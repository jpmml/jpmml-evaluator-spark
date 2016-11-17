/*
 * Copyright (c) 2015 Villu Ruusmann
 *
 * This file is part of JPMML-Spark
 *
 * JPMML-Spark is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * JPMML-Spark is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with JPMML-Spark.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.jpmml.spark;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Transformer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Row;
import org.jpmml.evaluator.Evaluator;

public class EvaluationExample {

	static
	public void main(String... args) throws Exception {

		if(args.length != 3){
			System.err.println("Usage: java " + EvaluationExample.class.getName() + " <PMML file> <Input CSV file> <Output directory>");

			System.exit(-1);
		}

		Evaluator evaluator = EvaluatorUtil.createEvaluator(new File(args[0]));

		TransformerBuilder modelBuilder = new TransformerBuilder(evaluator)
			.withTargetCols()
			.withOutputCols()
			.exploded(true);

		Transformer transformer = modelBuilder.build();

		SparkConf conf = new SparkConf();

		try(JavaSparkContext sparkContext = new JavaSparkContext(conf)){
			SQLContext sqlContext = new SQLContext(sparkContext);

			DataFrameReader reader = sqlContext.read()
				.format("com.databricks.spark.csv")
				.option("header", "true")
				.option("inferSchema", "true");

			Dataset<Row> dataFrame = reader.load(args[1]);

			dataFrame = transformer.transform(dataFrame);

			DataFrameWriter writer = dataFrame.write()
				.format("com.databricks.spark.csv")
			    .option("header", "true");

			writer.save(args[2]);
		}
	}
}
