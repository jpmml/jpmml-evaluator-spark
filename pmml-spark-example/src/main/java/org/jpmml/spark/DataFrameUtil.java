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

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SQLContext;

public class DataFrameUtil {

	private DataFrameUtil(){
	}

	static
	public DataFrame loadCsv(SQLContext sqlContext, String path){
		DataFrameReader reader = sqlContext.read()
			.format("com.databricks.spark.csv")
			.option("header", "true")
			.option("inferSchema", "true");

		return reader.load(path);
	}

	static
	public void storeCsv(SQLContext sqlContext, DataFrame dataFrame, String path){
		DataFrameWriter writer = dataFrame.write()
			.format("com.databricks.spark.csv")
		    .option("header", "true");

		writer.save(path);
	}
}