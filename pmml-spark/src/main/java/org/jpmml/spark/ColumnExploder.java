/*
 * Copyright (c) 2016 Villu Ruusmann
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

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ColumnExploder extends Transformer {

	private String structCol = null;


	public ColumnExploder(String structCol){
		setStructCol(structCol);
	}

	@Override
	public String uid(){
		return null;
	}

	@Override
	public ColumnExploder copy(ParamMap extra){
		throw new UnsupportedOperationException();
	}

	@Override
	public StructType transformSchema(StructType schema){
		StructType structSchema = getStructSchema(schema);

		StructType result = schema;

		StructField[] fields = structSchema.fields();
		for(StructField field : fields){
			result = result.add(field);
		}

		return result;
	}

	@Override
	public Dataset<Row> transform(Dataset<?> dataFrame){
		StructType schema = dataFrame.schema();

		StructType structSchema = getStructSchema(schema);

		Column structColumn = dataFrame.apply(getStructCol());

		Dataset<?> result = dataFrame;

		StructField[] fields = structSchema.fields();
		for(StructField field : fields){
			String name = field.name();

			Column fieldColumn = structColumn.getField(name);

			result = result.withColumn(name, fieldColumn);
		}

		return result.toDF();
	}

	private StructType getStructSchema(StructType schema){
		StructField structField = schema.apply(getStructCol());

		return (StructType)structField.dataType();
	}

	public String getStructCol(){
		return this.structCol;
	}

	private void setStructCol(String structCol){
		this.structCol = structCol;
	}
}
