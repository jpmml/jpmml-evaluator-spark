/*
 * Copyright (c) 2016 Villu Ruusmann
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
package org.jpmml.evaluator.spark;

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
	public Dataset<Row> transform(Dataset<?> dataset){
		StructType schema = dataset.schema();

		StructType structSchema = getStructSchema(schema);

		Column structColumn = dataset.apply(DatasetUtil.escapeColumnName(getStructCol()));

		Dataset<Row> result = dataset.toDF();

		StructField[] fields = structSchema.fields();
		for(StructField field : fields){
			String name = field.name();

			Column fieldColumn = structColumn.getField(DatasetUtil.escapeColumnName(name));

			result = result.withColumn(DatasetUtil.escapeColumnName(name), fieldColumn);
		}

		return result;
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
