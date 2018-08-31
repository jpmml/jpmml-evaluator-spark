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

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.dmg.pmml.DataType;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.OutputField;

class OutputColumnProducer extends ColumnProducer<OutputField> {

	private boolean formatString = false;


	OutputColumnProducer(OutputField field, String columnName){
		super(field, columnName != null ? columnName : getName(field));
	}

	@Override
	public StructField init(Evaluator evaluator){
		OutputField field = getField();

		DataType dataType = field.getDataType();
		if(dataType == null){
			dataType = DataType.STRING;

			this.formatString = true;
		}

		return DataTypes.createStructField(getColumnName(), SchemaUtil.translateDataType(dataType), false);
	}

	@Override
	public Object format(Object value){

		if(this.formatString){
			return value.toString();
		}

		return value;
	}

	static
	private String getName(OutputField field){
		return (field.getName()).getValue();
	}
}