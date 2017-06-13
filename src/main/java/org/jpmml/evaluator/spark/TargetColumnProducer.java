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
import org.jpmml.evaluator.TargetField;

class TargetColumnProducer extends ColumnProducer<TargetField> {

	TargetColumnProducer(TargetField field, String columnName){
		super(field, columnName != null ? columnName : getName(field));
	}

	@Override
	public StructField init(Evaluator evaluator){
		TargetField field = getField();

		DataType dataType = field.getDataType();

		return DataTypes.createStructField(getColumnName(), SchemaUtil.translateDataType(dataType), false);
	}

	@Override
	public Object format(Object value){
		return org.jpmml.evaluator.EvaluatorUtil.decode(value);
	}

	static
	private String getName(TargetField field){

		if(field.isSynthetic()){
			return "_target";
		}

		return (field.getName()).getValue();
	}
}