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

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class SchemaUtil {

	private SchemaUtil(){
	}

	static
	public DataType translateDataType(org.dmg.pmml.DataType dataType){

		switch(dataType){
			case STRING:
				return DataTypes.StringType;
			case INTEGER:
				return DataTypes.IntegerType;
			case FLOAT:
				return DataTypes.FloatType;
			case DOUBLE:
				return DataTypes.DoubleType;
			case BOOLEAN:
				return DataTypes.BooleanType;
			default:
				throw new IllegalArgumentException();
		}
	}
}