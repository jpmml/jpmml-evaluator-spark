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

import java.io.Serializable;
import java.util.Objects;

import org.apache.spark.sql.types.StructField;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.ResultField;

abstract
class ColumnProducer<F extends ResultField> implements Serializable {

	private F field = null;

	private String columnName = null;


	ColumnProducer(F field, String columnName){
		setField(Objects.requireNonNull(field));
		setColumnName(Objects.requireNonNull(columnName));
	}

	abstract
	public StructField init(Evaluator evaluator);

	abstract
	public Object format(Object value);

	public F getField(){
		return this.field;
	}

	private void setField(F field){
		this.field = field;
	}

	public String getColumnName(){
		return this.columnName;
	}

	private void setColumnName(String columnName){
		this.columnName = columnName;
	}
}