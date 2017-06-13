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

import java.util.List;

import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.HasProbability;
import org.jpmml.evaluator.TargetField;

class ProbabilityColumnProducer extends ColumnProducer<TargetField> {

	private List<String> labels = null;


	ProbabilityColumnProducer(TargetField field, String columnName, List<String> labels){
		super(field, columnName != null ? columnName : "probability");

		setLabels(labels);
	}

	@Override
	public StructField init(Evaluator evaluator){
		return DataTypes.createStructField(getColumnName(), new VectorUDT(), false);
	}

	@Override
	public Vector format(Object value){
		List<String> labels = getLabels();

		HasProbability hasProbability = (HasProbability)value;

		double[] probabilities = new double[labels.size()];

		for(int i = 0; i < labels.size(); i++){
			String label = labels.get(i);

			probabilities[i] = hasProbability.getProbability(label);
		}

		return new DenseVector(probabilities);
	}

	public List<String> getLabels(){
		return this.labels;
	}

	private void setLabels(List<String> labels){
		this.labels = labels;
	}
}