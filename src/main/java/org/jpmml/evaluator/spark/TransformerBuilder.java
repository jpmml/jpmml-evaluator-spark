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

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.feature.ColumnPruner;
import org.dmg.pmml.ResultFeature;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.OutputField;
import org.jpmml.evaluator.TargetField;

public class TransformerBuilder {

	private Evaluator evaluator = null;

	private List<ColumnProducer<?>> columnProducers = new ArrayList<>();

	private boolean exploded = false;


	public TransformerBuilder(Evaluator evaluator){
		setEvaluator(evaluator);
	}

	public TransformerBuilder withTargetCols(){
		Evaluator evaluator = getEvaluator();

		List<TargetField> targetFields = evaluator.getTargetFields();
		for(TargetField targetField : targetFields){
			this.columnProducers.add(new TargetColumnProducer(targetField, null));
		}

		return this;
	}

	public TransformerBuilder withLabelCol(String columnName){
		Evaluator evaluator = getEvaluator();

		List<TargetField> targetFields = evaluator.getTargetFields();
		if(targetFields.size() != 1){
			throw new IllegalArgumentException();
		}

		TargetField targetField = targetFields.get(0);

		this.columnProducers.add(new TargetColumnProducer(targetField, columnName));

		return this;
	}

	public TransformerBuilder withProbabilityCol(String columnName){
		return withProbabilityCol(columnName, null);
	}

	public TransformerBuilder withProbabilityCol(String columnName, List<String> labels){
		Evaluator evaluator = getEvaluator();

		List<TargetField> targetFields = evaluator.getTargetFields();
		if(targetFields.size() != 1){
			throw new IllegalArgumentException();
		}

		TargetField targetField = targetFields.get(0);

		List<String> values = new ArrayList<>();

		List<OutputField> outputFields = evaluator.getOutputFields();
		for(OutputField outputField : outputFields){
			org.dmg.pmml.OutputField pmmlOutputField = outputField.getOutputField();

			ResultFeature resultFeature = pmmlOutputField.getResultFeature();
			switch(resultFeature){
				case PROBABILITY:
					String value = pmmlOutputField.getValue();

					if(value != null){
						values.add(value);
					}
					break;
				default:
					break;
			}
		}

		if(values.isEmpty()){
			throw new IllegalArgumentException();
		} // End if

		if(labels != null && (labels.size() != values.size() || !labels.containsAll(values))){
			throw new IllegalArgumentException();
		}

		this.columnProducers.add(new ProbabilityColumnProducer(targetField, columnName, labels != null ? labels : values));

		return this;
	}

	public TransformerBuilder withOutputCols(){
		Evaluator evaluator = getEvaluator();

		List<OutputField> outputFields = evaluator.getOutputFields();
		for(OutputField outputField : outputFields){
			this.columnProducers.add(new OutputColumnProducer(outputField, null));
		}

		return this;
	}

	public TransformerBuilder exploded(boolean exploded){
		this.exploded = exploded;

		return this;
	}

	public Transformer build(){
		Evaluator evaluator = getEvaluator();

		PMMLTransformer pmmlTransformer = new PMMLTransformer(evaluator, this.columnProducers);

		if(this.exploded){
			ColumnExploder columnExploder = new ColumnExploder(pmmlTransformer.getOutputCol());

			ColumnPruner columnPruner = new ColumnPruner(ScalaUtil.singletonSet(pmmlTransformer.getOutputCol()));

			PipelineModel pipelineModel = new PipelineModel(null, new Transformer[]{pmmlTransformer, columnExploder, columnPruner});

			return pipelineModel;
		}

		return pmmlTransformer;
	}

	private Evaluator getEvaluator(){
		return this.evaluator;
	}

	private void setEvaluator(Evaluator evaluator){
		this.evaluator = evaluator;
	}
}