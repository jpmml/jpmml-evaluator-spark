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
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.feature.ColumnPruner;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.ResultFeature;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.MissingAttributeException;
import org.jpmml.evaluator.OutputField;
import org.jpmml.evaluator.PMMLAttributes;
import org.jpmml.evaluator.ResultField;
import org.jpmml.evaluator.TargetField;
import scala.collection.immutable.Set;

public class TransformerBuilder {

	private Evaluator evaluator = null;

	private List<ColumnProducer<? extends ResultField>> columnProducers = new ArrayList<>();

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

		TargetField targetField = getTargetField(evaluator);

		this.columnProducers.add(new TargetColumnProducer(targetField, columnName));

		return this;
	}

	public TransformerBuilder withProbabilityCol(String columnName){
		return withProbabilityCol(columnName, null);
	}

	public TransformerBuilder withProbabilityCol(String columnName, List<String> labels){
		Evaluator evaluator = getEvaluator();

		TargetField targetField = getTargetField(evaluator);

		List<OutputField> probabilityOutputFields = getProbabilityFields(evaluator, targetField);

		List<String> targetCategories = probabilityOutputFields.stream()
			.map(probabilityOutputField -> {
				org.dmg.pmml.OutputField pmmlOutputField = probabilityOutputField.getField();

				String value = pmmlOutputField.getValue();
				if(value == null){
					throw new MissingAttributeException(pmmlOutputField, PMMLAttributes.OUTPUTFIELD_VALUE);
				}

				return value;
			})
			.collect(Collectors.toList());

		if((labels != null) && (labels.size() != targetCategories.size() || !labels.containsAll(targetCategories))){
			throw new IllegalArgumentException("Model has an incompatible set of probability-type output fields (expected " + labels + ", got " + targetCategories + ")");
		}

		this.columnProducers.add(new ProbabilityColumnProducer(targetField, columnName, labels != null ? labels : targetCategories));

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

			ColumnPruner columnPruner = new ColumnPruner(new Set.Set1<>(pmmlTransformer.getOutputCol()));

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

	static
	private TargetField getTargetField(Evaluator evaluator){
		List<TargetField> targetFields = evaluator.getTargetFields();

		if(targetFields.size() < 1){
			throw new IllegalArgumentException("Model does not have a target field");
		} else

		if(targetFields.size() > 1){
			throw new IllegalArgumentException("Model has multiple target fields (" + targetFields + ")");
		}

		return targetFields.get(0);
	}

	static
	private List<OutputField> getProbabilityFields(Evaluator evaluator, TargetField targetField){
		List<OutputField> outputFields = evaluator.getOutputFields();

		Predicate<OutputField> predicate = new Predicate<OutputField>(){

			@Override
			public boolean test(OutputField outputField){
				org.dmg.pmml.OutputField pmmlOutputField = outputField.getField();

				ResultFeature resultFeature = pmmlOutputField.getResultFeature();
				switch(resultFeature){
					case PROBABILITY:
						FieldName targetFieldName = pmmlOutputField.getTargetField();

						return Objects.equals(targetFieldName, null) || Objects.equals(targetFieldName, targetField.getName());
					default:
						return false;
				}
			}
		};

		List<OutputField> probabilityOutputFields = outputFields.stream()
			.filter(predicate)
			.collect(Collectors.toList());

		if(probabilityOutputFields.size() < 1){
			throw new IllegalArgumentException("Model does not have probability-type output fields");
		}

		return probabilityOutputFields;
	}
}