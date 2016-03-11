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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.spark.ml.PredictionModel;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.CreateStruct;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.ScalaUDF;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.dmg.pmml.FieldName;
import org.jpmml.evaluator.Evaluator;
import scala.Function1;
import scala.runtime.AbstractFunction1;

public class PMMLPredictionModel extends PredictionModel<Row, PMMLPredictionModel> {

	private Evaluator evaluator = null;


	public PMMLPredictionModel(Evaluator evaluator){
		setEvaluator(evaluator);
	}

	@Override
	public String uid(){
		return null;
	}

	@Override
	public PMMLPredictionModel copy(ParamMap extra){
		throw new UnsupportedOperationException();
	}

	@Override
	public DataType featuresDataType(){
		return new StructType();
	}

	@Override
	public StructType transformSchema(StructType schema){
		Evaluator evaluator = getEvaluator();

		StructType outputSchema = EvaluatorUtil.createOutputSchema(evaluator);

		StructField targetField = getTarget(evaluator, outputSchema);

		return schema.add(getPredictionCol(), targetField.dataType(), targetField.nullable());
	}

	/**
	 * @throws UnsupportedOperationException Always.
	 */
	@Override
	public double predict(Row row){
		throw new UnsupportedOperationException();
	}

	@Override
	public DataFrame transformImpl(final DataFrame dataFrame){
		final
		Evaluator evaluator = getEvaluator();

		final
		StructType inputSchema = EvaluatorUtil.createInputSchema(evaluator);

		final
		StructType outputSchema = EvaluatorUtil.createOutputSchema(evaluator);

		List<Expression> inputExpressions = Lists.transform(Arrays.asList(inputSchema.fields()), new Function<StructField, Expression>(){

			@Override
			public Expression apply(StructField field){
				Column column = dataFrame.col(field.name());

				return column.expr();
			}
		});

		CreateStruct createStruct = new CreateStruct(ScalaUtil.toSeq(new ArrayList<>(inputExpressions)));

		final
		StructField targetField = getTarget(evaluator, outputSchema);

		Function1<Row, Object> evaluatorFunction = new SerializableAbstractFunction1<Row, Object>(){

			private int targetIndex = outputSchema.fieldIndex(targetField.name());


			@Override
			public Object apply(Row inputRow){
				Row outputRow = EvaluatorUtil.evaluate(evaluator, inputRow, inputSchema);

				return outputRow.get(this.targetIndex);
			}
		};

		Expression evaluateExpression = new ScalaUDF(evaluatorFunction, targetField.dataType(), ScalaUtil.<Expression>singletonSeq(createStruct), ScalaUtil.<DataType>emptySeq());

		Column targetColumn = new Column(evaluateExpression);

		return dataFrame.withColumn(getPredictionCol(), targetColumn);
	}

	static
	private StructField getTarget(Evaluator evaluator, StructType schema){
		List<FieldName> targetFields = org.jpmml.evaluator.EvaluatorUtil.getTargetFields(evaluator);
		if(targetFields.size() != 1){
			throw new IllegalArgumentException();
		}

		FieldName targetField = targetFields.get(0);

		return schema.apply(EvaluatorUtil.formatTargetName(targetField));
	}

	public Evaluator getEvaluator(){
		return this.evaluator;
	}

	private void setEvaluator(Evaluator evaluator){
		this.evaluator = evaluator;
	}

	static
	abstract
	public class SerializableAbstractFunction1<T1, R> extends AbstractFunction1<T1, R> implements Serializable {
	}
}