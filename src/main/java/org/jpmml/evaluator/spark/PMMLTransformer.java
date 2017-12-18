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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.CreateStruct;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.ScalaUDF;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.dmg.pmml.FieldName;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.InputField;
import org.jpmml.evaluator.ResultField;
import scala.Function1;
import scala.runtime.AbstractFunction1;

public class PMMLTransformer extends Transformer {

	private String outputCol = "pmml";

	private Evaluator evaluator = null;

	private List<ColumnProducer<?>> columnProducers = null;

	private StructType outputSchema = null;


	public PMMLTransformer(Evaluator evaluator, List<ColumnProducer<?>> columnProducers){
		StructType outputSchema = new StructType();

		for(ColumnProducer<?> columnProducer : columnProducers){
			StructField structField = columnProducer.init(evaluator);

			outputSchema = outputSchema.add(structField);
		}

		setEvaluator(evaluator);
		setColumnProducers(columnProducers);
		setOutputSchema(outputSchema);
	}

	@Override
	public String uid(){
		return null;
	}

	@Override
	public PMMLTransformer copy(ParamMap extra){
		throw new UnsupportedOperationException();
	}

	@Override
	public StructType transformSchema(StructType schema){
		StructField outputField = DataTypes.createStructField(getOutputCol(), getOutputSchema(), false);

		return schema.add(outputField);
	}

	@Override
	public Dataset<Row> transform(final Dataset<?> dataset){
		final
		Evaluator evaluator = getEvaluator();

		final
		List<ColumnProducer<?>> columnProducers = getColumnProducers();

		final
		List<InputField> inputFields = evaluator.getInputFields();

		Function<InputField, Expression> function = new Function<InputField, Expression>(){

			@Override
			public Expression apply(InputField inputField){
				Column column = dataset.apply(DatasetUtil.escapeColumnName((inputField.getName()).getValue()));

				return column.expr();
			}
		};

		List<Expression> activeExpressions = Lists.newArrayList(Lists.transform(inputFields, function));

		Function1<Row, Row> evaluatorFunction = new SerializableAbstractFunction1<Row, Row>(){

			@Override
			public Row apply(Row row){
				Map<FieldName, FieldValue> arguments = new LinkedHashMap<>();

				for(int i = 0; i < inputFields.size(); i++){
					InputField inputField = inputFields.get(i);
					FieldName name = inputField.getName();

					Object value = row.get(i);
					FieldValue preparedValue = inputField.prepare(value);

					arguments.put(name, preparedValue);
				}

				Map<FieldName, ?> result = evaluator.evaluate(arguments);

				List<Object> formattedValues = new ArrayList<>(columnProducers.size());

				for(int i = 0; i < columnProducers.size(); i++){
					ColumnProducer<?> columnProducer = columnProducers.get(i);

					ResultField resultField = columnProducer.getField();
					FieldName name = resultField.getName();

					Object value = result.get(name);
					Object formattedValue = columnProducer.format(value);

					formattedValues.add(formattedValue);
				}

				return RowFactory.create(formattedValues.toArray());
			}
		};

		Expression evaluateExpression = new ScalaUDF(evaluatorFunction, getOutputSchema(), ScalaUtil.<Expression>singletonSeq(new CreateStruct(ScalaUtil.<Expression>toSeq(activeExpressions))), ScalaUtil.<DataType>emptySeq());

		Column outputColumn = new Column(evaluateExpression);

		return dataset.withColumn(DatasetUtil.escapeColumnName(getOutputCol()), outputColumn);
	}

	public String[] getInputCols(){
		Evaluator evaluator = getEvaluator();

		List<InputField> inputFields = evaluator.getActiveFields();

		Function<InputField, String> function = new Function<InputField, String>(){

			@Override
			public String apply(InputField inputField){
				return (inputField.getName()).getValue();
			}
		};

		List<String> values = Lists.newArrayList(Lists.transform(inputFields, function));

		return values.toArray(new String[values.size()]);
	}

	public String getOutputCol(){
		return this.outputCol;
	}

	public void setOutputCol(String outputCol){

		if(outputCol == null){
			throw new IllegalArgumentException();
		}

		this.outputCol = outputCol;
	}

	public Evaluator getEvaluator(){
		return this.evaluator;
	}

	private void setEvaluator(Evaluator evaluator){
		this.evaluator = evaluator;
	}

	public List<ColumnProducer<?>> getColumnProducers(){
		return this.columnProducers;
	}

	private void setColumnProducers(List<ColumnProducer<?>> columnProducers){
		this.columnProducers = columnProducers;
	}

	public StructType getOutputSchema(){
		return this.outputSchema;
	}

	private void setOutputSchema(StructType outputSchema){
		this.outputSchema = outputSchema;
	}

	static
	abstract
	public class SerializableAbstractFunction1<T1, R> extends AbstractFunction1<T1, R> implements Serializable {
	}
}