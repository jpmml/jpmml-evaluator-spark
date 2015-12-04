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

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.jpmml.evaluator.Evaluator;

public class PMMLFunction implements Function<Row, Row> {

	private Evaluator evaluator = null;

	private StructType inputSchema = null;

	private StructType outputSchema = null;


	public PMMLFunction(Evaluator evaluator){
		setEvaluator(evaluator);
	}

	/**
	 * <p>
	 * Applies this scoring function to an input data record, producing an output data record.
	 * </p>
	 *
	 * @param row An input data record.
	 * The layout of the input data record is specified by the {@link #getInputSchema() input schema} of this scoring function.
	 * In practice, the ordering of values is not significant, because they are looked up by name. Extraneous values, if any, are ignored.
	 *
	 * @return An output data record.
	 * The layout of the output data record is specified by the {@link #getOutputSchema() output schema} of this scoring function.
	 *
	 * @see #getInputSchema()
	 * @see #getOutputSchema()
	 */
	@Override
	public Row call(Row row){
		Evaluator evaluator = getEvaluator();

		return EvaluatorUtil.evaluate(evaluator, row, row.schema());
	}

	/**
	 * <p>
	 * Returns the description of input fields.
	 * </p>
	 *
	 * @see Evaluator#getActiveFields()
	 */
	public StructType getInputSchema(){
		Evaluator evaluator = getEvaluator();

		if(this.inputSchema == null){
			this.inputSchema = EvaluatorUtil.createInputSchema(evaluator);
		}

		return this.inputSchema;
	}

	/**
	 * <p>
	 * Returns the description of primary and secondary output fields.
	 * </p>
	 *
	 * @see Evaluator#getTargetFields()
	 * @see Evaluator#getOutputFields()
	 */
	public StructType getOutputSchema(){
		Evaluator evaluator = getEvaluator();

		if(this.outputSchema == null){
			this.outputSchema = EvaluatorUtil.createOutputSchema(evaluator);
		}

		return this.outputSchema;
	}

	public Evaluator getEvaluator(){
		return this.evaluator;
	}

	private void setEvaluator(Evaluator evaluator){
		this.evaluator = evaluator;
	}
}