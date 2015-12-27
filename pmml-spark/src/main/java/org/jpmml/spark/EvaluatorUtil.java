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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;
import javax.xml.transform.Source;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.dmg.pmml.DataField;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.OutputField;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.ModelEvaluator;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.evaluator.OutputUtil;
import org.jpmml.evaluator.TypeAnalysisException;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.jpmml.model.visitors.LocatorTransformer;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class EvaluatorUtil {

	private EvaluatorUtil(){
	}

	static
	public Evaluator createEvaluator(File file) throws IOException, SAXException, JAXBException {

		try(InputStream is = new FileInputStream(file)){
			return createEvaluator(is);
		}
	}

	static
	public Evaluator createEvaluator(InputStream is) throws SAXException, JAXBException {
		Source source = ImportFilter.apply(new InputSource(is));

		PMML pmml = JAXBUtil.unmarshalPMML(source);

		// If the SAX Locator information is available, then transform it to java.io.Serializable representation
		LocatorTransformer locatorTransformer = new LocatorTransformer();
		locatorTransformer.applyTo(pmml);

		ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();

		ModelEvaluator<?> modelEvaluator = modelEvaluatorFactory.newModelManager(pmml);
		modelEvaluator.verify();

		return modelEvaluator;
	}

	/**
	 * <p>
	 * Applies the scoring function to an input data record, producing an output data record.
	 * </p>
	 *
	 * @param row An input data record.
	 * The ordering of values is not significant, because they are looked up by name. Extraneous values, if any, are ignored.
	 *
	 * @return An output data record.
	 *
	 * @see #createInputSchema(Evaluator)
	 * @see #createOutputSchema(Evaluator)
	 */
	static
	public Row evaluate(Evaluator evaluator, Row row){
		return evaluate(evaluator, row, row.schema());
	}

	static
	public Row evaluate(Evaluator evaluator, Row row, StructType schema){
		Map<FieldName, FieldValue> arguments = new LinkedHashMap<>();

		List<FieldName> activeFields = evaluator.getActiveFields();
		for(FieldName activeField : activeFields){
			int index = schema.fieldIndex(activeField.getValue());

			FieldValue activeValue = evaluator.prepare(activeField, row.get(index));

			arguments.put(activeField, activeValue);
		}

		Map<FieldName, ?> result = evaluator.evaluate(arguments);

		List<Object> values = new ArrayList<>();

		List<FieldName> targetFields = org.jpmml.evaluator.EvaluatorUtil.getTargetFields(evaluator);
		for(FieldName targetField : targetFields){
			Object targetValue = result.get(targetField);

			values.add(org.jpmml.evaluator.EvaluatorUtil.decode(targetValue));
		}

		List<FieldName> outputFields = evaluator.getOutputFields();
		for(FieldName outputField : outputFields){
			Object outputValue = result.get(outputField);

			values.add(outputValue);
		}

		return RowFactory.create(values.toArray());
	}

	/**
	 * <p>
	 * Returns the description of input fields.
	 * </p>
	 *
	 * @see Evaluator#getActiveFields()
	 */
	static
	public StructType createInputSchema(Evaluator evaluator){
		StructType schema = new StructType();

		List<FieldName> activeFields = evaluator.getActiveFields();
		for(FieldName activeField : activeFields){
			DataField dataField = evaluator.getDataField(activeField);

			schema = schema.add(activeField.getValue(), translateDataType(dataField.getDataType()), true);
		}

		return schema;
	}

	/**
	 * <p>
	 * Returns the description of primary and secondary output fields.
	 * </p>
	 *
	 * @see Evaluator#getTargetFields()
	 * @see Evaluator#getOutputFields()
	 */
	static
	public StructType createOutputSchema(Evaluator evaluator){
		StructType schema = new StructType();

		List<FieldName> targetFields = org.jpmml.evaluator.EvaluatorUtil.getTargetFields(evaluator);
		for(FieldName targetField : targetFields){
			DataField dataField = evaluator.getDataField(targetField);

			schema = schema.add(formatTargetName(targetField), translateDataType(dataField.getDataType()), false);
		}

		List<FieldName> outputFields = evaluator.getOutputFields();
		for(FieldName outputField : outputFields){
			OutputField output = evaluator.getOutputField(outputField);

			org.dmg.pmml.DataType dataType;

			try {
				dataType = OutputUtil.getDataType(output, (ModelEvaluator<?>)evaluator);
			} catch(TypeAnalysisException tae){
				dataType = org.dmg.pmml.DataType.STRING;
			}

			schema = schema.add(outputField.getValue(), translateDataType(dataType), false);
		}

		return schema;
	}

	static
	public String formatTargetName(FieldName name){
		return (name != null ? name.getValue() : "_default");
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