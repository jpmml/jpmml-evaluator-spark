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

import javax.xml.bind.JAXBException;
import javax.xml.transform.Source;

import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.ModelEvaluatorFactory;
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

		Evaluator evaluator = modelEvaluatorFactory.newModelManager(pmml);

		// Perform self-testing
		evaluator.verify();

		return evaluator;
	}
}