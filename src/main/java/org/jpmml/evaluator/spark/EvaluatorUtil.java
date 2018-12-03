/*
 * Copyright (c) 2015 Villu Ruusmann
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.bind.JAXBException;

import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.EvaluatorBuilder;
import org.jpmml.evaluator.LoadingModelEvaluatorBuilder;
import org.xml.sax.SAXException;

/**
 * @see LoadingModelEvaluatorBuilder
 */
public class EvaluatorUtil {

	private EvaluatorUtil(){
	}

	/**
	 * @see LoadingModelEvaluatorBuilder#load(File)
	 * @see LoadingModelEvaluatorBuilder#load(File, String)
	 */
	@Deprecated
	static
	public Evaluator createEvaluator(File file) throws IOException, SAXException, JAXBException {

		try(InputStream is = new FileInputStream(file)){
			return createEvaluator(is);
		}
	}

	/**
	 * @see LoadingModelEvaluatorBuilder#load(InputStream)
	 * @see LoadingModelEvaluatorBuilder#load(InputStream, String)
	 */
	@Deprecated
	static
	public Evaluator createEvaluator(InputStream is) throws SAXException, JAXBException {
		EvaluatorBuilder evaluatorBuilder = new LoadingModelEvaluatorBuilder()
			.load(is);

		Evaluator evaluator = evaluatorBuilder.build();

		// Perform self-testing
		evaluator.verify();

		return evaluator;
	}
}