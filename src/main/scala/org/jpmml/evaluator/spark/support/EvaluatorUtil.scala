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
package org.jpmml.evaluator.spark.support

import java.io.{File, FileInputStream, IOException, InputStream}
import javax.xml.bind.JAXBException

import org.jpmml.evaluator.{Evaluator, ModelEvaluatorFactory}
import org.jpmml.model.{ImportFilter, JAXBUtil}
import org.jpmml.model.visitors.LocatorTransformer
import org.xml.sax.{InputSource, SAXException}

object EvaluatorUtil {
  @throws[IOException]
  @throws[SAXException]
  @throws[JAXBException]
  def createEvaluator(file: File): Evaluator = {
    val is = new FileInputStream(file)

    try
      createEvaluator(is)
    finally
      if (is != null) is.close()
  }

  @throws[SAXException]
  @throws[JAXBException]
  def createEvaluator(is: InputStream): Evaluator = {
    val source = ImportFilter.apply(new InputSource(is))

    val pmml = JAXBUtil.unmarshalPMML(source)

    (new LocatorTransformer).applyTo(pmml)

    val evaluator = ModelEvaluatorFactory.newInstance.newModelEvaluator(pmml)

    // Perform self-testing
    evaluator.verify()

    evaluator
  }
}