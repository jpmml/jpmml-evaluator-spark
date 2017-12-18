package org.jpmml.evaluator.spark.sql

import java.io.InputStream
import javax.xml.bind.JAXBException

import org.jpmml.evaluator.{Evaluator, ModelEvaluatorFactory}
import org.jpmml.model.visitors.LocatorTransformer
import org.jpmml.model.{ImportFilter, JAXBUtil}
import org.xml.sax.{InputSource, SAXException}

object EvaluatorUtil {

  @throws[SAXException]
  @throws[JAXBException]
  def createEvaluator(is: InputStream): Evaluator = {
    val source = ImportFilter.apply(new InputSource(is))
    val pmml = JAXBUtil.unmarshalPMML(source)
    // If the SAX Locator information is available, then transform it to java.io.Serializable representation
    val locatorTransformer = new LocatorTransformer
    locatorTransformer.applyTo(pmml)
    val modelEvaluatorFactory = ModelEvaluatorFactory.newInstance
    val evaluator = modelEvaluatorFactory.newModelEvaluator(pmml)
    // Perform self-testing
    evaluator.verify()
    evaluator
  }
}
