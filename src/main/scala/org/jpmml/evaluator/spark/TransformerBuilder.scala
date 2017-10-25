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
package org.jpmml.evaluator.spark

import org.apache.spark.ml.Transformer
import org.dmg.pmml.{OutputField, ResultFeature}
import org.jpmml.evaluator.TargetField
import org.jpmml.evaluator.spark.producers.{OutputColumnProducer, ProbabilityColumnProducer, TargetColumnProducer}
import org.jpmml.evaluator.spark.transformers.PmmlColumnExploder
import org.jpmml.{evaluator => jpmmlEvaluator}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Build a spark transformer from an evaluator
  */
class TransformerBuilder(val evaluator: jpmmlEvaluator.Evaluator) {
  val targetFields: Seq[jpmmlEvaluator.TargetField] = evaluator.getTargetFields
  val outputFields: Seq[jpmmlEvaluator.OutputField] = evaluator.getOutputFields

  private val columnProducers: mutable.Buffer[ColumnProducer[_ <: jpmmlEvaluator.ResultField]] = mutable.Buffer()
  private var exploded: Boolean = false

  /**
    * Compute column producers from model target fields
    * @todo Use copy / case class
    */
  def withTargetCols: TransformerBuilder = {
    for (targetField <- targetFields) {
      columnProducers += new TargetColumnProducer(
        field = targetField,
        columnName = null)
    }

    this
  }

  /**
    * Add label col from column name
    * @todo Use copy / case class
    */
  def withLabelCol(columnName: String): TransformerBuilder = {
    if (targetFields.size != 1)
      throw new IllegalArgumentException

    val targetField: jpmmlEvaluator.TargetField = targetFields.get(0)

    columnProducers += new TargetColumnProducer(
      field = targetField,
      columnName = columnName)

    this
  }

  /**
    * Add probability col without label
    * @todo Use copy / case class
    */
  def withProbabilityCol(columnName: String): TransformerBuilder =
    withProbabilityCol(columnName, null)

  /**
    * Add probability col from a column name and a list of labels
    * @todo Use copy / case class
    */
  def withProbabilityCol(columnName: String, labels: Seq[String]): TransformerBuilder = {
    if (targetFields.size != 1)
      throw new IllegalArgumentException

    val targetField: TargetField = targetFields.get(0)
    val pmmlValues: Seq[String] = computePmmlValues

    if (pmmlValues.isEmpty) {
      throw new IllegalArgumentException
    }

    if (labels != null && (labels.size != pmmlValues.size || labels.intersect(pmmlValues).isEmpty))
      throw new IllegalArgumentException

    columnProducers += new ProbabilityColumnProducer(targetField, columnName,
      if (labels != null)
        labels
      else
        pmmlValues)

    this
  }

  private def computePmmlValues = {
    val values: mutable.Buffer[String] = mutable.Buffer()

    for (outputField <- outputFields) {
      val pmmlOutputField: OutputField = outputField.getOutputField
      val resultFeature: ResultFeature = pmmlOutputField.getResultFeature

      resultFeature match {
        case ResultFeature.PROBABILITY =>
          val pmmlValue: String = pmmlOutputField.getValue

          if (pmmlValue != null)
            values += pmmlValue

        case _ =>
      }
    }

    values
  }

  /**
    * Add output cols
    * @todo Use copy / case class
    */
  def withOutputCols: TransformerBuilder = {
    val outputFields: Seq[jpmmlEvaluator.OutputField] = evaluator.getOutputFields
    for (outputField <- outputFields) {
      columnProducers += new OutputColumnProducer(outputField, null)
    }

    this
  }

  /**
    * Set exploded
    * @todo Use copy / case class
    */
  def exploded(exploded: Boolean): TransformerBuilder = {
    this.exploded = exploded

    this
  }

  /**
    * Build transformer
    * @todo Use copy / case class
    */
  def build: Transformer = {
    val pmmlTransformer = PmmlTransformer(evaluator, columnProducers.toList)

    if (exploded) {
      PmmlPipelineModel("pmml-pipeline", Array(
        pmmlTransformer,
        PmmlColumnExploder(pmmlTransformer.outputCol),
        PmmlColumnPruner(Set(pmmlTransformer.outputCol))))
    }
    else
      pmmlTransformer
  }
}



