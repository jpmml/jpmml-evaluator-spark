package org.jpmml.evaluator.spark

import java.{util => ju}

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Model, PipelineModel, Transformer}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

case class PmmlPipelineModel  (override val uid: String,
                               stages: Array[Transformer])
  extends Model[PipelineModel] {

  def this(stages: Array[Transformer]) =
    this(Identifiable.randomUID("columnPruner"), stages)

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    stages.foldLeft(dataset.toDF)((cur, transformer) => transformer.transform(cur))
  }

  override def transformSchema(schema: StructType): StructType = {
    stages.foldLeft(schema)((cur, transformer) => transformer.transformSchema(cur))
  }

  override def copy(extra: ParamMap): PipelineModel = {
    PmmlPipelineModel(uid, stages.map(_.copy(extra))).setParent(parent)
  }
}

object PmmlColumnPruner {
  def apply(columnsToPrune: Set[String]): PmmlColumnPruner  =
    PmmlColumnPruner(Identifiable.randomUID("columnPruner"), columnsToPrune)
}

case class PmmlColumnPruner(override val uid: String, columnsToPrune: Set[String])
  extends Transformer {

  def this(columnsToPrune: Set[String]) =
    this(Identifiable.randomUID("columnPruner"), columnsToPrune)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val columnsToKeep = dataset.columns.filter(!columnsToPrune.contains(_))
    dataset.select(columnsToKeep.map(dataset.col): _*)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields.filter(col => !columnsToPrune.contains(col.name)))
  }

  override def copy(extra: ParamMap): PmmlColumnPruner = defaultCopy(extra)
}