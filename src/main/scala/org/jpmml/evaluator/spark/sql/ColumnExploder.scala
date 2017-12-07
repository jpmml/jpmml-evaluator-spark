package org.jpmml.evaluator.spark.sql

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * 将结构体中的列拆出来
  *
  * @param structCol 结构列名
  */
case class ColumnExploder(structCol: String) extends Transformer {
  override def transform(dataset: Dataset[_]): DataFrame = {
    val schema = dataset.schema
    //获取结构
    val structSchema = getStructSchema(schema)
    //获取列对象
    val structColumn = dataset.apply(DatasetUtil.escapeColumnName(structCol))

    var result = dataset.toDF

    val fields = structSchema.fields
    for (field <- fields) {
      val name = field.name
      val fieldColumn = structColumn.getField(DatasetUtil.escapeColumnName(name))
      result = result.withColumn(DatasetUtil.escapeColumnName(name), fieldColumn)
    }
    result.drop(structCol)
  }

  override def copy(extra: ParamMap): Transformer = throw new UnsupportedOperationException

  override def transformSchema(schema: StructType): StructType = {
    val structSchema = getStructSchema(schema)
    var result = schema
    val fields = structSchema.fields
    for (field <- fields) {
      result = result.add(field)
    }
    result
  }

  override val uid: String = null

  private def getStructSchema(schema: StructType): StructType = {
    val structField = schema.apply(structCol)
    structField.dataType.asInstanceOf[StructType]
  }
}
