package org.jpmml.evaluator.spark.support

import org.apache.spark.sql.types.{DataTypes, DataType => SparkDataType}
import org.dmg.pmml.DataType

object SchemaUtil {
  def translateDataType(dataType: DataType): SparkDataType = dataType match {
    case DataType.STRING =>
      DataTypes.StringType
    case DataType.INTEGER =>
      DataTypes.IntegerType
    case DataType.FLOAT =>
      DataTypes.FloatType
    case DataType.DOUBLE =>
      DataTypes.DoubleType
    case DataType.BOOLEAN =>
      DataTypes.BooleanType
    case _ =>
      throw new IllegalArgumentException
  }
}
