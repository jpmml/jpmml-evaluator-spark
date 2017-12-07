package org.jpmml.evaluator.spark.sql

import org.apache.spark.sql.types.{DataType, DataTypes}

object SchemaUtil {
  def translateDataType(dataType: org.dmg.pmml.DataType): DataType =
  {
    import org.dmg.pmml.DataType._
    dataType match {
      case STRING =>
        DataTypes.StringType
      case INTEGER =>
        DataTypes.IntegerType
      case FLOAT =>
        DataTypes.FloatType
      case DOUBLE =>
        DataTypes.DoubleType
      case BOOLEAN =>
        DataTypes.BooleanType
      case _ =>
        throw new IllegalArgumentException
    }
  }
}
