package org.jpmml.evaluator.spark.sql

object DatasetUtil {
  def escapeColumnName(name: String): String = {
    // A column name that contains special characters needs to be surrounded by backticks
    if (name.indexOf('.') > -1) "`" + name + "`" else name
  }
}
