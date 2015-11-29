JPMML-Spark
===========

PMML evaluator library for the Apache Spark cluster computing system (http://spark.apache.org/).

# Features #

* Full support for PMML specification versions 3.0 through 4.2. The evaluation is handled by the [JPMML-Evaluator] (https://github.com/jpmml/jpmml-evaluator) library.

# Prerequisites #

* Apache Spark version 1.5.0 or newer.

# Installation #

Enter the project root directory and build using [Apache Maven] (http://maven.apache.org/):
```
mvn clean install
```

The build produces two JAR files:
* `pmml-spark/target/pmml-spark-1.0-SNAPSHOT.jar` - Library JAR file.
* `pmml-spark-example/target/example-1.0-SNAPSHOT.jar` - Example application JAR file.

# Usage #

## Library ##

Constructing an instance of Apache Spark function class `org.jpmml.spark.PMMLFunction` based on a PMML document in local filesystem:
```java
File pmmlFile = ...;
Evaluator evaluator = PMMLFunctionUtil.createEvaluator(pmmlFile);
PMMLFunction pmmlFunction = new PMMLFunction(evaluator);
```

This function class is designed to work with Spark SQL. In brief, it accepts an input data record in the form of `org.apache.spark.sql.Row`, applies the scoring function to it, and produces an output data record in the form of `org.apache.spark.sql.Row`. The detailed description of data records can be retrieved using methods `PMMLFunction#getInputSchema()` and `PMMLFunction#getOutputSchema()`, respectively.

Scoring data:
```java
SQLContext sqlContext = ...;
DataFrame inputDataFrame = ...;
JavaRDD<Row> inputRDD = inputDataFrame.toJavaRDD();
JavaRDD<Row> outputRDD = inputRDD.map(pmmlFunction);
DataFrame outputDataFrame = sqlContext.createDataFrame(outputRDD, pmmlFunction.getOutputSchema());
```

A note about building and packaging JPMML-Spark applications. The JPMML-Evaluator library depends on JPMML-Model and Google Guava library versions that are in conflict with the ones that are bundled with Apache Spark and/or Apache Hadoop. It is easy to solve this conflict by relocating JPMML-Evaluator library dependencies to a different namespace using the [Apache Maven Shade Plugin] (https://maven.apache.org/plugins/maven-shade-plugin/). Please see the JPMML-Spark example application for a worked out example.

## Example application ##

The example application JAR file contains an executable class `org.jpmml.spark.Main`.

This class expects three command-line arguments:

1. The path of the **model** PMML file in local filesystem.
2. The path of the **input** CSV file in local filesystem.
3. The path of the **output** directory in local filesystem.

For example:
```
spark-submit --master local[2] --class org.jpmml.spark.Main example-1.0-SNAPSHOT.jar DecisionTreeIris.pmml Iris.csv /tmp/DecisionTreeIris
```

# License #

JPMML-Spark is dual-licensed under the [GNU Affero General Public License (AGPL) version 3.0] (http://www.gnu.org/licenses/agpl-3.0.html) and a commercial license.

# Additional information #

Please contact [info@openscoring.io] (mailto:info@openscoring.io)