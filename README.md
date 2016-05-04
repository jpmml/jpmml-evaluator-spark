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

Constructing an instance of Spark ML transformer class `org.jpmml.spark.PMMLPredictionModel` based on a PMML document in local filesystem:
```java
File pmmlFile = ...;
Evaluator evaluator = EvaluatorUtil.createEvaluator(pmmlFile);
PMMLPredictionModel pmmlModel = new PMMLPredictionModel(evaluator);
```

This class adheres to the specification of the [`org.apache.spark.ml.PredictionModel`] (https://spark.apache.org/docs/latest/api/java/org/apache/spark/ml/PredictionModel.html) transformer class.

Scoring data:
```java
DataFrame dataFrame = ...;
DataFrame transformedDataFrame = pmmlModel.transform(dataFrame);
```

**A note about building and packaging JPMML-Spark applications**. The JPMML-Evaluator library depends on JPMML-Model and Google Guava library versions that are in conflict with the ones that are bundled with Apache Spark and/or Apache Hadoop. This conflict can be easily solved by relocating JPMML-Evaluator library dependencies to a different namespace using the [Apache Maven Shade Plugin] (https://maven.apache.org/plugins/maven-shade-plugin/). Please see the JPMML-Spark example application for a worked out example.

## Example application ##

The example application JAR file contains an executable class `org.jpmml.spark.EvaluationExample`.

This class expects three command-line arguments:

1. The path of the **model** PMML file in local filesystem.
2. The path of the **input** CSV file in local filesystem.
3. The path of the **output** directory in local filesystem.

For example:
```
spark-submit --master local --class org.jpmml.spark.EvaluationExample example-1.0-SNAPSHOT.jar DecisionTreeIris.pmml Iris.csv /tmp/DecisionTreeIris
```

# License #

JPMML-Spark is dual-licensed under the [GNU Affero General Public License (AGPL) version 3.0] (http://www.gnu.org/licenses/agpl-3.0.html) and a commercial license.

# Additional information #

Please contact [info@openscoring.io] (mailto:info@openscoring.io)
