JPMML-Evaluator-Spark [![Build Status](https://travis-ci.org/jpmml/jpmml-evaluator-spark.png?branch=master)](https://travis-ci.org/jpmml/jpmml-evaluator-spark)
===========

PMML evaluator library for the Apache Spark cluster computing system (http://spark.apache.org/).

# Features #

* Full support for PMML specification versions 3.0 through 4.3. The evaluation is handled by the [JPMML-Evaluator](https://github.com/jpmml/jpmml-evaluator) library.

# Prerequisites #

* Apache Spark version 1.5.X, 1.6.X or 2.0.X.

# Installation #

Enter the project root directory and build using [Apache Maven](http://maven.apache.org/):
```
mvn clean install
```

Declare JPMML-Evaluator-Spark dependency:
```xml
<dependency>
	<groupId>org.jpmml</groupId>
	<artifactId>jpmml-evaluator-spark</artifactId>
	<version>1.2-SNAPSHOT</version>
</dependency>
```

**A note about building and packaging JPMML-Evaluator-Spark applications**. The JPMML-Evaluator library depends on JPMML-Model and Google Guava library versions that are in conflict with the ones that are bundled with Apache Spark and/or Apache Hadoop. This conflict can be easily solved by relocating JPMML-Evaluator library dependencies to a different namespace using the [Apache Maven Shade Plugin](https://maven.apache.org/plugins/maven-shade-plugin/).

# Usage #

Building a generic transformer based on a PMML document in local filesystem:
```java
File pmmlFile = ...;

Evaluator evaluator = EvaluatorUtil.createEvaluator(pmmlFile);

TransformerBuilder pmmlTransformerBuilder = new TransformerBuilder(evaluator)
	.withTargetCols()
	.withOutputCols()
	.exploded(false);

Transformer pmmlTransformer = pmmlTransformerBuilder.build();
```

Building an Apache Spark ML-style regressor when the PMML document is known to contain a regression model (eg. auto-mpg dataset):
```java
TransformerBuilder pmmlTransformerBuilder = new TransformerBuilder(evaluator)
	.withLabelCol("MPG") // Double column
	.exploded(true);
```

Building an Apache Spark ML-style classifier when the PMML document is known to contain a classification model (eg. iris-species dataset):
```java
TransformerBuilder pmmlTransformerBuilder = new TransformerBuilder(evaluator)
	.withLabelCol("Species") // String column
	.withProbabilityCol("Species_probability", Arrays.asList("setosa", "versicolor", "virginica")) // Vector column
	.exploded(true);
```

Scoring data:
```java
DataFrame input = ...;
DataFrame output = pmmlTransformer.transform(input);
```

In default mode, the transformation appends an intermediary "pmml" column to the data frame, which contains all the requested result columns:
```
root
 |-- Sepal_Length: double (nullable = true)
 |-- Sepal_Width: double (nullable = true)
 |-- Petal_Length: double (nullable = true)
 |-- Petal_Width: double (nullable = true)
 |-- pmml: struct (nullable = true)
 |    |-- Species: string (nullable = false)
 |    |-- Species_probability: vector (nullable = false)
```

In exploded mode, the transformation appends all the requested result columns to the data frame:
```
root
 |-- Sepal_Length: double (nullable = true)
 |-- Sepal_Width: double (nullable = true)
 |-- Petal_Length: double (nullable = true)
 |-- Petal_Width: double (nullable = true)
 |-- Species: string (nullable = false)
 |-- Species_probability: vector (nullable = false)
```

# License #

JPMML-Evaluator-Spark is licensed under the [GNU Affero General Public License (AGPL) version 3.0](http://www.gnu.org/licenses/agpl-3.0.html). Other licenses are available on request.

# Additional information #

Please contact [info@openscoring.io](mailto:info@openscoring.io)
