JPMML-Evaluator-Spark [![Build Status](https://travis-ci.org/jpmml/jpmml-evaluator-spark.png?branch=master)](https://travis-ci.org/jpmml/jpmml-evaluator-spark)
=====================

PMML evaluator library for the Apache Spark cluster computing system (https://spark.apache.org/).

# Features #

* Full support for PMML specification versions 3.0 through 4.3. The evaluation is handled by the [JPMML-Evaluator](https://github.com/jpmml/jpmml-evaluator) library.

# Prerequisites #

* Apache Spark version 2.0.X, 2.1.X, 2.2.X, 2.3.X or 2.4.X.

# Installation #

The JPMML-Evaluator-Spark library JAR file (together with accompanying Java source and Javadocs JAR files) is released via [Maven Central Repository](https://repo1.maven.org/maven2/org/jpmml/).

The current version is **1.2.2** (16 January, 2019).

```xml
<dependency>
	<groupId>org.jpmml</groupId>
	<artifactId>jpmml-evaluator-spark</artifactId>
	<version>1.2.2</version>
</dependency>
```

**A note about building and packaging JPMML-Evaluator-Spark applications**. The JPMML-Evaluator library depends on JPMML-Model and Google Guava library versions that are in conflict with the ones that are bundled with Apache Spark and/or Apache Hadoop. This conflict can be easily solved by relocating JPMML-Evaluator library dependencies to a different namespace using the [Apache Maven Shade Plugin](https://maven.apache.org/plugins/maven-shade-plugin/).

# Usage #

Building a generic transformer based on a PMML byte stream:
```java
InputStream pmmlIs = ...;

EvaluatorBuilder evaluatorBuilder = new LoadingModelEvaluatorBuilder()
	.setLocatable(false)
	.setVisitors(new DefaultVisitorBattery())
	.load(pmmlIs);

Evaluator evaluator = evaluatorBuilder.build();

// Performing a self-check (duplicates as a warm-up)
evaluator.verify();

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
Dataset<?> inputDs = ...;

Dataset<?> resultDs = pmmlTransformer.transform(inputDs);
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

JPMML-Evaluator-Spark is dual-licensed under the [GNU Affero General Public License (AGPL) version 3.0](https://www.gnu.org/licenses/agpl-3.0.html), and a commercial license.

# Additional information #

JPMML-Evaluator-Spark is developed and maintained by Openscoring Ltd, Estonia.

Interested in using JPMML software in your application? Please contact [info@openscoring.io](mailto:info@openscoring.io)
