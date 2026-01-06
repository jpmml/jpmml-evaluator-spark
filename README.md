JPMML-Evaluator-Spark [![Build Status](https://github.com/jpmml/jpmml-evaluator-spark/workflows/maven/badge.svg)](https://github.com/jpmml/jpmml-evaluator-spark/actions?query=workflow%3A%22maven%22)
=====================

PMML evaluator library for Apache Spark.

# Features #

This library provides an Apache Spark ML-compatible wrapper for the [JPMML-Evaluator](https://github.com/jpmml/jpmml-evaluator) library.

# Prerequisites #

* Apache Spark 3.0.X through 3.5.X, 4.0.X or 4.1.X.

# Installation #

### Compatibility matrix

Active development branches:

| JPMML-Evaluator-Spark branch | Apache Spark version | Scala version |
|------------------------------|----------------------|---------------|
| [`2.0.X`](https://github.com/jpmml/jpmml-evaluator-spark/tree/2.0.X) | 3.0.X through 3.5.X | 2.12.X |
| [`master`](https://github.com/jpmml/jpmml-evaluator-spark/tree/master/) | 4.0.X and 4.1.X | 2.13.X |

Archived development branches:

| JPMML-Evaluator-Spark branch | Apache Spark version | Scala version |
|------------------------------|----------------------|---------------|
| [`1.4.X`](https://github.com/jpmml/jpmml-evaluator-spark/tree/1.4.X) | 3.0.X through 3.5.X | 2.12.X |

### Library

The JPMML-Evaluator-Spark library JAR file (together with accompanying Java source and Javadocs JAR files) is released via [Maven Central Repository](https://repo1.maven.org/maven2/org/jpmml/).

The current version is **2.0.0** (5 January, 2026).

```xml
<dependency>
	<groupId>org.jpmml</groupId>
	<artifactId>jpmml-evaluator-spark</artifactId>
	<version>2.0.0</version>
</dependency>
```

### Runtime uber-JAR file

Enter the project root directory and build using [Apache Maven](https://maven.apache.org/):

```
mvn clean install
```

The build produces two JAR files:
* `target/jpmml-evaluator-spark-2.0-SNAPSHOT.jar` - Library JAR file.
* `target/jpmml-evaluator-spark-runtime-2.0-SNAPSHOT.jar` - Runtime uber-JAR file.

Use the library JAR file for integrating JPMML-Evaluator into a full-scale Apache Spark application. Note that you will need to solve a number of dependency conflicts in your build configuration file.

Use the runtime uber-JAR file for _ad hoc_ tasks, such as adding JPMML-Evaluator to a Toree Jupyter notebook.

# Usage #

## Workflow

Build a `org.jpmml.evaluator.Evaluator` object that will do the actual evaluation work:

```scala
import java.io.InputStream
import org.jpmml.evaluator.{Evaluator, LoadingModelEvaluatorBuilder}

val pmmlIs: InputStream = ???

val evaluator: Evaluator = try {
	new LoadingModelEvaluatorBuilder()
		.load(pmmlIs)
		.build()
} finally {
	pmmlIs.close()
}

// Perform self-check
evaluator.verify()
```

Wrap this `Evaluator` object into an `org.jpmml.evaluator.spark.PMMLTransformer` object to make it look and behave like a regular Apache Spark ML transformer.

There are two built-in implementation classes available:

* `org.jpmml.evaluator.spark.FlatPMMLTransformer` 
* `org.jpmml.evaluator.spark.NestedPMMLTransformer` 

They are functionally identical evaluation-wise.
The only difference is about how the result columns are structured (flat layout vs. nested layout).

It is possible to toggle column groups (ie. inputs, targets and outputs) on and off to keep the results maximally relevant.

```scala
import org.jpmml.evaluator.spark.{FlatPMMLTransformer, NestedPMMLTransformer, PMMLTransformer}

val pmmlTransformer = new FlatPMMLTransformer(evaluator)
//val pmmlTransformer = new NestedPMMLTransformer(evaluator)
```

A newly-constructed `PMMLTransformer` object is instantly ready for transformation work (ie. it does not exhibit any fitting behaviour).

```scala
val df = ???

val transformedDf = pmmlTransformer.transform(df)
```

Evaluation exceptions, if any, are caught and stored in a dedicated exceptions column.

## API

### `PMMLTransformer`

Abstract base class that provides common parameters and functionality.

Parameters:

* `inputs: BooleanParam = true`. Copy all columns from the input dataset to the transformed dataset?
* `targets: BooleanParam = true`. Produce columns for PMML target fields (ie. primary results)?
* `outputs: BooleanParam = true`. Produce columns for PMML output fields (ie. secondary results)?
* `exceptionCol: Param[String] = "pmmlException"`. The name of the exceptions column.
* `syntheticTargetName: Param[String] = "_target"`. The substitute name for a synthetic target field column (the default name for a synthetic target field is `null`, which is not a valid Apache Spark column name).

### `FlatPMMLTransformer`

Concrete implementation class, which maps all PMML result fields to top-level columns.

Parameters: N/A

Transformed schema for the example `DecisionTreeIris` model:

* Four input columns.
* One PMML target field column.
* Three PMML output field columns.
* One exceptions column.

```
root
 |-- Sepal.Length: double (nullable = true)
 |-- Sepal.Width: double (nullable = true)
 |-- Petal.Length: double (nullable = true)
 |-- Petal.Width: double (nullable = true)
 |-- Species: string (nullable = true)
 |-- probability(setosa): double (nullable = true)
 |-- probability(versicolor): double (nullable = true)
 |-- probability(virginica): double (nullable = true)
 |-- pmmlException: string (nullable = true)
```

If the evaluation fails for some row, then all PMML result fields columns contain `null` for that row.

### `NestedPMMLTransformer`

Concrete implementation class, which maps all PMML target and output fields to a single top-level nested results column.

Parameters:

* `resultsCol: Param[String] = "pmmlResults"`. The name of the nested results column.

Transformed schema for the example `DecisionTreeIris` model:

* Four input columns.
* One nested results column, containing one PMML target field and three output fields.
* One exceptions column.

```
root
 |-- Sepal.Length: double (nullable = true)
 |-- Sepal.Width: double (nullable = true)
 |-- Petal.Length: double (nullable = true)
 |-- Petal.Width: double (nullable = true)
 |-- pmmlResults: struct (nullable = true)
 |    |-- Species: string (nullable = true)
 |    |-- probability(setosa): double (nullable = true)
 |    |-- probability(versicolor): double (nullable = true)
 |    |-- probability(virginica): double (nullable = true)
 |-- pmmlException: string (nullable = true)
```

If the evaluation fails for some row, then the results column contains `null` for that row.

Use the dot notation to access individual fields afterwards:

```scala
transformedDf.select("pmmlResults.Species").show()
```

# License #

JPMML-Evaluator-Spark is licensed under the terms and conditions of the [GNU Affero General Public License, Version 3.0](https://www.gnu.org/licenses/agpl-3.0.html).
For a quick summary of your rights ("Can") and obligations ("Cannot" and "Must") under AGPLv3, please refer to [TLDRLegal](https://tldrlegal.com/license/gnu-affero-general-public-license-v3-(agpl-3.0)).

If you would like to use JPMML-Evaluator-Spark in a proprietary software project, then it is possible to enter into a licensing agreement which makes it available under the terms and conditions of the [BSD 3-Clause License](https://opensource.org/licenses/BSD-3-Clause) instead.

# Additional information #

JPMML-Evaluator-Spark is developed and maintained by Openscoring Ltd, Estonia.

Interested in using JPMML software in your software? Please contact [info@openscoring.io](mailto:info@openscoring.io)
