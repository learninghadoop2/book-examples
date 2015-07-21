# book-examples

This repository contains examples (and errata) for [Learning Hadoop 2](http://learninghadoop2.com).

## Requirements

Throughout the book we use Cloudera CDH 5.0 and Amazon EMR as reference systems. All examples target, 
and have been tested with, Java 7.

## Build the examples
The easiest way to build the examples, with CDH 5.0 dependencies, is to use the provided Gradle and sbt scripts.

### Gradle
We use [Gradle](https://gradle.org) to compile Java code and collect the required class files into a single JAR file.

```{bash}
$ ./gradlew jar
```

JARs can then be submitted to Hadoop with:

```{bash}
$ hadoop jar <job jarfile> <main class> <argument 1> ... <argument 2>
```

#### Example - Chapter 3 (Mapreduce and beyond)

To build ch3 examples 
```{bash}
$ git clone https://github.com/learninghadoop2/book-examples
$ cd book-examples/ch3
$ ./gradlew jar
```

The script will  take care of downloading a Gradle distribution from
the official repo
(https://services.gradle.org/distributions/gradle-2.0-bin.zip),
and use it to build the code under
src/main/java/com/learninghadoop2/mapreduce/. You will find the
resulting jar in build/libs/mapreduce-example.jar.

We can run the WordCount example as described in Chapter 3:
```{bash}
$ hadoop jar build/libs/mapreduce-example.jar \
com.learninghadoop2.mapreduce.WordCount \
input.txt \
output
```


For more information on how gradle is bootstrapped to run the build,
refer to https://docs.gradle.org/current/userguide/gradle_wrapper.html
The gradle_wrapper plugin is distributed with the examples 
(gradle/wrapper/gradle-wrapper.jar).


### SBT

We use [sbt](www.scala-sbt.org) to build, manage, and execute the Spark examples in Chapter 5.

The build.sbt file controls the codebase metadata and software dependencies.

The source code for all examples can be compiled with:
```{bash}
$ cd ch5
$ sbt compile
```

Or, it can be packaged into a JAR file with:
```{bash}
$ sbt package
```

For Spark in standalone mode, an helper script to execute compiled classes can be generated with:
```{bash}
$ sbt add-start-script-tasks
$ sbt start-script
```
The helper can be invoked as follows:

```{bash}
$ target/start <class name> <master> <param1> … <param n>
```


#### YARN on CDH5

To run the examples on a YARN grid on CDH5, you can build a JAR file using:
```{bash}
$ sbt package
```

and then ship it to the Resource Manager using the spark-submit command:

```{bash}
./bin/spark-submit --class application.to.execute --master yarn-cluster [options] target/scala-2.10/chapter-4_2.10-1.0.jar [<param1> … <param n>]
```
Unlike the standalone mode, we don't need to specify a <master> URI. 

More information on launching Spark on YARN can be found at http://spark.apache.org/docs/latest/running-on-yarn.html.
