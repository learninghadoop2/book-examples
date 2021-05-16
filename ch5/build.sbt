enablePlugins(JavaAppPackaging)

name := "Chapter 4"

version := "1.0"

scalaVersion := "2.10.3"

resolvers += "Typesafe Repo" at "https://srepo.typesafe.com/typesafe/releases/"

//libraryDependencies += "play" % "play_2.10" % "2.1.0"
libraryDependencies += "com.google.code.gson" % "gson" % "2.2.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.1.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.1.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.1.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.1.0"

resolvers += "Akka Repository" at "https://repo.akka.io/releases/"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "1.2.1"

libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "3.0.5"
