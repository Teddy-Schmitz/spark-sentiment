name := "SparkStreaming"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.2.1" % "provided"

libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "3.0.6"

libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.2.1" excludeAll(ExclusionRule(organization="org.twitter4j"))

libraryDependencies += "net.debasishg" %% "redisclient" % "2.15"