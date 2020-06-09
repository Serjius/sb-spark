name := "filter"

version := "1.0"

scalaVersion := "2.11.12"

target := file(".target")

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.4.5" % Provided,
    "org.apache.spark" %% "spark-sql" % "2.4.5" % Provided,
    "org.apache.spark" %% "spark-mllib" % "2.4.5" % Provided,
    "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.5" % Provided
)
