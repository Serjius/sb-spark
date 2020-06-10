name := "agg"

version := "1.0"

scalaVersion := "2.11.12"
val SparkVersion = "2.4.5"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % SparkVersion % Provided,
    "org.apache.spark" %% "spark-sql" % SparkVersion % Provided,
    "org.apache.spark" %% "spark-mllib" % SparkVersion % Provided,
    "org.apache.spark" %% "spark-streaming" % SparkVersion % Provided,
    "org.apache.spark" %% "spark-sql-kafka-0-10" % SparkVersion % Provided,
)
