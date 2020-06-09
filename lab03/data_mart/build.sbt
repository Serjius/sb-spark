name := "data_mart"

version := "1.0"

scalaVersion := "2.11.12"

target := file(".target")

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.4.5" % Provided,
    "org.apache.spark" %% "spark-sql" % "2.4.5" % Provided,
    "org.apache.spark" %% "spark-mllib" % "2.4.5" % Provided,
    "org.apache.commons" % "commons-text" % "1.8",
    "com.google.code.gson" % "gson" % "2.8.6",
    "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
    "org.apache.logging.log4j" % "log4j-core" % "2.13.2" % Runtime,
    "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3",
    "org.postgresql" % "postgresql" % "42.2.12",
    "org.elasticsearch" %% "elasticsearch-spark-20" % "6.8.9"
)