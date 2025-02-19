name := "datasink"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.4.6",
    "org.apache.spark" %% "spark-sql" % "2.4.6",
    "org.scalatest" %% "scalatest" % "3.0.7" % Test
)