cd lab07/mlproject
sbt package
scp target/scala-2.11/train_2.11-1.0.jarr sdm:lab07/.
scp resources/spark-properties.conf sdm:lab07/resources.
cd ../..