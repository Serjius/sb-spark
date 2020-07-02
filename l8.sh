cd lab08/dashboard
sbt package
scp target/scala-2.11/dashboard_2.11-1.0.jar sdm:lab08/.
scp resources/spark-properties.conf sdm:lab08/resources/.
scp secret.txt sdm:lab08/.
cd ../..

#spark-submit --class train --properties-file resources/spark-properties.conf train_2.11-1.0.jar 