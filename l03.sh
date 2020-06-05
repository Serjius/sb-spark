cd lab03/data_mart
#sbt package
sbt clean assembly
#scp .target/scala-2.11/data_mart_2.11-1.0.jar sdm:lab03/.
scp .target/scala-2.11/data_mart-assembly-1.0.jar sdm:lab03/.
scp resources/log4j2.xml sdm:lab03/resources/.
scp secret.txt sdm:lab03/.
cd ../..