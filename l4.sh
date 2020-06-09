cd lab04a/filter
sbt package
#sbt clean assembly
scp .target/scala-2.11/filter_2.11-1.0.jar sdm:lab04/.
#scp .target/scala-2.11/data_mart-assembly-1.0.jar sdm:lab03/.
scp ./src/main/resources/log4j2.xml sdm:lab04/resources/.
#scp secret.txt sdm:lab03/.
cd ../..