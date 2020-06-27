import org.apache.spark.sql.SparkSession

trait SparkSupport {
    val spark = SparkSession
        .builder()
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    println()
    println()
    println()
    println()
    println()

    //spark-submit --name "Sergey.Puchnin lab07"\
    //
    // --class train \
    // --properties-file /resources/spark-properties.conf \
    // train_2.11-1.0.jar
}
