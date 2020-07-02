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

}
