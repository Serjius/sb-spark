import org.apache.spark.sql.SparkSession


trait SparkSupport {
    val spark = SparkSession
        .builder()
        .appName("Sergey.Puchnin Lab06")
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

}
