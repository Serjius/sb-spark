import java.nio.charset.CodingErrorAction

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{SparkSession, functions => f}

import scala.io.{Codec, Source}

case class Clients(uid: String, gender: String, age: Int)

object data_mart extends Logging {
    private val CASSANDRA_IP = "10.0.1.9"
    private val CASSANDRA_PORT = "9042"
    private val CASSANDRA_KEYSPACE = "labdata"
    private val CASSANDRA_TABLE = "clients"

    private val POSTGRESQL_IP = "10.0.1.9"
    private val POSTGRESQL_PORT = "5432"
    private val POSTGRESQL_USER = "sergey_puchnin"

    private val POSTGRESQL_DB = "labdata"
    private val POSTGRESQL_TABLE = "domain_cats"
    private val POSTGRESQL_RESULT_TABLE = "clients"

    private val ELASTIC_NODES = "10.0.1.9"
    private val ELASTIC_PORT = "9200"
    private val ELASTIC_INDEX = "visits"

    private val JSON_FILE_NAME = "hdfs:///labs/laba03/weblogs.json"

    private val testUserUID = "d50192e5-c44e-4ae8-ae7a-7cfe67c8b777"

    private def loadPassword(): String = {

        implicit val codec: Codec = Codec("UTF-8")
        codec.onMalformedInput(CodingErrorAction.REPLACE)
        codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

        logger.info("Start application")
        try {
            val in = "secret.txt"
            val src = Source.fromFile(in)
            logger.info(s"Read file $in to vector")
            val strArr = src.getLines.toArray
            src.close()
            strArr(0)

        }
        catch {
            case e: Throwable =>
                logger.error(e.getMessage)
                logger.error(e.getStackTrace.mkString("\n"))
                System.exit(-1)
                ""
        }


    }


    def main(args: Array[String]): Unit = {

        val spark = SparkSession
            .builder
            .appName("sergey.puchnin Lab03")
            .config("spark.cassandra.connection.host", CASSANDRA_IP)
            .config("spark.cassandra.connection.port", CASSANDRA_PORT)
            .getOrCreate

        spark.sparkContext.setLogLevel("WARN")

        val POSTGRESQL_PWD =  loadPassword()


        println
        println
        println

        println("Cassandra begin")

        val clientDF = spark
            .read
            .format("org.apache.spark.sql.cassandra")
            .options(Map(
                "table" -> CASSANDRA_TABLE,
                "keyspace" -> CASSANDRA_KEYSPACE
            ))
            .load()

        println(clientDF.count)
        clientDF.printSchema()
        clientDF.show(truncate = false)

        //18-24, 25-34, 35-44, 45-54, >=55
        val clientAgeDF = clientDF.select("uid", "age", "gender")
            .withColumn("age_cat",
                f.when(f.col("age") >= 18 and f.col("age") < 25, "18-24")
                    .when(f.col("age") >= 25 and f.col("age") < 35, "25-34")
                    .when(f.col("age") >= 35 and f.col("age") < 45, "35-44")
                    .when(f.col("age") >= 45 and f.col("age") < 55, "45-54")
                    .when(f.col("age") >= 55, ">=55")
                    .otherwise("Unknown")
            )


        println(clientAgeDF.count)
        clientAgeDF.printSchema()
        clientAgeDF.show(truncate = false)

        val clientResultDF = clientAgeDF.select("uid", "gender", "age_cat").cache()

        println("Cassandra done")


        println("PostgreSQL begin")
        val driver = "org.postgresql.Driver"
        Class.forName(driver)


        val domainCategoryDF = spark.read
            .format("jdbc")
            .option("driver", driver)
            .option("url", s"jdbc:postgresql://$POSTGRESQL_IP:$POSTGRESQL_PORT/$POSTGRESQL_DB")
            .option("dbtable", POSTGRESQL_TABLE)
            .option("user", POSTGRESQL_USER)
            .option("password", POSTGRESQL_PWD)
            .load()

        println(domainCategoryDF.count)
        domainCategoryDF.printSchema()
        domainCategoryDF.show(truncate = false)
        println("PostgreSQL done")

        println("Elastic start")
        val elasticDF = spark.read
            .format("org.elasticsearch.spark.sql")
            .option("es.nodes.wan.only", "true")
            .option("es.batch.write.refresh", "false")
            .option("es.port", ELASTIC_PORT)
            .option("es.nodes", ELASTIC_NODES)
            .load(ELASTIC_INDEX)

        elasticDF.cache()
        println(elasticDF.count)
        elasticDF.printSchema()
        elasticDF.show(1, truncate = 100, vertical = true)


        val onlyShopUserDF = elasticDF.filter(f.col("uid").isNotNull)

        onlyShopUserDF.filter(f.col("uid") === testUserUID).show(false)


        //add 1 for pivot. replace "-" and " " to _. lowercase. add shop_ prefix to column_name
        val tableColumnsListDF = onlyShopUserDF.select(
            f.col("uid"),
            f.col("category"),
            f.lit(1).alias("count"))
            .withColumn("shopCategory", f.regexp_replace(f.col("category"), "-", "_"))
            .withColumn("shopCategory", f.regexp_replace(f.col("shopCategory"), " ", "_"))
            .withColumn("shopCategory", f.lower(f.col("shopCategory")))
            .withColumn("shopCategory", f.concat(f.lit("shop_"), f.col("shopCategory")))

        //pivot table
        val shopResultDF = tableColumnsListDF.groupBy("uid").pivot("shopCategory").sum("count").cache()

        println("Filter group by UID, Category")
        println(shopResultDF.count)
        shopResultDF.printSchema()
        shopResultDF.show(1, 100, vertical = true)

        //just a sample
        tableColumnsListDF.filter(f.col("uid") === testUserUID).show(10, truncate = 100)

        println("Elastic done")


        println("JSON start")
        //load data
        val jsonDF = spark.read.json(JSON_FILE_NAME).toDF()

        //remove nulls
        val onlyWebUsersDF = jsonDF.filter(f.col("uid").isNotNull)

        //show raw logs for test user
        onlyWebUsersDF.filter(f.col("uid") === testUserUID).show(false)


        //check column names and types
        onlyWebUsersDF.printSchema()

        //explode array column "visits" to list column "visitsList"
        val explodeUrlListDF = onlyWebUsersDF.select(f.col("uid"), f.explode(f.col("visits")).alias("visitsList"))

        //parse_url and get host name
        val parseUrlDF = explodeUrlListDF
            .select(f.col("uid"), f.col("visitsList.url").alias("web_url"))
            .withColumn("full_domain", f.callUDF("parse_url", f.col("web_url"), f.lit("HOST")))

        //remove www. from domain names
        val webShopUsrDomainDF = parseUrlDF.select(f.col("uid"), f.col("full_domain"))
            .withColumn("domain",
                f.regexp_replace(f.col("full_domain"), "^www.", ""))

        println(webShopUsrDomainDF.count)
        webShopUsrDomainDF.printSchema()
        webShopUsrDomainDF.show(10, truncate = 100)


        println("JSON done")

        println("join parts start")
        //join domain category to the web visits
        val joinedWebUserCategoryDF = webShopUsrDomainDF.join(domainCategoryDF, Seq("domain"), "inner")

        val webCategoryDF = joinedWebUserCategoryDF.select("uid", "domain", "category")
        webCategoryDF.printSchema()
        webCategoryDF.show(10, 150)


        //pivot
        val webPivotDF = webCategoryDF.select(
            f.col("uid"),
            f.col("category"),
            f.lit(1).alias("count"))
            .withColumn("webCategory", f.concat(f.lit("web_"), f.col("category")))
        webPivotDF.show(10, 150)

        //pivot table
        val webResultDF = webPivotDF.groupBy("uid").pivot("webCategory").sum("count").cache()
        println(webResultDF.count)
        webResultDF.printSchema()


        //just a sample
        webResultDF.filter(f.col("uid") === testUserUID).show(20, truncate = 100, vertical = true)


        val joinedClientShop = clientResultDF.join(shopResultDF, Seq("uid"), "left")
        val joinedClientShopWeb = joinedClientShop.join(webResultDF, Seq("uid"), "left")

        joinedClientShopWeb.printSchema


        //check before commit
        joinedClientShopWeb.groupBy("gender", "age_cat").count.show



        //from final result
        joinedClientShopWeb.filter(f.col("uid") === testUserUID).show(1, 100, vertical = true)
        //from Client
        clientAgeDF.filter(f.col("uid") === testUserUID).show(false)
        //from Shop
        tableColumnsListDF.filter(f.col("uid") === testUserUID).show(false)
        //from web
        webCategoryDF.filter(f.col("uid") === testUserUID).show(false)


        println("join parts done")

        println("try to save it ")


        joinedClientShopWeb
            .write
            .mode("append")
            .format("jdbc")
            .option("driver", driver)
            .option("url", s"jdbc:postgresql://$POSTGRESQL_IP:$POSTGRESQL_PORT/")
            .option("dbtable", POSTGRESQL_RESULT_TABLE)
            .option("user", POSTGRESQL_USER)
            .option("password", POSTGRESQL_PWD)
            .save()

        println("Done!")
        println("check from psql ")

    }


}