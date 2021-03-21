import java.net.{URL, URLDecoder}
import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.cassandra._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object data_mart extends App {

  val conf = new SparkConf()
    .set("spark.cassandra.connection.host", "10.0.0.5")
    .set("spark.cassandra.connection.port", "9042")
    .set("spark.cassandra.output.consistency.level", "ANY")
    .set("spark.cassandra.input.consistency.level", "ONE")

  val spark = SparkSession.builder()
    .appName("Lab03")
    .config(conf = conf)
    .master("yarn")
    .getOrCreate()

  import spark.implicits._

  //importing data from Cassandra
  val tableOpts = Map("table" -> "clients", "keyspace" -> "labdata")
  val cassCustomers = spark
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(tableOpts)
    .load()

  def ageCat = udf((age: Integer) => {
    age match {
      case age if (age >= 18) && (age <= 24) => "18-24"
      case age if (age >= 25) && (age <= 34) => "25-34"
      case age if (age >= 35) && (age <= 44) => "35-44"
      case age if (age >= 45) && (age <= 54) => "45-54"
      case age if (age >= 55) => ">=55"
      case _ => "NA"
    }
  })

  val cassCustWithAgeCat = cassCustomers
    .select(col("uid"), col("gender"), ageCat('age).alias("age_cat"))


  //importing data from Elastic
  val esOptions =
    Map(
      "es.nodes" -> "10.0.0.5:9200",
      "es.batch.write.refresh" -> "false",
      "es.nodes.wan.only" -> "true",
      "es.net.http.auth.user" -> "konstantin.rebrin",
      "es.net.http.auth.pass" -> "twEmq7MG"
    )


  val logs = spark
    .read
    .format("org.elasticsearch.spark.sql")
    .options(esOptions).load("visits")
    .na.drop("any", "uid" :: Nil)

  def shopPrefix = udf((shop_cat: String) => {
    "shop_" + shop_cat.replaceAll(" |-", "_")
  })

  val elasticShopCat = logs.select(col("uid"), lower(shopPrefix('category)).alias("shop_cat"))

  //importing data from PostgreSQL
  val pgOptions = Map(
    "url" -> "jdbc:postgresql://10.0.0.5:5432/labdata",
    "dbtable" -> "domain_cats",
    "user" -> "konstantin_rebrin",
    "password" -> "twEmq7MG",
    "driver" -> "org.postgresql.Driver"
  )

  val pgCat = spark
    .read
    .format("jdbc")
    .options(pgOptions)
    .load()

  def webPrefix = udf((web_cat: String) => {
    "web_" + web_cat.replaceAll(" |-", "_")
  })

  val pgWebCat = pgCat
    .select(col("domain"), lower(webPrefix('category)).alias("web_cat"))

  //importing data from HDFS
  val webLogs = spark
    .read.json("hdfs:///labs/laba03/weblogs.json")
    .na.drop("Any", "uid" :: Nil)
    .filter('uid =!= "")

  val webLogsSplitted = webLogs
    .select('uid, explode('visits).alias("visits"))
    .select(col("uid"), col("visits.timestamp").alias("timestamp"), col("visits.url").alias("url"))

  def cleanURL = udf((url: String) => Try {
    val decodedURL = new URL(URLDecoder.decode(url, "UTF-8"))
    if (url.startsWith("http")) {
      decodedURL.getHost.replaceAll("""^www(.*?)\.""", "")
    }
    else {
      ""
    }
  } match {
    case Success(validURL) => validURL
    case Failure(e) => {
      if (url.startsWith("http")) {
        url.replaceAll("""^(?:https?)?(.*?)//(www\.)?""", "").split("/")(0)
      }
      else {
        ""
      }
    }
  })

  val webLogsCleaned = webLogsSplitted.select(col("*"), cleanURL('url).alias("cleanURL"))

  val logsWebCat = webLogsCleaned
    .join(broadcast(pgWebCat), col("cleanURL") === col("domain"), "inner")
    .select('uid, 'timestamp, 'domain, 'web_cat)

  val resWebCat = logsWebCat
    .groupBy(col("uid"))
    .pivot("web_cat")
    .agg(count("uid"))
    .na.fill(0)

  val resShopCat = elasticShopCat
    .groupBy(col("uid"))
    .pivot("shop_cat")
    .agg(count("uid"))
    .na.fill(0)

  val result = cassCustWithAgeCat
    .join(resShopCat, Seq("uid"), "left")
    .join(resWebCat, Seq("uid"), "left")
    .na.fill(0)

  result.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://10.0.0.5:5432/konstantin_rebrin")
    .option("dbtable", "clients")
    .option("user", "konstantin_rebrin")
    .option("password", "twEmq7MG")
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
}
