import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs._

import scala.util.Try

object users_items extends App {

  val spark = SparkSession.builder()
    .appName("Lab05")
    .master("local[1]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()

  import spark.implicits._

  val write_mode = spark.conf.get("spark.users_items.update")
  val input_dir = spark.conf.get("spark.users_items.input_dir")
  val output_dir = spark.conf.get("spark.users_items.output_dir")


  //  val jsonSchema = StructType(Array(
  //    StructField("category", StringType),
  //    StructField("event_type", StringType),
  //    StructField("item_id", StringType),
  //    StructField("item_price", LongType),
  //    StructField("timestamp", LongType),
  //    StructField("uid", StringType),
  //    StructField("date", StringType)
  //  ))

  //reading buy_items
  val buyLogs = spark
    .read
    .format("json")
    //    .schema(jsonSchema)
    .json(input_dir + "/buy/*")
    .na.drop("Any", "uid" :: Nil)
    .filter('uid =!= "")

  def buyPrefix = udf((buy_cat: String) => {
    "buy_" + buy_cat.replaceAll("( |-)", "_")
  })

  val buyLogsNormalized = buyLogs
    .select(col("uid"), col("date"), lower(buyPrefix('item_id)).alias("item_id"))

  val buyMax = buyLogsNormalized
    .agg(max(col("date")))
    .first
    .getString(0)
    .toInt

  val buyLogsPivot = buyLogsNormalized
    .groupBy(col("uid"))
    .pivot("item_id")
    .agg(count("uid"))
    .na.fill(0)


  //reading view_items
  val viewLogs = spark
    .read
    .format("json")
    //    .schema(jsonSchema)
    .json(input_dir + "/view/*")
    .na.drop("Any", "uid" :: Nil)
    .filter('uid =!= "")

  def viewPrefix = udf((view_cat: String) => {
    "view_" + view_cat.replaceAll("( |-)", "_")
  })

  val viewLogsNormalized = viewLogs
    .select(col("uid"), col("date"), lower(viewPrefix('item_id)).alias("item_id"))

  val viewMax = viewLogsNormalized
    .agg(max(col("date")))
    .first
    .getString(0)
    .toInt

  val viewLogsPivot = viewLogsNormalized
    .groupBy(col("uid"))
    .pivot("item_id")
    .agg(count("uid"))
    .na.fill(0)

  def maxValue(buyMax: Int, viewMax: Int) = {
    if (buyMax >= viewMax) {
      buyMax
    } else viewMax

  }

  val maxDate = maxValue(buyMax, viewMax)

  val result = viewLogsPivot.join(buyLogsPivot, Seq("uid"), "full").na.fill(0)

  //checking if visits folder not empty
  //  val hdfsFolder = new Path(input_dir)
  //  val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  //
  //  val hdfsFile = hdfs
  //    .listStatus(hdfsFolder)
  //    .map(_.getPath)
  //    .filter(_.getName.startsWith("20"))
  //    .map(_.toString.split("/").lastOption)
  //
  //  val intArray = hdfsFile.flatMap(s => Try(s.getOrElse("").toInt).toOption)
  //
  //  var maxDateToRead = 0
  //  if (!intArray.isEmpty) {
  //    maxDateToRead = intArray.maxBy(x => x)
  //  }
  //
  //  if (maxDateToRead != 0 && write_mode == "1") {

  if (write_mode == "1") {

    //reading existing matrix users-items
    val users_items = spark
      .read
      .parquet(output_dir + "/*")
      .na.drop("Any", "uid" :: Nil)
      .filter('uid =!= "")

    val appendedDf = users_items.union(result)

    appendedDf
      .write
      .mode("overwrite")
      .parquet(output_dir + s"""/${maxDate}""")
  } else {
    result
      .write
      .mode("overwrite")
      .parquet(output_dir + s"""/${maxDate}""")

  }

}
