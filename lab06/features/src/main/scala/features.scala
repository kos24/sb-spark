import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object features extends App {

  val spark = SparkSession.builder()
    .appName("Lab06")
    .master("yarn")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()

  import spark.implicits._

  val webLogs = spark.read.json("hdfs:///labs/laba03/weblogs.json")

  val webLogsSplitted =
    webLogs
      .select('uid, explode('visits).alias("visits"))
      .select(col("uid"), col("visits.timestamp").alias("timestamp"),
        col("visits.url").alias("url"))

  val parsedUrl =
    webLogsSplitted
      .withColumn("host", lower(callUDF("parse_url", $"url", lit("HOST"))))
      .withColumn("domain", regexp_replace($"host", "www.", ""))
      .na.drop("Any", "domain" :: Nil)
      .cache

  //Getting top domains vectorized
  val topDomains =
    parsedUrl
      .groupBy('domain)
      .agg(count("*").alias("count"))
      .sort('count.desc, 'domain)
      .limit(1000)

  val orderedDomains = topDomains.sort('domain)

  val topDomainsJoined =
    parsedUrl
      .join(orderedDomains, Seq("domain"), "inner")
      .select('uid, 'timestamp, 'domain)
      .sort('domain)

  val pivotTop =
    topDomainsJoined
      .groupBy(col("uid"))
      .pivot("domain")
      .agg(count("uid"))
      .na.fill(0)

  def remove(a: Array[String], i: Int): List[String] = {
    val b = a.toBuffer
    b.remove(i)
    b.toList
  }

  val listOfColumns = remove(pivotTop.columns, 0)

  val dfWithVector =
    pivotTop
      .withColumn("domain_features", array(listOfColumns.map(x => col(s"`$x`")): _*))
      .select('uid, 'domain_features)

  //Getting time features

  //day of week
  val dfDay =
    parsedUrl
      .withColumn("dayOfWeek", date_format(($"timestamp" / 1000).cast(TimestampType), "E"))
      .na.drop("Any", "uid" :: Nil)
      .filter('uid =!= "")

  def addWebDay = udf((day: String) => {
    day match {
      case day if (day == "Mon") => "web_day_mon"
      case day if (day == "Tue") => "web_day_tue"
      case day if (day == "Wed") => "web_day_wed"
      case day if (day == "Thu") => "web_day_thu"
      case day if (day == "Fri") => "web_day_fri"
      case day if (day == "Sat") => "web_day_sat"
      case day if (day == "Sun") => "web_day_sun"
      case _ => "NA"
    }
  })

  val webDayDf = dfDay.withColumn("webDay", addWebDay('dayOfWeek))

  val pivotDfDay =
    webDayDf
      .groupBy('uid)
      .pivot("webDay")
      .agg(count("uid"))
      .na.fill(0)

  //visits per hour
  val dfHour = parsedUrl
    .withColumn("hour", date_format(($"timestamp" / 1000).cast(TimestampType), "H"))
    .na.drop("Any", "uid" :: Nil)
    .filter('uid =!= "")

  val webHourDf =
    dfHour
      .withColumn("webHour", concat(lit("web_hour_"), col("hour")))

  val pivotDfHour =
    webHourDf
      .groupBy('uid)
      .pivot("webHour")
      .agg(count("uid"))
      .na.fill(0)

  //getting fractions of web visits in particular time of a day
  val webFraction =
    dfHour
      .withColumn("webWorkHours", when('hour >= 9 && 'hour < 18, 1).otherwise(0))
      .withColumn("webEveningHours", when('hour >= 18 && 'hour < 24, 1).otherwise(0))

  val webHoursFraction =
    webFraction
      .groupBy('uid)
      .agg((sum("webWorkHours") / count("*")).alias("web_fraction_work_hours"),
        (sum("webEveningHours") / count("*")).alias("web_fraction_evening_hours"))

  //joining datasets
  val result =
    webHoursFraction
      .join(pivotDfDay, Seq("uid"), "left")
      .join(pivotDfHour, Seq("uid"), "left")
      .join(dfWithVector, Seq("uid"), "left")

  val users_items = spark.read.parquet("/user/konstantin.rebrin/users-items/*")

  val finalDf = result.join(users_items, Seq("uid"), "full").na.fill(0)

  finalDf.write.mode("overwrite").parquet("hdfs:///user/konstantin.rebrin/features")
}
