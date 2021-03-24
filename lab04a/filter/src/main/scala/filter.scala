
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}

object filter extends App {

  val spark = SparkSession.builder()
    .appName("Lab04")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  val topic_name = spark.conf.get("spark.filter.topic_name")
  val offset = spark.conf.get("spark.filter.offset")
  val output_dir_prefix = spark.conf.get("spark.filter.output_dir_prefix")


  val kafkaLogs = spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("subscribe", topic_name)
    .option("failOnDataLoss", "false")
    .option("startingOffsets",
      if (offset == "earliest") s"earliest"
      else s""" {"${topic_name}": {"0":${offset}}}""")
    .load()

  val jsonString = kafkaLogs.select('value.cast("string")).as[String]

  val logsParsed = spark.read.json(jsonString)

  def addTime = udf { (timestamp: Long) => {

    val time = Instant.ofEpochMilli(timestamp)
    val timeUtc = ZonedDateTime.ofInstant(time, ZoneId.of("UTC"))
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    dateTimeFormatter.format(timeUtc)
  }
  }

  val logsWithDate = logsParsed.withColumn("date", addTime('timestamp))
    .withColumn("p_date", 'date)

  logsWithDate
    .filter('event_type === "view")
    .write
    .mode("overwrite")
    .partitionBy("p_date")
    .json(output_dir_prefix + "/view/")

  logsWithDate
    .filter('event_type === "buy")
    .write.mode("overwrite")
    .partitionBy("p_date")
    .json(output_dir_prefix + "/buy/")
}
