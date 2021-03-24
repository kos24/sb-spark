
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}

object agg extends App {

  val spark = SparkSession
    .builder()
    .master("yarn")
    .getOrCreate()

  import spark.implicits._

  val kafkaParams = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
//    "subscribe" -> "konstantin.rebrin",
    "subscribe" -> "lab04_input_data",
    "failOnDataLoss" -> "false",
    "startingOffsets" -> """earliest"""
  )

  val sdf = spark
    .readStream
    .format("kafka")
    .options(kafkaParams)
    .load

  val jsonSchema = StructType(Array(
    StructField("category", StringType),
    StructField("event_type", StringType),
    StructField("item_id", StringType),
    StructField("item_price", LongType),
    StructField("timestamp", LongType),
    StructField("uid", StringType)
  ))

  val parsedJson = sdf
    .select('value.cast("string"))
    .select(from_json($"value", jsonSchema).as("data"))
    .select("data.*")

//  def addTime = udf {(timestamp:Long) => {
//
//    val time = Instant.ofEpochMilli(timestamp)
//    val timeUtc = ZonedDateTime.ofInstant(time, ZoneId.of("UTC"))
//    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S")
//    dateTimeFormatter.format(timeUtc)
//  }
//  }

  val parsedWithDate = parsedJson
      .withColumn("date", ('timestamp/1000).cast(TimestampType))

  val groupedHCount = parsedWithDate
    //     .withWatermark("date", "60 minutes")
    .groupBy(window($"date", "1 hour"))
    .agg(sum(when('event_type === "buy",'item_price).otherwise(0)).as("revenue"),
      sum(when('uid.isNotNull,1).otherwise(0)).as("visitors"),
      sum(when('event_type === "buy",1).otherwise(0)).as("purchases"),
      (sum(when('event_type === "buy",'item_price).otherwise(0))/sum(when('event_type === "buy",1).otherwise(0))).as("aov"))

  val result = groupedHCount
    .select(col("window.start").cast("long").as("start_ts"),
      col("window.end").cast("long").as("end_ts"),
      col("revenue"),
      col("visitors"),
      col("purchases"),
      col("aov")
    )

  val jsoned = result.select(to_json(struct("*")).as("value"))

  val writer = jsoned
    .writeStream
    .format("kafka")
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("topic", "konstantin_rebrin_lab04b_out")
    .option("checkpointLocation", "/user/konstantin.rebrin/chk/konstantin_rebrin")
    .outputMode("update")
    .start()

  writer.awaitTermination()
}
