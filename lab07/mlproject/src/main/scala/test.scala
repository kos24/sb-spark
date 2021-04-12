import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._


object test extends App {
  val spark = SparkSession
    .builder()
    .appName("lab07_test")
    .master("yarn")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()

  import spark.implicits._

  val input_dir = spark.conf.get("spark.mlproject.input_dir", "hdfs:///user/konstantin.rebrin/pipelineModel")

  val kafkaParams = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "subscribe" -> "konstantin_rebrin",
    "startingOffsets" -> """earliest"""
//    "startingOffsets" -> """latest"""
    //    "maxOffsetsPerTrigger" -> "10"
  )

  val sdf = spark
    .readStream
    .format("kafka")
    .options(kafkaParams)
    .load

  val jsonSchema2 = StructType(Array(
    StructField("uid", StringType),
    StructField("visits", ArrayType(
      StructType(Array(
        StructField("url",StringType),
        StructField("timestamp", LongType))
      )))
  ))

  val parsedJson2 = sdf
    .select('value.cast("string"))
    .select(from_json($"value", jsonSchema2).as("data"))
    .select("data.*")

  val webLogsSplitted2 =
    parsedJson2
      .select('uid,explode('visits).alias("visits"))
      .select(col("uid"), col("visits.url").alias("url"))

  val cleanLogs2 =
    webLogsSplitted2
      .withColumn("host", lower(callUDF("parse_url", $"url", lit("HOST"))))
      .withColumn("domain", regexp_replace($"host", "www.", ""))
      .na.drop("Any", "domain"::Nil)

  val test =
    cleanLogs2
      .groupBy("uid")
      .agg(collect_list("domain").as("domains"))

  val model = PipelineModel.load(input_dir)

  val prediction = model.transform(test)

  val result = prediction
    .select(to_json(
      struct(
        col("uid"),
        col("gender_age_reversed").as("gender_age"))
    ).as("value"))

  val writer = result
    .writeStream
    .format("kafka")
         .trigger(Trigger.Once())
//    .trigger(Trigger.ProcessingTime("300 seconds"))
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("topic", "konstantin_rebrin_lab07_out")
    .option("checkpointLocation", "/user/konstantin.rebrin/chk/konstantin_rebrin_lab07")
    .outputMode("update")
    .start()

  writer.awaitTermination()
}
