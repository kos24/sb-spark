import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object dashboard extends App {
  val spark = SparkSession
    .builder()
    .appName("lab08")
    .master("yarn")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()

  import spark.implicits._

  val input_dir = spark.conf.get("spark.dashboard.input_dir", "hdfs:///labs/laba08")
  val path_to_model = spark.conf.get("spark.dashboard.output_dir", "hdfs:///user/konstantin.rebrin/pipelineModel")

  val jsonSchema = StructType(Array(
    StructField("date", LongType),
    StructField("uid", StringType),
    StructField("visits", ArrayType(
      StructType(Array(
        StructField("url",StringType),
        StructField("timestamp", LongType))
      )))
  ))

  val webLogs = spark.read.schema(jsonSchema).json(input_dir)

  val webLogsSplitted =
    webLogs
      .select('uid, 'date, explode('visits).alias("visits"))
      .select(col("uid"), col("date"), col("visits.url").alias("url"))

  val cleanLogs =
    webLogsSplitted
      .withColumn("host", lower(callUDF("parse_url", $"url", lit("HOST"))))
      .withColumn("domain", regexp_replace($"host", "www.", ""))

  val test =
    cleanLogs
      .groupBy("uid", "date")
      .agg(collect_list("domain").as("domains"))

  val model = PipelineModel.load(path_to_model)

  val prediction =
    model.transform(test)
      .withColumn("gender_age", col("gender_age_reversed"))
      .select("uid", "gender_age", "date")

  val esOptions =
    Map(
      "es.nodes" -> "10.0.0.5:9200",
      "es.batch.write.refresh" -> "false",
      "es.nodes.wan.only" -> "true",
      "es.net.http.auth.user" -> "konstantin.rebrin",
      "es.net.http.auth.pass" -> "twEmq7MG"
    )

  prediction
    .write
    .mode("append")
    .format("org.elasticsearch.spark.sql")
    .options(esOptions)
    .save("konstantin_rebrin_lab08/_doc")

  spark.stop()
}
