import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, IndexToString, StringIndexer}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions._

object train extends App {
  val spark = SparkSession
    .builder()
    .appName("lab07_train")
    .master("yarn")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()

  import spark.implicits._

  val input_dir = spark.conf.get("spark.mlproject.input_dir", "hdfs:///labs/laba07")
  val output_dir = spark.conf.get("spark.mlproject.output_dir", "hdfs:///user/konstantin.rebrin/pipelineModel")

  val jsonSchema = StructType(Array(
    StructField("uid", StringType),
    StructField("gender_age", StringType),
    StructField("visits", ArrayType(
      StructType(Array(
        StructField("url",StringType),
        StructField("timestamp", LongType))
      )))
  ))

  val webLogs = spark.read.schema(jsonSchema).json(input_dir)

  val webLogsSplitted =
    webLogs
      .select('uid,'gender_age,explode('visits).alias("visits"))
      .select(col("uid"), col("visits.url").alias("url"),col("gender_age"))

  val cleanLogs =
    webLogsSplitted
      .withColumn("host", lower(callUDF("parse_url", $"url", lit("HOST"))))
      .withColumn("domain", regexp_replace($"host", "www.", ""))
      .na.drop("Any", "domain"::Nil)

  val training =
    cleanLogs
      .groupBy("uid","gender_age")
      .agg(collect_list("domain").as("domains"))

  val cv = new CountVectorizer()
    .setInputCol("domains")
    .setOutputCol("features")

  val indexer = new StringIndexer()
    .setInputCol("gender_age")
    .setOutputCol("label")
    .fit(training)

  val reverseStringIndexer = new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("gender_age_reversed")
    .setLabels(indexer.labels)

  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.001)

  val pipeline = new Pipeline()
    .setStages(Array(cv, indexer , lr, reverseStringIndexer))

  val model = pipeline.fit(training)

  model.write.overwrite().save(output_dir)

}
