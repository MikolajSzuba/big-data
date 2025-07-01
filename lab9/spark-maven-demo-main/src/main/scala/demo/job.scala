package demo

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Job {
  private val spark = SparkSession.builder.appName("demo-job").getOrCreate()

  /** 1️⃣ wczytaj CSV */
  def load(path: String): DataFrame =
    spark.read.option("header", "true").csv(path)

  /** 2️⃣ transformacja – filtr i nowa kolumna */
  def transform(df: DataFrame): DataFrame = {
    import spark.implicits._             // ← DODAJ TO!
    df.filter($"value" > 0)
      .withColumn("value_doubled", $"value" * 2)
  }

  /** 3️⃣ zapis do Parquet */
  def save(df: DataFrame, out: String): Unit =
    df.write.mode("overwrite").parquet(out)

  def main(args: Array[String]): Unit = {
    val in  = args(0)
    val out = args(1)
    val res = transform(load(in))
    res.show()
    save(res, out)
    spark.stop()
  }
}
