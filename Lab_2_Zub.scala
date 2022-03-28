// Databricks notebook source
// Zadanie 1
import org.apache.spark.sql.types.{ByteType, ShortType, IntegerType, DoubleType, StringType, DateType, StructType, StructField}

val schema = StructType(Array(
  StructField("imdb_title_id", StringType, false),
  StructField("title", StringType, false),
  StructField("orginal_title", StringType, false),
  StructField("year", ShortType, false),
  StructField("date_published", DateType, false),
  StructField("genre", StringType, false),
  StructField("duration", ShortType, false),
  StructField("country", StringType, false),
  StructField("language", StringType, false),
  StructField("director", StringType, false),
  StructField("writer", StringType, false),
  StructField("production_company", StringType, false),
  StructField("actors", StringType, false),
  StructField("description", StringType, false),
  StructField("avg_vote", DoubleType, false),
  StructField("votes", IntegerType, false),
  StructField("budget", StringType, true),
  StructField("usa_gross_income", StringType, true),
  StructField("worlwide_gross_income", StringType, true),
  StructField("metascore", ByteType, true),
  StructField("reviews_from_users", ShortType, true),
  StructField("reviews_from_critics", ShortType, true)
))

val csv_path = "dbfs:/FileStore/tables/Files/movies.csv"
val movies_csv_df = spark.read.format("csv")
            .option("header","true")
            .schema(schema)
            .load(csv_path)

display(movies_csv_df)

// COMMAND ----------

//Zadanie 2
movies_csv_df.toJSON.head(2)

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

def jsonToDataFrame(json: String, schema: StructType = null): DataFrame = {
  val reader = spark.read
  Option(schema).foreach(reader.schema)
  reader.json(sc.parallelize(Array(json)))
}

val string = """
[{"imdb_title_id":"tt0000009","title":"Miss Jerry","orginal_title":"Miss Jerry","year":1894,"date_published":"1894-10-09","genre":"Romance","duration":45,"country":"USA","language":"None","director":"Alexander Black","writer":"Alexander Black","production_company":"Alexander Black Photoplays","actors":"Blanche Bayliss, William Courtenay, Chauncey Depew","description":"The adventures of a female reporter in the 1890s.","votes":154}, {"imdb_title_id":"tt0000574","title":"The Story of the Kelly Gang","orginal_title":"The Story of the Kelly Gang","year":1906,"genre":"Biography, Crime, Drama","duration":70,"country":"Australia","language":"None","director":"Charles Tait","writer":"Charles Tait","production_company":"J. and N. Tait","actors":"Elizabeth Tait, John Tait, Norman Campbell, Bella Cola, Will Coyne, Sam Crewes, Jack Ennis, John Forde, Vera Linden, Mr. Marshall, Mr. McKenzie, Frank Mills, Ollie Wilson","description":"True story of notorious Australian outlaw Ned Kelly (1855-80).","votes":589,"budget":"$ 2250"}]
"""

val movies_json_df = jsonToDataFrame(string,schema)
display(movies_json_df.select(collect_list('title) as 'title))

// COMMAND ----------

// Zadanie 3
import org.apache.spark.sql.functions._

val kolumny = Seq("timestamp","unix", "date","string")
val dane = Seq(("2015-03-22T14:13:34", 1646641525847L,"May, 2021","24-03-2022 11-01-19-403"),
               ("2015-03-22T15:03:18", 1646641557555L,"Mar, 2021","12-11-2018 18-56-55-409"),
               ("2015-03-22T14:38:39", 1646641578622L,"Jan, 2021","01-12-2019 16-50-59-406"))

var dataFrame = spark.createDataFrame(dane).toDF(kolumny:_*)
  .withColumn("current_date",current_date().as("current_date"))
  .withColumn("current_timestamp",current_timestamp().as("current_timestamp"))
display(dataFrame)


// COMMAND ----------

val unix_timestamp_example = dataFrame.select($"timestamp",unix_timestamp($"timestamp","yyyy-MM-dd'T'HH:mm:ss")).show()

// COMMAND ----------

val date_format_example = dataFrame.select($"timestamp",date_format($"timestamp","yyyy/MM/dd HH:mm:ss")).show()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT to_unix_timestamp("2015-03-22T14:13:34","yyyy-MM-dd'T'HH:mm:ss");

// COMMAND ----------

val from_unixtime_example = dataFrame.select($"unix",from_unixtime($"unix","yyyy-MM-dd'T'HH:mm:ss")).show()

// COMMAND ----------

val to_date_example = dataFrame.select($"current_timestamp",to_date($"current_timestamp","yyyy-MM-dd")).show()

// COMMAND ----------

val to_timestamp_example = dataFrame.select($"string", to_timestamp($"string","dd-MM-yyyy HH-mm-ss-SSS")).show()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT from_utc_timestamp('2022-03-11 12:00:00.0', 'America/Los_Angeles');

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT to_utc_timestamp('2022-03-11 12:00:00.0', 'America/Los_Angeles');

// COMMAND ----------

//Zadanie 4
val movies = spark.read.format("csv")
            .option("header","true")
            .schema(schema)
            .option("badRecordsPath","/mnt/source/badrecords")
            .load(csv_path)
display(movies)

// COMMAND ----------

val movies = spark.read.format("csv")
            .option("header","true")
            .schema(schema)
            .option("mode","PERMISSIVE")
            .load(csv_path)
display(movies)

// COMMAND ----------

val movies = spark.read.format("csv")
            .option("header","true")
            .schema(schema)
            .option("mode","DROPMALFORMED")
            .load(csv_path)
display(movies)

// COMMAND ----------

val movies = spark.read.format("csv")
            .option("header","true")
            .schema(schema)
            .option("mode","FAILFAST")
            .load(csv_path)
display(movies)

// COMMAND ----------

// Zadanie 5
val writer = movies_csv_df.write
writer.format("parquet")
            .save("dbfs:/FileStore/tables/Files/movies_parquet")

val movies_parquet = spark.read.format("parquet")
                    .load("dbfs:/FileStore/tables/Files/movies_parquet")
display(movies_parquet)

// COMMAND ----------

writer.format("json")
            .save("dbfs:/FileStore/tables/Files/movies_json")

val movies_json = spark.read.format("json")
                    .load("dbfs:/FileStore/tables/Files/movies_json")
display(movies_json)
