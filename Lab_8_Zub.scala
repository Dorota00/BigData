// Databricks notebook source
// MAGIC %md
// MAGIC Zadanie 1

// COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

// COMMAND ----------

spark.catalog.clearCache()

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schema = StructType(
  List(
    StructField("timestamp", StringType, false),
    StructField("site", StringType, false),
    StructField("requests", IntegerType, false)
  )
)

val fileName = "dbfs:/databricks-datasets/wikipedia-datasets/data-001/pageviews/raw"

var df = spark.read
  .option("header", "true")
  .option("sep", "\t")
  .schema(schema)
  .csv(fileName)

val path = "dbfs:/wikipedia.parquet"

df.write.mode("overwrite").parquet(path)

df.explain
df.count()

// COMMAND ----------

df.show()

// COMMAND ----------

df.rdd.getNumPartitions

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(8)
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(1)  
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(7)
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(9)
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(16)
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(24)
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(96)
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(200)
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(4000)
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .coalesce(6)
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .coalesce(5)
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .coalesce(4)
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .coalesce(3)
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .coalesce(2)
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .coalesce(1)
  .groupBy("site")
  .sum()

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 2

// COMMAND ----------

import java.nio.file.{Paths, Files}

def path_exists(path:String):Boolean = { 
    val res = dbutils.fs.ls(path)
    if( res.length > 0 ){
      return true
    }
    return false
}


// COMMAND ----------

def check_extenstion(path:String):Boolean = {
    val fileName = Paths.get(path).getFileName  
    val extension = fileName.toString.split("\\.").last
    if (extension != "parquet") {
      return false
    }
    return true
  }


// COMMAND ----------

def isNumber(str:String): Boolean = {
    val regex = "\\d+\\.?\\d+"
    return str.matches(regex)
}

// COMMAND ----------

def isEmpty(str:String): Boolean = {
    return str.isEmpty
}

// COMMAND ----------

def table_exists(table: String) =
  sqlContext.tableNames.contains(table)

// COMMAND ----------

import org.apache.spark.sql.DataFrame

def isEmpty(df:DataFrame): Boolean = {
  if(df.rdd.take(1).length == 0 ){
    return true
  }
   return false
  
}
