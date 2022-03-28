// Databricks notebook source
// MAGIC %md
// MAGIC Zadanie 1

// COMMAND ----------

// Pobierz wszystkie tabele z schematu SalesLt i zapisz lokalnie bez modyfikacji w formacie delta
val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val tables = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM INFORMATION_SCHEMA.TABLES")
  .load()
display(tables)

// COMMAND ----------

val sales_tables = tables.filter($"TABLE_SCHEMA" === "SalesLT")
                          .select($"TABLE_NAME")
                          .map(f=>f.getString(0))
                          .collect
                          .toList

// COMMAND ----------

sales_tables.map{table_name => spark.read
                    .format("jdbc")
                    .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
                    .option("user","sqladmin")
                    .option("password","$3bFHs56&o123$")
                    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
                    .option("query",s"SELECT * FROM SalesLT.${table_name}")
                    .load()
                    .write.format("delta").save(s"dbfs:/FileStore/tables/Files/SalesLt/${table_name}")
                }

// COMMAND ----------

val df = sales_tables.map{table_name => spark.read
                    .format("delta")
                    .load(s"dbfs:/FileStore/tables/Files/SalesLt/${table_name}")
                }

// COMMAND ----------

val df_map = (sales_tables zip df).toMap

// COMMAND ----------

// W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
import org.apache.spark.sql.functions.{col,when, count}
import org.apache.spark.sql.Column

def countCol(columns:Array[String]):Array[Column]={
    columns.map(c=>{
      count(when(col(c).isNull,c)).alias(c)
    })
}

df.map{table => table.select(countCol(table.columns):_*).show()}

// COMMAND ----------

def countRow(columns:Array[String]):Column={
    columns.map(r =>{
      when(col(r).isNull, 1).otherwise(0)}).reduce(_+_)
}

df.map{table => table.withColumn("Nulls", countRow(table.columns)).show()}

// COMMAND ----------

// Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
df.map{table => table.na.fill("--").show()}

// COMMAND ----------

//Użyj funkcji drop żeby usunąć nulle
df.map{table => table.na.drop("any").count}

// COMMAND ----------

// wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
display(df_map("SalesOrderHeader"))

// COMMAND ----------

import org.apache.spark.sql.functions._
df_map("SalesOrderHeader").agg(sum("TaxAmt").as("sumTaxAmt"),sum("Freight").as("sumFreight")).show()

// COMMAND ----------

df_map("SalesOrderHeader").agg(max("TaxAmt").as("maxTaxAmt"),max("Freight").as("maxFreight")).show()

// COMMAND ----------

df_map("SalesOrderHeader").agg(mean("TaxAmt").as("meanTaxAmt"),mean("Freight").as("meanFreight")).show()

// COMMAND ----------

//Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg()
display(df_map("Product"))


// COMMAND ----------

df_map("Product").groupBy("ProductModelId","Color","ProductCategoryID").agg(sum("StandardCost").as("sumStandardCost")).show()

// COMMAND ----------

df_map("Product").groupBy("ProductModelId","Color","ProductCategoryID").agg(max("StandardCost").as("maxStandardCost")).show()

// COMMAND ----------

df_map("Product").groupBy("ProductModelId","Color","ProductCategoryID").agg(mean("StandardCost").as("meanStandardCost")).show()

// COMMAND ----------

//Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 2

// COMMAND ----------

// Stwórz 3 funkcje UDF do wybranego zestawu danych (Dwie funkcje działające na liczbach, int, double oraz jedna funkcja na string)
df_map("Product").groupBy("ProductModelId","Color","ProductCategoryID").agg(mean("StandardCost").as("meanStandardCost")).show()

// COMMAND ----------

df_map("Product").schema

// COMMAND ----------

val intUdf = udf((x: Int) => x + 5 )
val doubleUdf = udf((x: Double) => x / 100 )
val stringUdf = udf((x: String) => if (x != null) x.length else -1 )

// COMMAND ----------

df_map("Product").select(intUdf($"ProductModelID") as "intUDF", doubleUdf($"Weight") as "doubleUDF", stringUdf($"ProductNumber") as "stringUDF").show()

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 3

// COMMAND ----------

//Flatten json, wybieranie atrybutów z pliku json.
val path = "dbfs:/FileStore/tables/Files/brzydki.json"
val json_file = spark.read.option("multiline","true").json(path)
display(json_file)

// COMMAND ----------

json_file.select("jobDetails.jobId").show()
