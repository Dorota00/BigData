// Databricks notebook source
// MAGIC %md
// MAGIC Zadanie 1

// COMMAND ----------

// MAGIC %md
// MAGIC Hive nie posiada indeksacji od wersji 3.0.
// MAGIC Metody przyśpieszając działanie zapytań:
// MAGIC - Materialized views with automatic rewriting 
// MAGIC - Użycie formatów Parquet, ORC

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 3

// COMMAND ----------

// MAGIC %sql
// MAGIC Create database if not exists Sample;
// MAGIC CREATE TABLE IF NOT EXISTS Sample.Transactions ( AccountId INT, TranDate VARCHAR(10), TranAmt DECIMAL(8, 2));
// MAGIC CREATE TABLE IF NOT EXISTS Sample.Logical (RowID INT,FName VARCHAR(20), Salary SMALLINT);

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC INSERT INTO Sample.Transactions VALUES 
// MAGIC ( 1, '2011-01-01', 500),
// MAGIC ( 1, '2011-01-15', 50),
// MAGIC ( 1, '2011-01-22', 250),
// MAGIC ( 1, '2011-01-24', 75),
// MAGIC ( 1, '2011-01-26', 125),
// MAGIC ( 1, '2011-01-28', 175),
// MAGIC ( 2, '2011-01-01', 500),
// MAGIC ( 2, '2011-01-15', 50),
// MAGIC ( 2, '2011-01-22', 25),
// MAGIC ( 2, '2011-01-23', 125),
// MAGIC ( 2, '2011-01-26', 200),
// MAGIC ( 2, '2011-01-29', 250),
// MAGIC ( 3, '2011-01-01', 500),
// MAGIC ( 3, '2011-01-15', 50 ),
// MAGIC ( 3, '2011-01-22', 5000),
// MAGIC ( 3, '2011-01-25', 550),
// MAGIC ( 3, '2011-01-27', 95 ),
// MAGIC ( 3, '2011-01-30', 2500);
// MAGIC 
// MAGIC INSERT INTO Sample.Logical VALUES 
// MAGIC (1,'George', 800),
// MAGIC (2,'Sam', 950),
// MAGIC (3,'Diane', 1100),
// MAGIC (4,'Nicholas', 1250),
// MAGIC (5,'Samuel', 1250),
// MAGIC (6,'Patricia', 1300),
// MAGIC (7,'Brian', 1500),
// MAGIC (8,'Thomas', 1600),
// MAGIC (9,'Fran', 2450),
// MAGIC (10,'Debbie', 2850),
// MAGIC (11,'Mark', 2975),
// MAGIC (12,'James', 3000),
// MAGIC (13,'Cynthia', 3000),
// MAGIC (14,'Christopher', 5000);

// COMMAND ----------

import org.apache.spark.sql.catalog.Catalog
val databases = spark.catalog. listDatabases()
display(databases)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM Sample.Logical;

// COMMAND ----------

val deleteContent = (database:String) => {
    val names = spark.catalog.listTables(database).select("name").as[String].collect.toList
    names.map(f=>spark.sql(s"DELETE FROM $database.$f"))
}

// COMMAND ----------

deleteContent("Sample")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM Sample.Logical;

// COMMAND ----------

// MAGIC %md
// MAGIC Jar

// COMMAND ----------

// MAGIC %md
// MAGIC https://github.com/Dorota00/Jar
