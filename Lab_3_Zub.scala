// Databricks notebook source
// MAGIC %md
// MAGIC Zadanie 1

// COMMAND ----------

// MAGIC %md
// MAGIC Names.csv

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/names.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(namesDf)

// COMMAND ----------

// Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
import org.apache.spark.sql.functions._
namesDf.withColumn("time", unix_timestamp(current_timestamp())).explain()

// COMMAND ----------

//Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
namesDf.withColumn("height_ft", $"height" *  0.032808).explain()

// COMMAND ----------

// Odpowiedz na pytanie jakie jest najpopularniesze imię?
namesDf.withColumn("name", split($"name", " "))
        .select($"name".getItem(0))
        .groupBy("name[0]")
        .count()
        .agg(first("name[0]"), max("count"))
        .explain()

//Ales 

// COMMAND ----------

// Dodaj kolumnę i policz wiek aktorów
import org.apache.spark.sql.types.IntegerType
val new_df = namesDf.filter($"date_of_death".isNull)
new_df.withColumn("age_now", (months_between(current_date(),to_date($"date_of_birth","dd.MM.yyyy"))/12).cast(IntegerType)).explain()

// COMMAND ----------

// Usuń kolumny (bio, death_details)
namesDf.drop("bio","death_details").explain()

// COMMAND ----------

// Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
namesDf.withColumnRenamed("imdb_name_id","imdbNameId")
       .withColumnRenamed("birth_name","birthName")
       .withColumnRenamed("birth_details","birthDetails")
       .withColumnRenamed("date_of_birth","dateOfBirth")
       .withColumnRenamed("place_of_birth","placeOfBirth")
       .withColumnRenamed("death_details","deathDetails")
       .withColumnRenamed("date_of_death","dateOfDeath")
       .withColumnRenamed("place_of_death","placeOfDeath")
       .withColumnRenamed("reason_of_death","reasonOfDeath")
       .withColumnRenamed("spouses_string","spousesString")
       .withColumnRenamed("spouses_with_children","spousesWithChildren").explain()

// COMMAND ----------

//Posortuj dataframe po imieniu rosnąco
namesDf.sort("name").explain()

// COMMAND ----------

// MAGIC %md
// MAGIC Movies.csv

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/movies.csv"
val moviesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(moviesDf)

// COMMAND ----------

//Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
moviesDf.withColumn("time", unix_timestamp(current_timestamp())).explain()

// COMMAND ----------

//Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
import java.time.Year 
moviesDf.withColumn("years_passed", (col("year").cast("int") - Year.now.getValue) * -1).explain()

// COMMAND ----------

//Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
moviesDf.withColumn("budget_double", split($"budget", " "))
        .select($"budget_double".getItem(1))
        .explain()


// COMMAND ----------

//Usuń wiersze z dataframe gdzie wartości są null
moviesDf.na.drop().explain()

// COMMAND ----------

// MAGIC %md
// MAGIC Ratings.csv

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/ratings.csv"
val ratingsDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(ratingsDf)

// COMMAND ----------

//Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
ratingsDf.withColumn("time", unix_timestamp(current_timestamp())).explain()

// COMMAND ----------

//Dla każdego z poniższych wyliczeń nie bierz pod uwagę nulls

// COMMAND ----------

// Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)


// COMMAND ----------

// Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote

// COMMAND ----------

// Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
ratingsDf.agg(sum($"males_allages_avg_vote" * $"males_allages_votes") as "sum_avg_m", sum($"males_allages_votes") as "sum_weight_m", sum($"females_allages_avg_vote" * $"females_allages_votes") as "sum_avg_f",sum($"females_allages_votes") as "sum_weight_f").select($"sum_avg_m"/$"sum_weight_m" as "men",$"sum_avg_f"/$"sum_weight_f" as "female").explain()

//female

// COMMAND ----------

// Dla jednej z kolumn zmień typ danych do long
ratingsDf.withColumn("votes_10",$"votes_10".cast("long")).explain()

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 2

// COMMAND ----------

// MAGIC %md
// MAGIC Spark UI
// MAGIC * Jobs - wyświetla informacje o wszystkich zadaniach w klastrze. 
// MAGIC     * User - Użytkownik
// MAGIC     * Total Uptime - Całkowity czas działania
// MAGIC     * Scheduling Mode - Kolejność wykonywania zadań (FIFO lub FAIR)
// MAGIC     * Completed Jobs - Liczba zadań w poszczególnych statusach (Aktywne, Ukończone lub Nieudane)
// MAGIC     * Event Timeline - Oś czasu
// MAGIC     * Completed jobs - Podsumowanie wykonanych zadań (Id zadania, opis, data złożenia, czas wykonania, etapy, pasek postępu zadań )
// MAGIC * Stages - pokazuje aktualny stan wszystkich etapów wszystkich zadań
// MAGIC     * Stages for All Jobs - Podsumowanie zawierające liczbę wszystkich etapów według statusu
// MAGIC     * Fair Scheduler Pools - Podsumowanie zgrupowany zadań w pule
// MAGIC     * Completed stages - Podsumowanie wykonanych etapoów (Id etapu, pula, opis, data złożenia, czas wykonania, pasek postępu zadań )
// MAGIC * Storage - wyświetla informacje o rozmiarach wczytanych/wpisanych danych
// MAGIC * Environment - pokazuje informacje o  zmiennych środowiskowych i konfiguracyjnych
// MAGIC     * Runtime Information - informacje o Javie i Sparku
// MAGIC     * Spark Properties - właściwości Sparka (zasoby, właściwości Hadoopa, właściwości systemu, ścieżki)
// MAGIC * Executors - wyświetla informacje na temat executorów utworzonych dla aplikacji (np. pamięć, zajęta pamięc, ilość róznych typów zadań)
// MAGIC * SQL - pokazuje informacje o wykonanych zapytaniach SQL (Id, opis, czas złożenia, czas wykonania, Id zadań)
// MAGIC * JDBC/ODBC Server - wyświetla informacje o sesjach i przesłanych operacjach SQL
// MAGIC     * Started at  - Data rozpczęcia
// MAGIC     * Time since start - Czas działania
// MAGIC     * Session Statistics - Statystyki sesji
// MAGIC     *  SQL Statistics - Statystyki SQL
// MAGIC * Structured Streaming - pokazuje statystyki dla uruchomionych i zakończonych zapytań

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 3

// COMMAND ----------

moviesDf.groupBy("language").count().show()

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 4

// COMMAND ----------

val hostname = "bidevtestserver.database.windows.net"
val port = 1433
val database = "testdb"

val df = spark.read.format("jdbc")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("url", s"jdbc:sqlserver://${hostname}:${port};database=${database}")
  .option("dbtable","(SELECT * FROM information_schema.tables) as tmp")
  .option("user", "sqladmin")
  .option("password", "$3bFHs56&o123$")
  .load()

display(df)
