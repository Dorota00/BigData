// Databricks notebook source
// MAGIC %md
// MAGIC Zadanie 1

// COMMAND ----------

// MAGIC %md
// MAGIC Modelowanie danych to proces tworzenia schematów i podsumowania informacji zebranych w hurtowni danych.

// COMMAND ----------

// MAGIC %md
// MAGIC Cardinality określa występowanie danych. High cardinality oznacza, że dana wartość występuje rzadko, a normal cardinality informuje, że wartość występuje rzadko, ale więcej niż 1 raz. Low cardinality oznacza, że wartość pojawia się często.

// COMMAND ----------

// MAGIC %md
// MAGIC Normalizacja polega na uporządkowaniu danych, usuwając jej nadmiarowość, zmieniając zapis, tak aby były łatwiejsze w analizie. Normalizacja przyśpiesza cały proces analizy.
// MAGIC 
// MAGIC Denormalizacja dodaje duplikaty danych w bazie, tak, aby uniknąć czasochłonnych łączeń tabel i przyśpieszyć działanie zapytań.

// COMMAND ----------

// MAGIC %md
// MAGIC Datamart jesto zazwyczaj pewna część hurtowni danych, która intersuje dany dział, przez co znacznie przyśpiesza analizę danych konkretnych obszarów.

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 2

// COMMAND ----------

// MAGIC %md
// MAGIC Online Analytical Processing (OLAP) strukturą danych, która pozwala pokonać ograniczenia relacyjnych baz danych, umożliwiając szybką analizę danych. Pozwala na analizę danych z wielu baz danych w tym samym czasie i ekstrakcję interesującej nas części. Bazy danych OLAP są podzielone na kostki, przez co przypomina wielowymiarowy arkusz kalkulacyjny. Znacząco ułatwia tworzenie raportów.
