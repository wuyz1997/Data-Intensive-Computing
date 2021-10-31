// Databricks notebook source
// MAGIC %md # ID2221 Lab1 Apache Spark

// COMMAND ----------

// MAGIC %md Yizhan Wu (yizhanw@kth.se), Yage Hao (yage@kth.se)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 1 Spark

// COMMAND ----------

// val spark= SparkSession.builder().getOrCreate()
import org.apache.spark.rdd.RDD
val pagecounts = sc.textFile("/FileStore/tables/pagecounts_20160101_000000_parsed.out")
print(pagecounts)
pagecounts.count()

// COMMAND ----------

// MAGIC %md ### Part1: convert the pagecounts from RDD[String] into RDD[Log]:

// COMMAND ----------

// MAGIC %md #### 1 Create a case class called Log using the four ﬁeld names of the dataset.

// COMMAND ----------

case class Log(code: String, title:String, hits:Int, size:Long)

// COMMAND ----------

// MAGIC %md #### 2 Create a function that takes a string, split it by white space and converts it into a log object.

// COMMAND ----------

def Log_obj(s: String) : Log ={
    val splits = s.split(" ")
    return Log(splits(0).toString,splits(1).toString,splits(2).toInt,splits(3).toLong)
}

// COMMAND ----------

// MAGIC %md #### 3 Create a function that takes an RDD[String] and returns an RDD[Log].

// COMMAND ----------

def RDD_Log(s: RDD[String]) : RDD[Log] = {
    return s.map(x=>Log_obj(x))
}

// COMMAND ----------

val rdd_page = RDD_Log(pagecounts)

// COMMAND ----------

// MAGIC %md ### Part2

// COMMAND ----------

// MAGIC %md #### 1. Retrieve the ﬁrst 15 records and print out the result.

// COMMAND ----------

for (x <- rdd_page.take(15)) {
   println(x)
}

// COMMAND ----------

// MAGIC %md #### 2. Determine the number of records the dataset has in total.

// COMMAND ----------

rdd_page.count()

// COMMAND ----------

// MAGIC %md #### 3. Compute the min, max, and average page size.

// COMMAND ----------

val page_size = rdd_page.map(x=>x.size)
val page_size_min = page_size.min()
val page_size_max = page_size.max()
val page_size_average = page_size.mean()

// COMMAND ----------

// MAGIC %md #### 4. Determine the record(s) with the largest page size. If multiple records have the same size, list all of them.

// COMMAND ----------

rdd_page.filter(_.size == page_size_max).collect()(0)

// COMMAND ----------

// MAGIC %md #### 5. Determine the record with the largest page size again. But now, pick the most popular.

// COMMAND ----------

rdd_page.filter(_.size == page_size_max).collect().maxBy(x=>x.hits)

// COMMAND ----------

// MAGIC %md #### 6. Determine the record(s) with the largest page title. If multiple titles have the same length, list all of them.

// COMMAND ----------

val page_title_len = rdd_page.map(x=>x.title.length)
val page_title_len_max = page_title_len.max()

rdd_page.filter(_.title.length() == page_title_len_max).collect().foreach(print)

// COMMAND ----------

// MAGIC %md #### 7. Use the results of Question 3, and create a new RDD with the records that have greater page size than the average.

// COMMAND ----------

def GreaterThanAvg(i: Log): Boolean ={
    if (i.size <= page_size_average){
        return false
    }
    else{
        return true
    }
}
val rdd_page_greater=rdd_page.filter(GreaterThanAvg)

// COMMAND ----------

print("number of pages with size greater than average: " + rdd_page_greater.count())

print("\nfirst 10 cases for example:")
rdd_page_greater.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md #### 8. Compute the total number of pageviews for each project (as the schema shows, the ﬁrst ﬁeld of each record contains the project code).

// COMMAND ----------

val pageview = rdd_page.map(x=>(x.code, x.hits)).reduceByKey(_ + _)

print("\nfirst 10 cases for example: \n")
pageview.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md #### 9. Report the 10 most popular pageviews of all projects, sorted by the total number of hits.

// COMMAND ----------

val top10 = pageview.map(x=>(x._2, x._1)).sortByKey().top(10)
top10.map(x=>(x._2,x._1)).foreach(println)

// COMMAND ----------

// MAGIC %md #### 10. Determine the number of page titles that start with the article “The”. How many of those page titles are not part of the English project (Pages that are part of the English project have “en” as the ﬁrst ﬁeld)?

// COMMAND ----------

val pagesEN = rdd_page.filter(x => (!x.code.startsWith("en") && x.title.startsWith("The"))).count()

// COMMAND ----------

// MAGIC %md #### 11. Determine the percentage of pages that have only received a single page view in this one hour of log data.

// COMMAND ----------

val onepageview = rdd_page.filter(x => (x.hits == 1)).count().toFloat
val totalpage = rdd_page.count().toFloat
val percentage = onepageview/totalpage

// COMMAND ----------

// MAGIC %md #### 12. Determine the number of unique terms appearing in the page titles. Note that in page titles, terms are delimited by “ ” instead of a white space. You can use any number of normalization steps (e.g., lowercasing, removal of non-alphanumeric characters).

// COMMAND ----------

val terms = rdd_page.map(x=>x.title.toLowerCase).flatMap(x=>x.split("_")).map(_.replaceAll("[^A-Za-z0-9]", "")).filter(x => x.length >0)

// COMMAND ----------

val distinct_terms = terms.distinct().count()

// COMMAND ----------

// MAGIC %md #### 13. Determine the most frequently occurring page title term in this dataset.

// COMMAND ----------

val res = terms.map(x=>(x,1)).reduceByKey(_ + _).map(x=>(x._2, x._1)).sortByKey().top(1)

// COMMAND ----------

res.filter(x=>x._2 == "of").take(10)

// COMMAND ----------

// MAGIC %md ## Task 2 -Spark SQL

// COMMAND ----------

val df = rdd_page.toDF("code","title","hits","size")

// COMMAND ----------

df.show()

// COMMAND ----------

// MAGIC %md #### 3. Compute the min, max, and average page size.

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
//import SparkSession._

df.select(max("size"), min("size"), avg("size")).show()

// COMMAND ----------

// MAGIC %md #### 5. Determine the record with the largest page size again. But now, pick the most popular.

// COMMAND ----------

val sizemax = df.agg(max(col("size"))).collect()(0)(0)
val sizemin = df.agg(min(col("size"))).collect()(0)(0)
val sizemean = df.agg(mean(col("size"))).collect()(0)(0)

// COMMAND ----------

val df_sizemax = df.filter(df("size") === sizemax)
df_sizemax.show()

// COMMAND ----------

val ordered = Window.orderBy(df_sizemax.col("hits").desc)

// COMMAND ----------

val ranked = df_sizemax.withColumn("rank", dense_rank.over(ordered))
val maxDf = ranked.filter("rank == 1")
maxDf.show()

// COMMAND ----------

// MAGIC %md #### 7. Use the results of Question 3, and create a new RDD with the records that have greater page size than the average.

// COMMAND ----------

val df_greater=df.filter(df("size") > sizemean)
df_greater.show(10)

// COMMAND ----------

// MAGIC %md #### 12. Determine the number of unique terms appearing in the page titles. Note that in page titles, terms are delimited by “\_” instead of a white space. You can use any number of normalization steps (e.g., lowercasing, removal of non-alphanumeric characters).

// COMMAND ----------

val df1 = df.select(explode(split(lower(col("title")),"_")))

// COMMAND ----------

val df_term = df1.withColumn("col",regexp_replace($"col", "[^A-Za-z0-9]", "")).filter(length($"col")>0)

// COMMAND ----------

val num_term = df_term.distinct.count()

// COMMAND ----------

// MAGIC %md #### 13.Determine the most frequently occurring page title term in this dataset.

// COMMAND ----------

val df_term_new = df_term.groupBy("col").count()
df_term_new.show(10)

// COMMAND ----------

df_term_new.orderBy($"count".desc).first

// COMMAND ----------


