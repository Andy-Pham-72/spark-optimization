# Spark Query Plan Optimization

Suppose we want to compose query in which we get for each question also the number of answers to this question for each month. See the query below which does that in a suboptimal way and try to rewrite it to achieve a more optimal plan.



```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month, broadcast

import os
import time

spark = SparkSession.builder.appName('Optimize I').getOrCreate()

base_path = os.getcwd()

project_path = ('/').join(base_path.split('/')[0:-3]) 

answers_input_path = os.path.join(project_path, '/spark-optimization/data/answers')

questions_input_path = os.path.join(project_path, '/spark-optimization/data/questions')

answersDF = spark.read.option('path', answers_input_path).load()

questionsDF = spark.read.option('path', questions_input_path).load()

```


```python
# check the answersDF
answersDF.show()
```

    +-----------+---------+--------------------+--------+-------+-----+
    |question_id|answer_id|       creation_date|comments|user_id|score|
    +-----------+---------+--------------------+--------+-------+-----+
    |     226592|   226595|2015-12-29 18:46:...|       3|  82798|    2|
    |     388057|   388062|2018-02-22 13:52:...|       8|    520|   21|
    |     293286|   293305|2016-11-17 16:35:...|       0|  47472|    2|
    |     442499|   442503|2018-11-22 01:34:...|       0| 137289|    0|
    |     293009|   293031|2016-11-16 08:36:...|       0|  83721|    0|
    |     395532|   395537|2018-03-25 01:51:...|       0|   1325|    0|
    |     329826|   329843|2017-04-29 11:42:...|       4|    520|    1|
    |     294710|   295061|2016-11-26 20:29:...|       2| 114696|    2|
    |     291910|   291917|2016-11-10 05:56:...|       0| 114696|    2|
    |     372382|   372394|2017-12-03 21:17:...|       0| 172328|    0|
    |     178387|   178394|2015-04-25 13:31:...|       6|  62726|    0|
    |     393947|   393948|2018-03-17 18:22:...|       0| 165299|    9|
    |     432001|   432696|2018-10-05 04:47:...|       1| 102218|    0|
    |     322740|   322746|2017-03-31 14:10:...|       0|    392|    0|
    |     397003|   397008|2018-04-01 07:31:...|       1| 189394|    6|
    |     223572|   223628|2015-12-12 00:40:...|       0|  94772|   -1|
    |     220328|   220331|2015-11-24 10:57:...|       3|  92883|    1|
    |     176400|   176491|2015-04-16 09:13:...|       0|  40330|    0|
    |     265167|   265179|2016-06-28 07:58:...|       0|  46790|    0|
    |     309103|   309105|2017-02-01 12:00:...|       2|  89597|    2|
    +-----------+---------+--------------------+--------+-------+-----+
    only showing top 20 rows
    



```python
# check the questionsDF
questionsDF.show()
```

    +-----------+--------------------+--------------------+--------------------+------------------+--------+-------+-----+
    |question_id|                tags|       creation_date|               title|accepted_answer_id|comments|user_id|views|
    +-----------+--------------------+--------------------+--------------------+------------------+--------+-------+-----+
    |     382738|[optics, waves, f...|2018-01-28 02:22:...|What is the pseud...|            382772|       0|  76347|   32|
    |     370717|[field-theory, de...|2017-11-25 04:09:...|What is the defin...|              null|       1|  75085|   82|
    |     339944|[general-relativi...|2017-06-17 16:32:...|Could gravitation...|              null|      13| 116137|  333|
    |     233852|[homework-and-exe...|2016-02-04 16:19:...|When does travell...|              null|       9|  95831|  185|
    |     294165|[quantum-mechanic...|2016-11-22 06:39:...|Time-dependent qu...|              null|       1| 118807|   56|
    |     173819|[homework-and-exe...|2015-04-02 11:56:...|Finding Magnetic ...|              null|       5|  76767| 3709|
    |     265198|    [thermodynamics]|2016-06-28 10:56:...|Physical meaning ...|              null|       2|  65035| 1211|
    |     175015|[quantum-mechanic...|2015-04-08 21:24:...|Understanding a m...|              null|       1|  76155|  326|
    |     413973|[quantum-mechanic...|2018-06-27 09:29:...|Incorporate spino...|              null|       3| 167682|   81|
    |     303670|[quantum-field-th...|2017-01-08 01:05:...|A Wilson line pro...|              null|       0| 101968|  184|
    |     317368|[general-relativi...|2017-03-08 14:53:...|Shouldn't Torsion...|              null|       0|  20427|  305|
    |     369982|[quantum-mechanic...|2017-11-20 22:11:...|Incompressible in...|              null|       4| 124864|   83|
    |     239745|[quantum-mechanic...|2016-02-25 03:51:...|Is this correct? ...|            239773|       2|  89821|   78|
    |     412294|[quantum-mechanic...|2018-06-17 20:46:...|Is electron/photo...|              null|       0|    605|   61|
    |     437521|[thermodynamics, ...|2018-10-29 02:49:...|Distance Dependen...|              null|       2| 211152|   19|
    |     289701|[quantum-field-th...|2016-10-29 23:56:...|Generalize QFT wi...|              null|       4|  31922|   49|
    |     239505|[definition, stab...|2016-02-24 05:51:...|conditions for so...|              null|       3| 102021|  121|
    |     300744|[electromagnetism...|2016-12-24 13:14:...|Maxwell equations...|            300749|       0| 112190|  171|
    |     217315|[nuclear-physics,...|2015-11-08 04:13:...|Is the direction ...|              null|       1|  60150| 1749|
    |     334778|[cosmology, cosmo...|2017-05-22 09:58:...|Why are fluctatio...|            334791|       3| 109312|  110|
    +-----------+--------------------+--------------------+--------------------+------------------+--------+-------+-----+
    only showing top 20 rows
    


# Original query


```python
'''
Answers aggregation

Here we : get number of answers per question per month
'''
# initialize start_time

start_time = time.time()

answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))

resultDF = questionsDF.join(answers_month, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')

resultDF.orderBy('question_id', 'month').show()

print("Processing time: %s seconds" % (time.time() - start_time))
print("\n")
resultDF.explain()
```

    +-----------+--------------------+--------------------+-----+---+
    |question_id|       creation_date|               title|month|cnt|
    +-----------+--------------------+--------------------+-----+---+
    |     155989|2014-12-31 20:59:...|Frost bubble form...|    1|  1|
    |     155989|2014-12-31 20:59:...|Frost bubble form...|    2|  1|
    |     155990|2014-12-31 21:51:...|The abstract spac...|    1|  2|
    |     155992|2014-12-31 22:44:...|centrifugal force...|    1|  1|
    |     155993|2014-12-31 22:56:...|How can I estimat...|    1|  1|
    |     155995|2015-01-01 00:16:...|Why should a solu...|    1|  3|
    |     155996|2015-01-01 01:06:...|Why do we assume ...|    1|  2|
    |     155996|2015-01-01 01:06:...|Why do we assume ...|    2|  1|
    |     155996|2015-01-01 01:06:...|Why do we assume ...|   11|  1|
    |     155997|2015-01-01 01:26:...|Why do square sha...|    1|  3|
    |     155999|2015-01-01 02:01:...|Diagonalizability...|    1|  1|
    |     156008|2015-01-01 03:48:...|Capturing a light...|    1|  2|
    |     156008|2015-01-01 03:48:...|Capturing a light...|   11|  1|
    |     156016|2015-01-01 05:31:...|The interference ...|    1|  1|
    |     156020|2015-01-01 06:19:...|What is going on ...|    1|  1|
    |     156021|2015-01-01 06:21:...|How to calculate ...|    2|  1|
    |     156022|2015-01-01 06:55:...|Advice on Major S...|    1|  1|
    |     156025|2015-01-01 07:32:...|Deriving the Cano...|    1|  1|
    |     156026|2015-01-01 07:49:...|Does Bell's inequ...|    1|  3|
    |     156027|2015-01-01 07:49:...|Deriving X atom f...|    1|  1|
    +-----------+--------------------+--------------------+-----+---+
    only showing top 20 rows
    
    Processing time: 1.6626389026641846 seconds
    
    
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [question_id#12L, creation_date#14, title#15, month#94, cnt#110L]
       +- BroadcastHashJoin [question_id#12L], [question_id#0L], Inner, BuildRight, false
          :- Filter isnotnull(question_id#12L)
          :  +- FileScan parquet [question_id#12L,creation_date#14,title#15] Batched: true, DataFilters: [isnotnull(question_id#12L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Volumes/Moon/SpringBoard/spark-optimization/data/questions], PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp,title:string>
          +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]),false), [id=#215]
             +- HashAggregate(keys=[question_id#0L, month#94], functions=[count(1)])
                +- Exchange hashpartitioning(question_id#0L, month#94, 200), ENSURE_REQUIREMENTS, [id=#212]
                   +- HashAggregate(keys=[question_id#0L, month#94], functions=[partial_count(1)])
                      +- Project [question_id#0L, month(cast(creation_date#2 as date)) AS month#94]
                         +- Filter isnotnull(question_id#0L)
                            +- FileScan parquet [question_id#0L,creation_date#2] Batched: true, DataFilters: [isnotnull(question_id#0L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Volumes/Moon/SpringBoard/spark-optimization/data/answers], PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp>
    
    


**Task**:

See the query plan of the previous result and rewrite the query to optimize it


# Solution 1
- In a distributed environment, having proper data distribution becomes a key tool for boosting performance. `repartition()` allows us to control the data distribution on the Spark cluster. Therefore, we can reduce the number of shuffles than the orignal query.
- We will repartition the answersDF dataframe by `creation_date` column as new column named `month`


```python
# set the adaptive = true
spark.conf.set("spark.sql.adaptive.enabled", "true")

# initialize start_time
start_time = time.time()

answers_month = answersDF.withColumn('month', month('creation_date'))

# Repartition answers_month by month
answers_month = answers_month.repartition(col("month"))

answers_month = answers_month.groupBy('question_id', 'month').agg(count('*').alias('cnt'))

resultDF_1 = questionsDF.join(answers_month, "question_id").select('question_id', 'creation_date', 'title', 'month', 'cnt')
resultDF_1.orderBy('question_id', 'month').show()

print("Processing time: %s seconds" % (time.time() - start_time))
print("\n")
resultDF_1.explain()
```

    +-----------+--------------------+--------------------+-----+---+
    |question_id|       creation_date|               title|month|cnt|
    +-----------+--------------------+--------------------+-----+---+
    |     155989|2014-12-31 20:59:...|Frost bubble form...|    1|  1|
    |     155989|2014-12-31 20:59:...|Frost bubble form...|    2|  1|
    |     155990|2014-12-31 21:51:...|The abstract spac...|    1|  2|
    |     155992|2014-12-31 22:44:...|centrifugal force...|    1|  1|
    |     155993|2014-12-31 22:56:...|How can I estimat...|    1|  1|
    |     155995|2015-01-01 00:16:...|Why should a solu...|    1|  3|
    |     155996|2015-01-01 01:06:...|Why do we assume ...|    1|  2|
    |     155996|2015-01-01 01:06:...|Why do we assume ...|    2|  1|
    |     155996|2015-01-01 01:06:...|Why do we assume ...|   11|  1|
    |     155997|2015-01-01 01:26:...|Why do square sha...|    1|  3|
    |     155999|2015-01-01 02:01:...|Diagonalizability...|    1|  1|
    |     156008|2015-01-01 03:48:...|Capturing a light...|    1|  2|
    |     156008|2015-01-01 03:48:...|Capturing a light...|   11|  1|
    |     156016|2015-01-01 05:31:...|The interference ...|    1|  1|
    |     156020|2015-01-01 06:19:...|What is going on ...|    1|  1|
    |     156021|2015-01-01 06:21:...|How to calculate ...|    2|  1|
    |     156022|2015-01-01 06:55:...|Advice on Major S...|    1|  1|
    |     156025|2015-01-01 07:32:...|Deriving the Cano...|    1|  1|
    |     156026|2015-01-01 07:49:...|Does Bell's inequ...|    1|  3|
    |     156027|2015-01-01 07:49:...|Deriving X atom f...|    1|  1|
    +-----------+--------------------+--------------------+-----+---+
    only showing top 20 rows
    
    Processing time: 0.7143349647521973 seconds
    
    
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [question_id#12L, creation_date#14, title#15, month#152, cnt#168L]
       +- BroadcastHashJoin [question_id#12L], [question_id#0L], Inner, BuildRight, false
          :- Filter isnotnull(question_id#12L)
          :  +- FileScan parquet [question_id#12L,creation_date#14,title#15] Batched: true, DataFilters: [isnotnull(question_id#12L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Volumes/Moon/SpringBoard/spark-optimization/data/questions], PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp,title:string>
          +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]),false), [id=#416]
             +- HashAggregate(keys=[question_id#0L, month#152], functions=[count(1)])
                +- HashAggregate(keys=[question_id#0L, month#152], functions=[partial_count(1)])
                   +- Exchange hashpartitioning(month#152, 200), REPARTITION_BY_COL, [id=#408]
                      +- Project [question_id#0L, month(cast(creation_date#2 as date)) AS month#152]
                         +- Filter isnotnull(question_id#0L)
                            +- FileScan parquet [question_id#0L,creation_date#2] Batched: true, DataFilters: [isnotnull(question_id#0L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Volumes/Moon/SpringBoard/spark-optimization/data/answers], PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp>
    
    


# Solution 2
- Using `broadcast()` function to optimize the execution plan. Since Spark splits up data on different nodes in a cluster so multiple computers can process data in parallel. traditional joins are less optimal with Spark because the data is split.
- `Broadcast joins` are easier to run on a cluster. Spark can `broadcast` a small DataFrame by sending all the data in the small DataFrame to all nodes in the cluster. After the small DataFrame is broadcasted, Spark can perform a join without shuffling more data in the larger DataFrame.


```python
# set the adaptive = true
spark.conf.set("spark.sql.adaptive.enabled", "true")

start_time = time.time()

answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))

# using broadcast() with "answers_month"
resultDF_2 = questionsDF.join(broadcast(answers_month), "question_id").select('question_id', 'creation_date', 'title', 'month', 'cnt')

resultDF_2.orderBy('question_id', 'month').show()

print("Processing time: %s seconds" % (time.time() - start_time))
print("\n")
resultDF_2.explain()
```

    +-----------+--------------------+--------------------+-----+---+
    |question_id|       creation_date|               title|month|cnt|
    +-----------+--------------------+--------------------+-----+---+
    |     155989|2014-12-31 20:59:...|Frost bubble form...|    1|  1|
    |     155989|2014-12-31 20:59:...|Frost bubble form...|    2|  1|
    |     155990|2014-12-31 21:51:...|The abstract spac...|    1|  2|
    |     155992|2014-12-31 22:44:...|centrifugal force...|    1|  1|
    |     155993|2014-12-31 22:56:...|How can I estimat...|    1|  1|
    |     155995|2015-01-01 00:16:...|Why should a solu...|    1|  3|
    |     155996|2015-01-01 01:06:...|Why do we assume ...|    1|  2|
    |     155996|2015-01-01 01:06:...|Why do we assume ...|    2|  1|
    |     155996|2015-01-01 01:06:...|Why do we assume ...|   11|  1|
    |     155997|2015-01-01 01:26:...|Why do square sha...|    1|  3|
    |     155999|2015-01-01 02:01:...|Diagonalizability...|    1|  1|
    |     156008|2015-01-01 03:48:...|Capturing a light...|    1|  2|
    |     156008|2015-01-01 03:48:...|Capturing a light...|   11|  1|
    |     156016|2015-01-01 05:31:...|The interference ...|    1|  1|
    |     156020|2015-01-01 06:19:...|What is going on ...|    1|  1|
    |     156021|2015-01-01 06:21:...|How to calculate ...|    2|  1|
    |     156022|2015-01-01 06:55:...|Advice on Major S...|    1|  1|
    |     156025|2015-01-01 07:32:...|Deriving the Cano...|    1|  1|
    |     156026|2015-01-01 07:49:...|Does Bell's inequ...|    1|  3|
    |     156027|2015-01-01 07:49:...|Deriving X atom f...|    1|  1|
    +-----------+--------------------+--------------------+-----+---+
    only showing top 20 rows
    
    Processing time: 0.6551077365875244 seconds
    
    
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [question_id#12L, creation_date#14, title#15, month#210, cnt#226L]
       +- BroadcastHashJoin [question_id#12L], [question_id#0L], Inner, BuildRight, false
          :- Filter isnotnull(question_id#12L)
          :  +- FileScan parquet [question_id#12L,creation_date#14,title#15] Batched: true, DataFilters: [isnotnull(question_id#12L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Volumes/Moon/SpringBoard/spark-optimization/data/questions], PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp,title:string>
          +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]),false), [id=#611]
             +- HashAggregate(keys=[question_id#0L, month#210], functions=[count(1)])
                +- Exchange hashpartitioning(question_id#0L, month#210, 200), ENSURE_REQUIREMENTS, [id=#608]
                   +- HashAggregate(keys=[question_id#0L, month#210], functions=[partial_count(1)])
                      +- Project [question_id#0L, month(cast(creation_date#2 as date)) AS month#210]
                         +- Filter isnotnull(question_id#0L)
                            +- FileScan parquet [question_id#0L,creation_date#2] Batched: true, DataFilters: [isnotnull(question_id#0L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Volumes/Moon/SpringBoard/spark-optimization/data/answers], PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp>
    
    


# Solution 3
- We are combining `repartition` and `broadcast` into the query to optimize the performance!


```python
# set the adaptive = true
spark.conf.set("spark.sql.adaptive.enabled", "true")

# initialize start_time
start_time = time.time()

answers_month = answersDF.withColumn('month', month('creation_date'))

# Repartition answers_month by month
answers_month = answers_month.repartition(col("month"))

answers_month = answers_month.groupBy('question_id', 'month').agg(count('*').alias('cnt'))

resultDF_3 = questionsDF.join(broadcast(answers_month), "question_id").select('question_id', 'creation_date', 'title', 'month', 'cnt')

resultDF_3.orderBy('question_id', 'month').show()

print("Processing time: %s seconds" % (time.time() - start_time))
print("\n")
resultDF_3.explain()
```

    +-----------+--------------------+--------------------+-----+---+
    |question_id|       creation_date|               title|month|cnt|
    +-----------+--------------------+--------------------+-----+---+
    |     155989|2014-12-31 20:59:...|Frost bubble form...|    1|  1|
    |     155989|2014-12-31 20:59:...|Frost bubble form...|    2|  1|
    |     155990|2014-12-31 21:51:...|The abstract spac...|    1|  2|
    |     155992|2014-12-31 22:44:...|centrifugal force...|    1|  1|
    |     155993|2014-12-31 22:56:...|How can I estimat...|    1|  1|
    |     155995|2015-01-01 00:16:...|Why should a solu...|    1|  3|
    |     155996|2015-01-01 01:06:...|Why do we assume ...|    1|  2|
    |     155996|2015-01-01 01:06:...|Why do we assume ...|    2|  1|
    |     155996|2015-01-01 01:06:...|Why do we assume ...|   11|  1|
    |     155997|2015-01-01 01:26:...|Why do square sha...|    1|  3|
    |     155999|2015-01-01 02:01:...|Diagonalizability...|    1|  1|
    |     156008|2015-01-01 03:48:...|Capturing a light...|    1|  2|
    |     156008|2015-01-01 03:48:...|Capturing a light...|   11|  1|
    |     156016|2015-01-01 05:31:...|The interference ...|    1|  1|
    |     156020|2015-01-01 06:19:...|What is going on ...|    1|  1|
    |     156021|2015-01-01 06:21:...|How to calculate ...|    2|  1|
    |     156022|2015-01-01 06:55:...|Advice on Major S...|    1|  1|
    |     156025|2015-01-01 07:32:...|Deriving the Cano...|    1|  1|
    |     156026|2015-01-01 07:49:...|Does Bell's inequ...|    1|  3|
    |     156027|2015-01-01 07:49:...|Deriving X atom f...|    1|  1|
    +-----------+--------------------+--------------------+-----+---+
    only showing top 20 rows
    
    Processing time: 0.5135302543640137 seconds
    
    
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [question_id#12L, creation_date#14, title#15, month#268, cnt#284L]
       +- BroadcastHashJoin [question_id#12L], [question_id#0L], Inner, BuildRight, false
          :- Filter isnotnull(question_id#12L)
          :  +- FileScan parquet [question_id#12L,creation_date#14,title#15] Batched: true, DataFilters: [isnotnull(question_id#12L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Volumes/Moon/SpringBoard/spark-optimization/data/questions], PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp,title:string>
          +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]),false), [id=#816]
             +- HashAggregate(keys=[question_id#0L, month#268], functions=[count(1)])
                +- HashAggregate(keys=[question_id#0L, month#268], functions=[partial_count(1)])
                   +- Exchange hashpartitioning(month#268, 200), REPARTITION_BY_COL, [id=#808]
                      +- Project [question_id#0L, month(cast(creation_date#2 as date)) AS month#268]
                         +- Filter isnotnull(question_id#0L)
                            +- FileScan parquet [question_id#0L,creation_date#2] Batched: true, DataFilters: [isnotnull(question_id#0L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Volumes/Moon/SpringBoard/spark-optimization/data/answers], PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp>
    
    


# Solution 4
- If we look at the questionsDF DataFrame, we will see the column `accepted_answer_id` so let assume that we only take the accepted answers in the consideration and filter out all the answers that have `null` value. 
- We also repartition the answersDF with column `month` as well as using `broadcast()`.


```python
# set the adaptive = true
spark.conf.set("spark.sql.adaptive.enabled", "true")

# initialize start_time
start_time = time.time()

answersDF = spark.read.option('path', answers_input_path).load()

# filter out all the answers that have null value
questionsDF = spark.read.option('path', questions_input_path).load().filter(col("accepted_answer_id").isNotNull())

start_time = time.time()

answers_month = answersDF.withColumn('month', month('creation_date')).repartition(col("month")).groupBy('question_id', 'month').agg(count('*').alias('cnt'))

resultDF_4 = answers_month.join(broadcast(questionsDF), "question_id").select('question_id', 'creation_date', 'title', 'month', 'cnt')

resultDF_4.orderBy('question_id', 'month').show()

print("Processing time: %s seconds" % (time.time() - start_time))
print("\n")
resultDF_4.explain()
```

    +-----------+--------------------+--------------------+-----+---+
    |question_id|       creation_date|               title|month|cnt|
    +-----------+--------------------+--------------------+-----+---+
    |     155989|2014-12-31 20:59:...|Frost bubble form...|    1|  1|
    |     155989|2014-12-31 20:59:...|Frost bubble form...|    2|  1|
    |     155990|2014-12-31 21:51:...|The abstract spac...|    1|  2|
    |     155995|2015-01-01 00:16:...|Why should a solu...|    1|  3|
    |     155996|2015-01-01 01:06:...|Why do we assume ...|    1|  2|
    |     155996|2015-01-01 01:06:...|Why do we assume ...|    2|  1|
    |     155996|2015-01-01 01:06:...|Why do we assume ...|   11|  1|
    |     155997|2015-01-01 01:26:...|Why do square sha...|    1|  3|
    |     156008|2015-01-01 03:48:...|Capturing a light...|    1|  2|
    |     156008|2015-01-01 03:48:...|Capturing a light...|   11|  1|
    |     156022|2015-01-01 06:55:...|Advice on Major S...|    1|  1|
    |     156025|2015-01-01 07:32:...|Deriving the Cano...|    1|  1|
    |     156027|2015-01-01 07:49:...|Deriving X atom f...|    1|  1|
    |     156050|2015-01-01 10:40:...|Position of Neutr...|    1|  2|
    |     156051|2015-01-01 10:43:...|Relation between ...|    1|  1|
    |     156055|2015-01-01 10:59:...|Meaning of revers...|    1|  1|
    |     156055|2015-01-01 10:59:...|Meaning of revers...|    4|  1|
    |     156055|2015-01-01 10:59:...|Meaning of revers...|   10|  1|
    |     156058|2015-01-01 11:31:...|are protons in pr...|    1|  1|
    |     156093|2015-01-01 17:09:...|Quartz Clock Does...|    1|  1|
    +-----------+--------------------+--------------------+-----+---+
    only showing top 20 rows
    
    Processing time: 0.42055773735046387 seconds
    
    
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [question_id#326L, creation_date#340, title#341, month#354, cnt#370L]
       +- BroadcastHashJoin [question_id#326L], [question_id#338L], Inner, BuildRight, false
          :- HashAggregate(keys=[question_id#326L, month#354], functions=[count(1)])
          :  +- HashAggregate(keys=[question_id#326L, month#354], functions=[partial_count(1)])
          :     +- Exchange hashpartitioning(month#354, 200), REPARTITION_BY_COL, [id=#990]
          :        +- Project [question_id#326L, month(cast(creation_date#328 as date)) AS month#354]
          :           +- Filter isnotnull(question_id#326L)
          :              +- FileScan parquet [question_id#326L,creation_date#328] Batched: true, DataFilters: [isnotnull(question_id#326L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Volumes/Moon/SpringBoard/spark-optimization/data/answers], PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp>
          +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]),false), [id=#999]
             +- Project [question_id#338L, creation_date#340, title#341]
                +- Filter (isnotnull(accepted_answer_id#342L) AND isnotnull(question_id#338L))
                   +- FileScan parquet [question_id#338L,creation_date#340,title#341,accepted_answer_id#342L] Batched: true, DataFilters: [isnotnull(accepted_answer_id#342L), isnotnull(question_id#338L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Volumes/Moon/SpringBoard/spark-optimization/data/questions], PartitionFilters: [], PushedFilters: [IsNotNull(accepted_answer_id), IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp,title:string,accepted_answer_id:bigint>
    
    


# Conclusion:

- As we can see from the above queries, **solution 3** give us better performance with the Processing time: `0.5135302543640137 seconds`.

- And if we only take the `accepted_answer_id` in the consideration, the **solution 4** gives us the optimal performance above all with the Processing time: `0.42055773735046387 seconds`.

