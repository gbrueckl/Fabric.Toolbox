# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8e6faaa0-29be-48c1-b0f3-1ffe17396203",
# META       "default_lakehouse_name": "TPCH",
# META       "default_lakehouse_workspace_id": "e2278f6a-27f2-4261-b759-052b593650b0"
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%configure -f
# MAGIC {
# MAGIC     // You can get a list of valid parameters to config the session from https://github.com/cloudera/livy#request-body.
# MAGIC     "driverMemory": "56g", // Recommended values: ["28g", "56g", "112g", "224g", "400g"]
# MAGIC     "driverCores": 8, // Recommended values: [4, 8, 16, 32, 64]
# MAGIC     "executorMemory": "56g",
# MAGIC     "executorCores": 8,
# MAGIC     "numExecutors": 4,
# MAGIC     "useStarterPool": false  // Set to true to force using starter pool
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import datetime as dt
import sempy.fabric as fabric

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window, Row

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

scale_factor = 10 # 1, 10 or 100

df = spark.sql(f"SELECT * FROM TPCH.sf{scale_factor}_lineitem")
display(df)

group_by_cols = ["OrderId"]
sorting_cols = ["LineNumber"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# for validation of the code
if False:
    rdd = sc.parallelize([
        [1, date(2024, 1, 7), 13.90],
        [1, date(2024, 1, 16), 14.50],
        [2, date(2024, 1, 9), 10.50],
        [2, date(2024, 1, 28), 9.90],
        [3, date(2024, 1, 5), 1.50]
    ])

    df = rdd.toDF(['product_key', 'date', 'price'])

    display(df)

    group_by_cols = ['product_key']
    sorting_cols = ['date']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def log(text: str, end: str = None):
  print(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\t" + str(text), end = end)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def evaluate_result(df):
    log("Simulating write operation ... ")
    df.write.format("noop").mode("overwrite").save()
    log("Done!")

# read the df once 
evaluate_result(df)
log(f"Rowcount: {df.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Get the most recent price per product

# MARKDOWN ********************

# ## using window function

# CELL ********************

# MAGIC %%timeit -n 1 -r 3
# MAGIC # define window, used DESC sort order
# MAGIC w = Window.partitionBy(group_by_cols).orderBy([F.col(x).desc() for x in sorting_cols])
# MAGIC 
# MAGIC #filter DataFrame to only show first row for each group
# MAGIC df_latest_window = df.withColumn('__row_num__', F.row_number().over(w)).filter(F.col('__row_num__') == 1).drop('__row_num__')
# MAGIC #display(df_latest_window)
# MAGIC #show_plan(df_latest_window)
# MAGIC evaluate_result(df_latest_window)
# MAGIC 
# MAGIC # SF1:      
# MAGIC # SF10:     21.9 s ± 381 ms per loop (mean ± std. dev. of 3 runs, 1 loop each)
# MAGIC # SF100:    1min 10s ± 18.4 s per loop (mean ± std. dev. of 3 runs, 1 loop each)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## using self join

# CELL ********************

# MAGIC %%timeit -n 1 -r 3
# MAGIC df_latest_dates_per_group = df.groupBy(group_by_cols).agg(*[F.max(x).alias(x) for x in sorting_cols])
# MAGIC #display(df_latest_dates_per_group)
# MAGIC 
# MAGIC df_latest_join = df.alias("base").join(df_latest_dates_per_group, group_by_cols + sorting_cols, "inner").select("base.*")#
# MAGIC #display(df_latest_join)
# MAGIC #show_plan(df_latest_join)
# MAGIC evaluate_result(df_latest_join)
# MAGIC 
# MAGIC # SF1:      
# MAGIC # SF10:     23.9 s ± 1.35 s per loop (mean ± std. dev. of 3 runs, 1 loop each)
# MAGIC # SF100:    1min 21s ± 4.76 s per loop (mean ± std. dev. of 3 runs, 1 loop each)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## using max and struct

# CELL ********************

# MAGIC %%timeit -n 1 -r 3
# MAGIC df_latest_max_struct = df.groupBy(group_by_cols).agg(F.max(F.struct(*sorting_cols + [x for x in df.columns if x not in sorting_cols])).alias("latest")).select("latest.*")
# MAGIC #display(df_latest_max_struct)
# MAGIC #show_plan(df_latest_max_struct)
# MAGIC evaluate_result(df_latest_max_struct)
# MAGIC 
# MAGIC # SF1:      
# MAGIC # SF10:     21.6 s ± 1.5 s per loop (mean ± std. dev. of 3 runs, 1 loop each)
# MAGIC # SF100:    58.3 s ± 511 ms per loop (mean ± std. dev. of 3 runs, 1 loop each)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## using max_by

# CELL ********************

# MAGIC %%timeit -n 1 -r 3
# MAGIC df_latest_max_by = df.groupBy(group_by_cols).agg(F.max_by(F.struct("*"), F.struct(*sorting_cols)).alias("latest")).select("latest.*")
# MAGIC #display(df_latest_max_struct)
# MAGIC #show_plan(df_latest_max_struct)
# MAGIC evaluate_result(df_latest_max_by)
# MAGIC 
# MAGIC # SF1:      
# MAGIC # SF10:     24.4 s ± 5.76 s per loop (mean ± std. dev. of 3 runs, 1 loop each)
# MAGIC # SF100:    53.5 s ± 1.24 s per loop (mean ± std. dev. of 3 runs, 1 loop each)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## using max_by - no struct in sort

# CELL ********************

# MAGIC %%timeit -n 1 -r 3
# MAGIC df_latest_max_by = df.groupBy(group_by_cols).agg(F.max_by(F.struct("*"), sorting_cols[0]).alias("latest")).select("latest.*")
# MAGIC #display(df_latest_max_struct)
# MAGIC #show_plan(df_latest_max_struct)
# MAGIC evaluate_result(df_latest_max_by)
# MAGIC 
# MAGIC # SF1:      
# MAGIC # SF10:     27.5 s ± 4.28 s per loop (mean ± std. dev. of 3 runs, 1 loop each)
# MAGIC # SF100:    52.8 s ± 1.21 s per loop (mean ± std. dev. of 3 runs, 1 loop each)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
