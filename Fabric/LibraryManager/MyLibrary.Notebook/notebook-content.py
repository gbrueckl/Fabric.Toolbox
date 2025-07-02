# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# variable "spark" is not set when calling sc.addPyFile()
_is_sc_addPyFile = not "spark" in locals()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import DataFrame
from pyspark import  SparkContext
from pyspark.sql import DataFrame, SparkSession

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

PI = 3.14

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_area(radius):
    area = float(PI)*radius*radius
    return area

# some code to run only when interactively developing the library
if not _is_sc_addPyFile:
    print(get_area(4))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
