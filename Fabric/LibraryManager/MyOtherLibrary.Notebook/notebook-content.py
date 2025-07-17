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

import datetime as dt

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# when called via sc.addPyFile can import other libraries as well using the syntax below
if _is_sc_addPyFile:
    from .MyLibrary import PI
else:
    # for debugging we need to define the variables here
    PI = 123

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_circumference(radius):
    return 2*radius*PI

if not _is_sc_addPyFile:
    print(get_circumference(1))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def log(text: str, end: str = None):
  print(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\t" + str(text), end = end)

# some code to run only when interactively developing the library
if not _is_sc_addPyFile:
    log("This is a test log!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
