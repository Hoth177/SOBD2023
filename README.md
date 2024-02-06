# SOBD2023
```python
#Import other modules not related to PySpark
import os
import sys
import pandas as pd
from pandas import DataFrame
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib
import math
from IPython.core.interactiveshell import InteractiveShell
from datetime import *
import statistics as stats
from pyspark.sql.functions import percentile_approx, mean, avg, stddev, min, max, when, col, count, length, lag, expr, percent_rank
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.sql.functions import round
InteractiveShell.ast_node_interactivity = "all" 
%matplotlib inline

#Import PySpark related modules
import pyspark
from pyspark.rdd import RDD
from pyspark.sql import Row
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions
from pyspark.sql.functions import lit, desc, col, size, array_contains\
, isnan, udf, hour, array_min, array_max, countDistinct
from pyspark.sql.types import *

MAX_MEMORY = '8G'
# Initialize a spark session.
conf = pyspark.SparkConf().setMaster("local[*]") \
        .set('spark.executor.heartbeatInterval', 10000) \
        .set('spark.network.timeout', 10000) \
        .set("spark.core.connection.ack.wait.timeout", "3600") \
        .set("spark.executor.memory", MAX_MEMORY) \
        .set("spark.driver.memory", MAX_MEMORY)
def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Pyspark guide") \
        .config(conf=conf) \
        .getOrCreate()
    return spark

spark = init_spark()
filename_data = 'work/data/itineraries.csv'
# Load the main data set into pyspark data frame 
df = spark.read.csv(filename_data,header=True, mode="DROPMALFORMED", inferSchema=True)
from pyspark.sql.functions import col, array

print(df.dtypes)
```
[('legId', 'string'), ('searchDate', 'date'), ('flightDate', 'date'), ('startingAirport', 'string'), ('destinationAirport', 'string'), ('fareBasisCode', 'string'), ('travelDuration', 'string'), ('elapsedDays', 'int'), ('isBasicEconomy', 'boolean'), ('isRefundable', 'boolean'), ('isNonStop', 'boolean'), ('baseFare', 'double'), ('totalFare', 'double'), ('seatsRemaining', 'int'), ('totalTravelDistance', 'int'), ('segmentsDepartureTimeEpochSeconds', 'string'), ('segmentsDepartureTimeRaw', 'string'), ('segmentsArrivalTimeEpochSeconds', 'string'), ('segmentsArrivalTimeRaw', 'string'), ('segmentsArrivalAirportCode', 'string'), ('segmentsDepartureAirportCode', 'string'), ('segmentsAirlineName', 'string'), ('segmentsAirlineCode', 'string'), ('segmentsEquipmentDescription', 'string'), ('segmentsDurationInSeconds', 'string'), ('segmentsDistance', 'string'), ('segmentsCabinCode', 'string')]
```python
df.head(1)
```
[Row(legId='9ca0e81111c683bec1012473feefd28f', searchDate=datetime.date(2022, 4, 16), flightDate=datetime.date(2022, 4, 17), startingAirport='ATL', destinationAirport='BOS', fareBasisCode='LA0NX0MC', travelDuration='PT2H29M', elapsedDays=0, isBasicEconomy=False, isRefundable=False, isNonStop=True, baseFare=217.67, totalFare=248.6, seatsRemaining=9, totalTravelDistance=947, segmentsDepartureTimeEpochSeconds='1650214620', segmentsDepartureTimeRaw='2022-04-17T12:57:00.000-04:00', segmentsArrivalTimeEpochSeconds='1650223560', segmentsArrivalTimeRaw='2022-04-17T15:26:00.000-04:00', segmentsArrivalAirportCode='BOS', segmentsDepartureAirportCode='ATL', segmentsAirlineName='Delta', segmentsAirlineCode='DL', segmentsEquipmentDescription='Airbus A321', segmentsDurationInSeconds='8940', segmentsDistance='947', segmentsCabinCode='coach')]
```python
print(df.count())
```
82138753
