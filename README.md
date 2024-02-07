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
## Определяем типы признаков в датасетах
```python
print('Обзор данных')
df.printSchema()
```
Обзор данных  
root  
 |-- legId: string (nullable = true)  
 |-- searchDate: date (nullable = true)  
 |-- flightDate: date (nullable = true)  
 |-- startingAirport: string (nullable = true)  
 |-- destinationAirport: string (nullable = true)  
 |-- fareBasisCode: string (nullable = true)  
 |-- travelDuration: string (nullable = true)  
 |-- elapsedDays: integer (nullable = true)  
 |-- isBasicEconomy: boolean (nullable = true)  
 |-- isRefundable: boolean (nullable = true)  
 |-- isNonStop: boolean (nullable = true)  
 |-- baseFare: double (nullable = true)  
 |-- totalFare: double (nullable = true)  
 |-- seatsRemaining: integer (nullable = true)  
 |-- totalTravelDistance: integer (nullable = true)  
 |-- segmentsDepartureTimeEpochSeconds: string (nullable = true)  
 |-- segmentsDepartureTimeRaw: string (nullable = true)  
 |-- segmentsArrivalTimeEpochSeconds: string (nullable = true)  
 |-- segmentsArrivalTimeRaw: string (nullable = true)  
 |-- segmentsArrivalAirportCode: string (nullable = true)  
 |-- segmentsDepartureAirportCode: string (nullable = true)  
 |-- segmentsAirlineName: string (nullable = true)  
 |-- segmentsAirlineCode: string (nullable = true)  
 |-- segmentsEquipmentDescription: string (nullable = true)  
 |-- segmentsDurationInSeconds: string (nullable = true)  
 |-- segmentsDistance: string (nullable = true)  
 |-- segmentsCabinCode: string (nullable = true)  
 Обзор колонок  
| | Имя колонки |	Тип данных |
|-|-------------|------------------|
|0|	legId|	string|
|1|	searchDate|	date|
|2|	flightDate|	date|
|3|	startingAirport|	string|
|4|	destinationAirport|	string|
|5|	fareBasisCode|	string|
|6|	travelDuration|	string|
|7|	elapsedDays|	int|
|8|	isBasicEconomy|	boolean|
|9|	isRefundable|	boolean|
|10|	isNonStop|	boolean|
|11|	baseFare|	double|
|12|	totalFare|	double|
|13|	seatsRemaining|	int|
|14|	totalTravelDistance|	int|
|15|	segmentsDepartureTimeEpochSeconds|	string|
|16|	segmentsDepartureTimeRaw|	string|
|17|	segmentsArrivalTimeEpochSeconds|	string|
|18|	segmentsArrivalTimeRaw|	string|
|19|	segmentsArrivalAirportCode|	string|
|20|	segmentsDepartureAirportCode|	string|
|21|	segmentsAirlineName|	string|
|22|	segmentsAirlineCode|	string|
|23|	segmentsEquipmentDescription|	string|
|24|	segmentsDurationInSeconds|	string|
|25|	segmentsDistance|	string|
|26|	segmentsCabinCode|	string|
### Обозначим типы признаков:
legId: Идентификатор рейса.  
searchDate (дата поиска): Дата (ГГГГ-ММ-ДД), когда эта запись была взята из Expedia.  
flightDate (дата полета): Дата (ГГГГ-ММ-ДД) перелета.  
startingAirport: Трехсимвольный код аэропорта для начального местоположения.  
destinationAirport (аэропорт назначения): Трехсимвольный код аэропорта для места прибытия.  
fareBasisCode: Код основы тарифа.  
travelDuration (продолжительность поездки): Продолжительность поездки в часах и минутах.  
elapsedDays (истекшие дни): Количество истекших дней (обычно 0).  
isBasicEconomy: Булево значение, указывающее, является ли билет базовым экономичным.  
isRefundable: Булево значение, указывающее, можно ли вернуть деньги за билет.  
isNonStop: Булево значение, указывающее, является ли рейс беспосадочным.  
baseFare (базовый тариф): Цена билета (в USD).  
totalFare: Цена билета (в USD), включая налоги и другие сборы.  
seatsRemaining: Целое число для количества оставшихся мест.  
totalTravelDistance (общее расстояние): Общее расстояние поездки в милях.  
segmentsDepartureTimeEpochSeconds: Строка, содержащая время отправления (время Unix) для каждого этапа поездки. Записи для каждого этапа разделяются символом '||'.  
segmentsDepartureTimeRaw: Строка, содержащая время отправления (формат ISO 8601: YYYY-MM-DDThh:mm:ss.000±[hh]:00) для каждого этапа поездки. Записи для каждого из этапов разделяются символом '||'.  
segmentsArrivalTimeEpochSeconds: Строка, содержащая время прибытия (время Unix) для каждого этапа поездки. Записи для каждого этапа разделяются символом '||'.  
segmentsArrivalTimeRaw: Строка, содержащая время прибытия (формат ISO 8601: YYYY-MM-DDThh:mm:ss.000±[hh]:00) для каждого этапа поездки. Записи для каждого этапа разделяются символом '||'.  
segmentsArrivalAirportCode: Строка, содержащая код аэропорта для места прибытия для каждого этапа поездки. Записи для каждого этапа разделяются символом '||'.  
segmentsDepartureAirportCode: Строка, содержащая код аэропорта для места отправления для каждого этапа путешествия. Записи для каждого этапа разделяются символом '||'.  
segmentsAirlineName: Строка, содержащая название авиакомпании, обслуживающей каждый этап путешествия. Записи для каждого этапа разделяются символом '||'.  
segmentsAirlineCode: Строка, содержащая двухбуквенный код авиакомпании, обслуживающей каждый этап путешествия. Записи для каждого этапа разделяются символом '||'.  
segmentsEquipmentDescription: Строка, содержащая тип самолета, используемого для каждого этапа путешествия (например, "Airbus A321" или "Boeing 737-800"). Записи для каждого из этапов разделяются символом '||'.  
segmentsDurationInSeconds: Строка, содержащая продолжительность полета (в секундах) для каждого этапа путешествия. Записи для каждого этапа разделяются символом '||'.  
segmentsDistance: Строка, содержащая пройденное расстояние (в милях) для каждого этапа поездки. Записи для каждого этапа разделяются символом '||'.  
segmentsCabinCode: Строка, содержащая код салона для каждого этапа поездки (например, "coach"). Записи для каждого этапа разделяются символом '||'.  
