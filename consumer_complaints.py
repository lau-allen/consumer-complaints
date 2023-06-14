#!/usr/bin/python

#importing libraries 
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys 

#spark session
spark = SparkSession.builder.getOrCreate()
sc = pyspark.SparkContext.getOrCreate()

#defining the input path 
path = sys.argv[1]


#function to handle multiline csv reading 
def extractData(partId, records):
  if partId == 0:
    next(records)
  import csv
  reader = csv.reader(records)
  for row in reader:
    if len(row) == 18:
      yield row


#loading data into rdd using extractData function
data = sc.textFile(path, use_unicode=True).cache().mapPartitionsWithIndex(extractData)

#converting to pyspark dataframe to compute output 
df = data.toDF(['Date received', 'Product', 'Sub-product', 'Issue', 'Sub-issue', 'Consumer complaint narrative', 'Company public response', 'Company', 'State', 'ZIP code', 'Tags', 'Consumer consent provided?', 'Submitted via', 'Date sent to company', 'Company response to consumer', 'Timely response?', 'Consumer disputed?', 'Complaint ID'])

print(df.count())

#reading in the csv data, retrieving only the year for date, and making all product names lowercase 
df = df.withColumn('Date received', F.split('Date received','-')[0]) \
  .withColumnRenamed('Date received','year') \
  .withColumn('Product', F.lower('Product'))

#find the total complaints and total companies by product, year 
dfA = df.groupBy(['Product','year']).agg(F.count('*').alias('Total Complaints'),\
                                        F.countDistinct('Company').alias('Total Companies'))

#find the max total complaints by product, year when grouped by product, year, and company 
dfB = df.groupBy(['Product','year','Company']).agg(F.count('*')\
        .alias('Total Complaints')).groupBy(['Product','year']).agg(F.max('Total Complaints'))

#joining tables 
dfC = dfA.join(dfB, (dfA.Product == dfB.Product) & (dfA.year == dfB.year)).select(dfA['*'],dfB['max(Total Complaints)'])

#calculating the percentage, formatting table for output 
output = dfC.withColumn('Highest Perc of Total Complaints',F.round(dfC['max(Total Complaints)']/dfC['Total Complaints']*100).cast('integer'))\
      .select('Product','year','Total Complaints','Total Companies','Highest Perc of Total Complaints').sort('Product','year')

#converting columns to string, and converting into rdd containing strings with comma separated values 
output = output.select([output[c].cast('string') for c in output.columns]).rdd.map(lambda x: ','.join(x))

#saving as csv 
output.saveAsTextFile(sys.argv[2])

print(output.count())
