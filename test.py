import pandas as pd
import json
from pandas import DataFrame
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

#Create Pandas Dataframe
companies_data = pd.read_json('test.json')
dataFrame = pd.DataFrame(companies_data)
print(dataFrame.T)

#manipulate Pandas dataframe and write back to json file

change_companies_data = dataFrame.loc['exchange_code']= 123456
json_rewrite = dataFrame.to_json('test.json')
dataFrameTranspose = dataFrame.T

#Convert pandas dataframe to spark dataframe
spark_dataframe = spark.createDataFrame(dataFrameTranspose.astype(str))
spark_dataframe.show()

#Convert Spark dataframe to pandas dataframe 
spark_to_panda_dataframe = spark_dataframe.toPandas()
print(spark_to_panda_dataframe)







