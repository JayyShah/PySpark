from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Structured_streaming').getOrCreate()
import pyspark.sql.functions as F 
from pyspark.sql.types import *


# Create some Self-generated Data that can be pushed into a respective local directory ("csv Folder") to be read by structured streaming 
# The data will generate contains 4 columns as it is in a csv format (user_id, app, Timr spent, Age) 

df_1 = df_1=spark.createDataFrame([("XN203",'FB',300,30),
("XN201",'Twitter',10,19),("XN202",'Insta',500,45)],
["user_id","app","time_in_secs","age"]).write.csv("csv_folder",mode='append')

# Once the dataframes are created, Define the schema

schema = StructType().add("user_id", "string").add("app","string").add("time_in_secs", "integer").add("age","integer")

# The API to read a Static dataframe is similar to that for reading a streaming dataframe 

data = spark.readStream.option("sep",",").schema(schema).csv("csv_folder")

# Validate the schema of the dataframe

data.printSchema()


"""

Operations:
	- Once having streaming data available, Multiple transformations can be applied, in order to get different results, based on specific requirements.

"""

app_count = data.groupBy('app').count()

# Now, In order to view the results, mention the output mode, in addition to the desired location.builder

query = (app_count.writeStream.queryName('count_query').outputMode('complete').format('memory').start())

spark.sql("select * from count_query").toPandas().head(5)

# In the below example, the query is being written to filter only the records of the Facebook app. The average time spent by the user on the app is the calculated

fb_data = data.filter(data['app']=='FB')
fb_avg_time = fb_data.groupBy('user_id').avg(F.avg("time_in_secs"))
fb_query = (fb_avg_time.writeStream.queryName('fb_query').outputMode('complete').format('memory').start())


# Since there is only one dataframe currently in the local folder, the output of one user acceesing FB and the time spent. In Order, to view more relative results, Push more self-generated data in the folder

df_2 = =spark.createDataFrame([("XN203",'FB',100,30),("XN201",'FB',10,19),("XN202",'FB',2000,45)],["user_id","app","time_in_secs","age"]).write.csv("csv_folder",mode='append')



# Spark Structured Streaming has read the new records and append them into the streaaming dataframe and the new results for the same query will differ from the last one

spark.sql("select * from fb_query").toPandas().head(5)

# Adding few more data in the folder

df_3 = spark.createDataFrame([("XN203",'FB',500,30),("XN201",'Insta',30,19),("XN202",'Twitter',100,45)],["user_id","app","time_in_secs","age"]).write.csv("csv_folder",mode='append')

spark.sql("select * from fb_query").toPandas().head(5)


# In the above output, we see Aggregation and sorting of the query on the existing dataframe in the local folder

app_df = data.groupBy('app').agg(F.sum('time_in_secs').alias('total_time')).orderBy('total_time',ascending=False)
app_query = (app_df.writeStream.queryName('app_wise_query').outputMode('complete').format('memory').start())

spark.sql("select * from app_wise_query").toPandas().head(5)

# As we have results for each aap and the total time spent by the users on the respective app, using Stream dataframe

df_4=spark.createDataFrame([("XN203",'FB',500,30),("XN201",'Insta',30,19),("XN202",'Twitter',100,45)],["user_id","app","time_in_secs","age"]).write.csv("csv_folder",mode='append')

spark.sql("select * from app_wise_query").toPandas().head(5)

# In this, we will try to find the average age of users for every app in data

age_df = data.groupBy('app').agg(F.avg('age').alias('mean_age')).orderBy('mean_age',ascending=False)
age_query = (age_df.writeStream.queryName('age_query').outputMode('complete').format('memory').start())


df_5=spark.createDataFrame([("XN210",'FB',500,50),("XN255",'Insta',30,23),("XN222",'Twitter',100,30)],["user_id","app","time_in_secs","age"]).write.csv("csv_folder",mode='append')
spark.sql("select * from age_query").toPandas().head(5)

# Since we have a static dataframe, we can simply write a new query to join the streaming Dataframe and merge both of them in the app column

app_stream_df = data.join(app_df,'app')
join_query = (app_stream_df.writeStream.queryName('join_query').outputMode('complete').format('memory').start())

spark.sql("select * from join_query").toPandas().head(50)
