# The first step is to create a SparkSession object, In order to use spark. 
# Import the required dependencies

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('data_processing').getOrCreate()
import pyspark.sql.functions as F 
from pyspark.sql.types import * 
import warnings
warnings.filterwarnings('ignore')

"""

1. Creating a Dataframe

"""
schema = StructType().add("user_id","string").add("country", "string").add("browser", "string").add("OS", "string").add("age","integer")

df =spark.createDataFrame([("A203", 'India', "Chrome", "WIN", 33), ("A201", 'China', "Safari","MacOS", 35), ("A205", 'UK', "Morzilla", "Linux", 25)], schema=schema)
# df.printSchema()
# df.show()


"""

2. Handling Null Values

"""

# Create a new Dataframe (df_na) with empty values

df_na=spark.createDataFrame([("A203",None,"Chrome","WIN",33),("A201",'China',None,"MacOS",35),("A205",'UK',"Mozilla","Linux",25)],schema=schema)
# df_na.show()

	# 2.1 Filling with Values

df_na.fillna('0').show()

	# 2.2 Fill with specific values

df_na.fillna({'country':'USA', 'browser':'Safari'}).show()


	# 2.3  In order to drop the rows with any null values, use na.drop

df_na.na.drop().show()

	# 2.4  Drop a particular subset

df_na.na.drop(subset='country').show()

	# 2.5 replace data points with particular values

df_na.replace("Chrome", "Google Chrome").show()

	# 2.6 Drop a particular column

df_na.drop('user_id').show()