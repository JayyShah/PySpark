from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('file-handling').getOrCreate()
import pyspark.sql.functions as F 
from pyspark.sql.types import * 
import warnings
warnings.filterwarnings('ignore')

# 1. Reading a CSV file

df = spark.read_csv('path/to/.csv', header=True, inferSchema=True)

# prints total number of rows in the file
df.count()

# prints total number of oolumns in the file
len(df.columns)

# Prints the file structure/Root of the file
df.printSchema()

# Shows the Dataframe (Only the top 3 rows)
df.show(3)

# prints the summary of the dataframe
df.summary().show()


"""

Subset of a Dataframe

Conditions:

	* Select
	* Filter 
	* Where

"""

# 1. Select 

df.select(['Customer_subtype', 'Avg_salary']).show()

df.filter(df['Avg_salary'] > 1000000).count()
# returns total counts of the condition

df.filter(df['Avg_salary'] > 100000).show()
# returns the output of the condition

# 2 apply more than one filter on the dataframe, by including more conditions.
# Can be done using two ways
	# 1. Applying Consecutive filters, then by using (&, or) operands with a Where statement

df.filter(df['Avg_salary'] > 500000).filter(df['Number_of_houses'] > 2).show()
# returns the dataframe with the applied filters


# ---------------------------------------------------------------------------------------------------------------------------------------------------------

"""

Aggregations:
	
	* Split 
	* Apply
	* Combine

"""

# First step is to split the data, based on a column or a group of columns , followed by performing the operations pn those small individual 
# groups(counts, max, avg, etc). Once done, combine all these results


df.groupBy('Customer_subtype').count().show()

for col in df.columns:
	if col!= 'Avg_salary':
		print(f'Aggregation for {col}')
		df.groupBy(col).count().orderBy('count',ascending=False).show(truncate=False)

# prints the Aggregation for Customer_subtype


# As mentioned, different kinds of operations on groups of records can be done as :
# 	* Mean
# 	* Max
# 	* Min
# 	* Sum


df.groupBy('Customer_main_type').agg(F.mean('Avg_salary')).show()
# Returns mean of Avg salary

df.groupBy('Customer_main_type').agg(F.max('Avg_salary')).show()
# returns the max of average salary

df.groupBy('Customer_main_type').agg(F.min('Avg_salary')).show()
# returns the min of average salary

df.groupBy('Customer_main_type').agg(F.sum('Avg_salary')).show()
# returns the sum of average salary



# Sometimes, there is simply a need to sort the data with aggregation/ without any sort of aggregation by using "Sort" or "orderBy"


df.sort("Avg_salary", ascending=False).show()
# returns the dataframe from top seniors salary to deducting form

df.groupBy('Customer_subtype').agg(F.avg('Avg_salary').alias('mean_salary').orderBy('mean_salary', ascending=False).show(50,False))
# returns the dataframe of customers avg pay/mean salary from top end seniors in deducting form

df.groupBy('Customer_subtype').agg(F.max('Avg_salary').alias('max_salary').orderBy('Max_salary', ascending=False).show(50,False))
# returns the dataframe of customers max salary from top end seniors in deducting form



# Collecting Values
	# * Collect List
	# * Collect Set

df.groupBy("Customer_subtype").agg(F.collect_list("Number_of_houses")).show()
# Collect lists provides all the values in the original order of occurrence (which can be reversed as well).

df.groupBy("Customer_subtype").agg(F.collect_set("Number_of_houses")).show()
# Collect set provides only the unique values. 


# The need to create a new column with a constant value can be very common. Using 'lit' function

df = df.withColumn('constant',F.lit('finance'))
df.select('Customer_subtype','constant').show()