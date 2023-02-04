"""

Joins

- Merging different Datasets is a  very generic requirement present in most of data-processing pipelines

"""



region_data = spark.createDataFrame([('Family with grown ups','PN'),
 ('Driven Growers','GJ'),
 ('Conservative families','DD'),
 ('Cruising Seniors','DL'),
 ('Average Family ','MN'),
 ('Living well','KA'),
 ('Successful hedonists','JH'),
 ('Retired and Religious','AX'),
 ('Career Loners','HY'),('Farmers','JH')],
schema=StructType().add("Customer_main_type","string").add("Region Code","string"))


region_data.show()

new_df = df.join(region_data, on='Customer_main_type')
new_df.groupBy("Region Code").count().show()
# Returns a dataframe with region code and total counts of the code


"""

Pivoting 

Create a pivot view of the dataframe for specific column

"""

df.groupBy('Customer_main_type').pivot('Avg_age').sum('Avg_salary').fillna(0).show()

df.groupBy('Customer_main_type').pivot('label').sum('Avg_salary').fillna(0).show()


"""

Windowed Functions Or Windowed Aggregates
	- Allows to perform certain operations on group of records known as "Within the window".
	- It calculates the results for each row within the window.

PySpark has three window functions 
	- Aggregations 
	- Ranking 
	- Analytics 

"""


from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

win = Window.orderBy(df['Avg_salary'].desc())
df = df.withColumn('rank', row_number().over(win).alias('rank'))
df.show()


win_1 = Window.partitionBy("Customer_subtype").orderBy(df['Avg_salary'].desc())
df = df.withColumn('rank', row_number().over(win_1).alias('rank'))

# As, we have a new column rank that consists of the rank or each category of Customer_type, Filter the top-three ranks for each category easily

df.groupBy('rank').count().orderBy('rank').show()

df.filter(col('rank')< 4).show()
