from pyspark.sql.functions import udf
df.groupBy("Avg_age").count().show()


# Writing a UDF to define age_category

def age_category(age):
	if age =="20-30 years":
		return "Young"
	elif age == "30-40 years":
		return "Mid Aged"
	elif ((age=="40-50 years") or (age == "50-60 years")):
		return "old"
	else:
		return "Very Old"


age_udf = udf(age_category, StringType())
df = df.withColumn('age_category',age_udf(df['Avg_age']))
df.select('Avg_age', 'age_category').show()

# Returns a table with all the age groups and their age age_category

df.groupBy("age_category").count().show()

# returns a dataframe with all the age age_categories and the total count present in the file



"""

Pandas UDF

"""

df.select('Avg_salary').summary().show()

# Pandas UDF type

def scaled_salary(salary):
	scaled_sal = (salary-min_sal)/(max_sal-min_sal)
	return scaled_sal

scaling_udf = pandas_udf(scaled_salary, DoubleType())
df.withColumn("scaled_salary",scaling_udf(df['Avg_salary'])).show(10,False)