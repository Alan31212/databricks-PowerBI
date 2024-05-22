# Databricks notebook source
import pandas as pd
import numpy as np 
from IPython.display import display_html
from pyspark.sql import SparkSession
import json
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC #Bronze Level

# COMMAND ----------

# MAGIC %md ####Loading data(json and csv)

# COMMAND ----------

#data1
with open('/Workspace/Users/tlu22@hawk.iit.edu/data/house_3.json') as json_file:
    data = json.load(json_file)

# Convert the JSON data into a DataFrame
df1 = pd.DataFrame(data)
df1.info()
display_html(df1.head(10))

# COMMAND ----------

#data2
df2 = pd.read_csv('/Workspace/Users/tlu22@hawk.iit.edu/data/housing_clean_2.csv', sep=';')
df2.info()
display_html(df2)

# COMMAND ----------

# MAGIC %md ####Cheking if there is a missing value for data frame 1

# COMMAND ----------

# Check for missing values in the DataFrame
missing_values = df1.isnull().sum()
print("Missing values:\n", missing_values)


# COMMAND ----------

# MAGIC %md ###filling missing vlaues

# COMMAND ----------

# Fill missing values in specific columns with 0
df_zero = ['latitude', 'longitude']
df_new_zero = df1.copy()
df_new_zero[df_zero] = df_new_zero[df_zero].fillna(value=0)

# Fill missing values columns with "Null"
df_null = ['description']
df_new_null = df1.copy()
df_new_null[df_null] = df_new_null[df_null].fillna(value='Null')

# Combine the results
df1_conver = df_new_zero.combine_first(df_new_null)
df1_conver.isnull().sum()

# COMMAND ----------

# MAGIC %md ###Preprosessing

# COMMAND ----------

# Define the schema of df1
schema = {
    'address': str,
    'product_tree': str,
    'time': str,
    'views': int,
    'title': str,
    'id': str,
    'price_gel': float,
    'price_usd': float,
    'space': str,
    'room': str,
    'bedroom': str,
    'floor': str,
    'description': str,
    'amenities': str,
    'latitude': float,
    'longitude': float,
    'poster_type': str,
    'poster_id': str
}

# Preprocess price, space, id, and room columns
df1_conver['price_gel'] = df1_conver['price_gel'].str.replace(',', '').astype(float)
df1_conver['price_usd'] = df1_conver['price_usd'].str.replace(',', '').astype(float)

df1_conver['space'] = df1_conver['space'].str.replace('Area: ', '')

df1_conver['id'] = df1_conver['id'].str.replace(':', '')

df1_conver['room'] = df1_conver['room'].str.replace('Room', '')

# Convert data types
df1_convert = df1_conver.astype(schema)

# Based on another data that only update to 29204, so we set the limit rows for data1
num_rows_to_keep = 29204
df1_converted = df1_convert.head(num_rows_to_keep)

# Display the limited DataFrame
df1_converted.info()

# COMMAND ----------

#set the time format
def parse_time(time_str):
    today = datetime.date(2020, 11, 11)
    yesterday = today - datetime.timedelta(days=1)
    
    if "Today" in time_str:
        time_str = time_str.replace("Today", today.strftime("%d %b"))
    elif "Yesterday" in time_str:
        time_str = time_str.replace("Yesterday", yesterday.strftime("%d %b"))
        

    time_str = time_str.strip()
    
    if len(time_str.split()) == 3:
        time_str += f' {today.year}'
    return datetime.datetime.strptime(time_str, "%d %b %H:%M %Y")

df1_converted['time'] = df1_converted['time'].apply(parse_time)


# COMMAND ----------

display_html(df1_converted.head(10))

# COMMAND ----------

# MAGIC %md ###Loading second data(CSV)

# COMMAND ----------

df2 = pd.read_csv('/Workspace/Users/tlu22@hawk.iit.edu/data/housing_clean_2.csv', sep=';')
display_html(df2)

# COMMAND ----------

df2.info()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Preposessing

# COMMAND ----------

# Split the single column into multiple columns based on commas
df2 = df2.iloc[:, 0].str.split(',', expand=True)

# Remove the first column
df2 = df2.drop(columns=0)

# Define column names
column_names = [
    'price', 'space', 'room', 'bedroom', 'furniture', 'latitude', 'longitude',
    'city_area', 'floor', 'max_floor', 'apartment_type', 'renovation_type', 'balcony'
]

# Assign column names to the DataFrame
df2.columns = column_names

# Set the post ID as the primary key
df2['postid'] = df2.index

df2.info()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Checking missing value

# COMMAND ----------

# Check for missing values in the DataFrame
missing_values = df2.isnull().sum()
print("Missing values:\n", missing_values)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Define schema

# COMMAND ----------

schema = {
    'price': float,
    'space': float,
    'room': int,
    'bedroom': int,
    'furniture': int,
    'latitude': float,
    'longitude': float,
    'city_area': str,
    'floor': int,
    'max_floor': int,
    'apartment_type': str,
    'renovation_type': str,
    'balcony': int,
    'postid': int
}

# Replace empty strings ('') with NaN in numeric columns
numeric_columns = ['latitude', 'longitude']
df2[numeric_columns] = df2[numeric_columns].replace('', np.nan)

# Convert data types
df2_converted = df2.astype(schema)

# Print DataFrame
df2_converted.info()
display_html(df2_converted)

# COMMAND ----------

display_html(df2_converted.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #Silver Level

# COMMAND ----------

# MAGIC %md
# MAGIC ####Generate fact and dimension tables

# COMMAND ----------

# Select specific columns from df1
df1_selected1 = df1_converted[['address', 'time', 'views', 'title', 'id', 'price_usd', 'description', 'amenities', 'poster_type', 'poster_id']]

# Select specific columns from df2
df2_selected1 = df2_converted[['postid', 'city_area', 'furniture', 'space', 'floor', 'max_floor', 'room', 'bedroom', 'apartment_type', 'renovation_type', 'balcony']]

makefact_order = ['postid', 'title', 'address', 'city_area', 'time', 'views', 'id', 'price_usd', 'description', 'amenities', 'space', 'floor', 'max_floor', 'room', 'bedroom', 'apartment_type', 'renovation_type', 'furniture', 'balcony', 'poster_type', 'poster_id']

# Concatenate the selected columns from both DataFrames
fact_table = pd.concat([df2_selected1, df1_selected1], axis=1)
fact_table = fact_table[makefact_order]

fact_table.info()
display_html(fact_table)



# COMMAND ----------

# Select specific columns from df1
df1_selected2 = df1_converted[['address', 'id', 'price_usd', 'amenities']]

# Select specific columns from df2_converted
df2_selected2 = df2_converted[['city_area', 'furniture', 'space', 'floor', 'max_floor', 'room', 'bedroom', 'apartment_type', 'renovation_type', 'balcony']]

# makeing order
make_order = ['id', 'address', 'city_area', 'price_usd', 'space', 'floor', 'max_floor', 'room', 'bedroom', 'furniture',  'amenities', 'balcony', 'apartment_type', 'renovation_type']

# Concatenate the selected columns from both DataFrames
dem_table1 = pd.concat([df2_selected2, df1_selected2], axis=1)
dem_table1 = dem_table1[make_order]

dem_table1.info()
display_html(dem_table1)

# COMMAND ----------

# Select specific columns from data1
dem_table2 = df1_converted[['poster_id', 'poster_type']]

dem_table2.info()
display_html(dem_table2)

# COMMAND ----------

# Select specific columns from df1
df1_selected_fact2 = df1_converted[['address', 'amenities']]

# Select specific columns from df2_converted
df2_selected_fact2 = df2_converted[['city_area', 'furniture', 'space', 'floor', 'max_floor', 'room', 'bedroom', 'apartment_type', 'renovation_type', 'balcony', 'postid']]

# makeing order
make_order_fact2 = ['postid', 'address', 'city_area', 'space', 'floor', 'max_floor', 'room', 'bedroom', 'furniture',  'amenities', 'balcony', 'apartment_type', 'renovation_type']

# Concatenate the selected columns from both DataFrames
fact_table2 = pd.concat([df2_selected_fact2, df1_selected_fact2], axis=1)
fact_house_info = fact_table2[make_order_fact2]


fact_house_info.info()
display_html(fact_house_info)

# COMMAND ----------

df1_select_dim2 = df1_converted[['amenities']]
df2_select_dim2 = df2_converted[['furniture', 'space', 'floor', 'max_floor', 'room', 'bedroom', 'apartment_type', 'renovation_type', 'balcony']]

dim_house_facilities = pd.concat([df1_select_dim2, df2_select_dim2], axis=1)

display_html(dim_house_facilities)

# COMMAND ----------

# MAGIC %md
# MAGIC #Gold Level

# COMMAND ----------

# MAGIC %md
# MAGIC ####create a schema and import the tables in catalog

# COMMAND ----------

# Create a schema
create_schema_sql = """
CREATE SCHEMA IF NOT EXISTS itmd526.housing
"""

spark.sql(create_schema_sql)

# COMMAND ----------

from pyspark.sql import SparkSession

spark_converted_data1 = spark.createDataFrame(fact_table)
spark_converted_data2 = spark.createDataFrame(dem_table1)
spark_converted_data3 = spark.createDataFrame(dem_table2)
spark_converted_data4 = spark.createDataFrame(fact_house_info)
spark_converted_data5 = spark.createDataFrame(dim_house_facilities)

spark_converted_data1.write.format("delta").mode("overwrite").saveAsTable("itmd526.housing.fact_table")
spark_converted_data2.write.format("delta").mode("overwrite").saveAsTable("itmd526.housing.dem_table1")
spark_converted_data3.write.format("delta").mode("overwrite").saveAsTable("itmd526.housing.dem_table2")
spark_converted_data4.write.format("delta").mode("overwrite").saveAsTable("itmd526.housing.fact_house_info")
spark_converted_data5.write.format("delta").mode("overwrite").saveAsTable("itmd526.housing.dim_house_facilities")

# COMMAND ----------

#spark.sql("DROP TABLE IF EXISTS itmd526.housing.fact_house_info")

