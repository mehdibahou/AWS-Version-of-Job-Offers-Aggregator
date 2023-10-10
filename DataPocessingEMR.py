from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, split, expr, current_timestamp
from pyspark.sql.types import IntegerType
import psycopg2

# Initialize a Spark session
spark = SparkSession.builder.appName("EMRDataProcessing").getOrCreate()

# Load data from S3 bucket
s3_bucket_path = "s3://your-s3-bucket/data/"
df = spark.read.csv(s3_bucket_path, header=True, inferSchema=True)

# Transformation 1: Remove unnecessary text from 'PublishDuration'
df = df.withColumn("PublishDuration", regexp_replace(
    df["PublishDuration"], r'\sago·More\.\.\.$', ''))

# Transformation 2: Extract 'Post_Period'
df = df.withColumn("Post_Period", split(df["PublishDuration"], " ")[0])

# Transformation 3: Remove 'Reposted' from 'PublishDuration'
df = df.withColumn("PublishDuration", regexp_replace(
    df["PublishDuration"], "Reposted", ""))

# Transformation 4: Remove leading/trailing spaces
df = df.withColumn("PublishDuration", expr("trim(PublishDuration)"))

# Transformation 5: Remove ' ago' and additional spaces
df = df.withColumn("PublishDuration", regexp_replace(
    df["PublishDuration"], " ago", ""))

# Transformation 6: Replace 'weeks' with 'w', 'week' with 'w', and 'days' with 'd'
df = df.withColumn("PublishDuration", regexp_replace(
    df["PublishDuration"], "weeks?", "w"))
df = df.withColumn("PublishDuration", regexp_replace(
    df["PublishDuration"], "days?", "d"))

# Transformation 7: Convert to timedelta
df = df.withColumn("PublishDuration", expr("interval(PublishDuration)"))

# Calculate the date 'X time ago' from the current date
current_date = current_timestamp()
df = df.withColumn("DateXTimeAgo", current_date - df["PublishDuration"])

# Calculate the difference in days and store it in a new column
df = df.withColumn("DaysDifference", (current_date -
                   df["DateXTimeAgo"]).cast(IntegerType()))

# Drop the 'Post_Period' column
df = df.drop("Post_Period")

# Transformation 8: Clean 'Region'
df = df.withColumn("Region", regexp_replace(df["Region"], r'\(.*\)', ""))
df = df.withColumn("Region", expr("trim(Region)"))

# Transformation 9: Add 'Platform_Posted' column
df = df.withColumn("Platform_Posted", expr("'LinkedIn'"))

# Write the DataFrame to Redshift
redshift_url = "jdbc:redshift://your-redshift-endpoint:5439/your-database"
redshift_table = "your-redshift-table"
redshift_properties = {
    "user": "your-redshift-username",
    "password": "your-redshift-password",
    "driver": "com.amazon.redshift.jdbc42.Driver"
}

df.write \
    .option("url", redshift_url) \
    .option("dbtable", redshift_table) \
    .option("user", redshift_properties["user"]) \
    .option("password", redshift_properties["password"]) \
    .option("driver", redshift_properties["driver"]) \
    .mode("append") \
    .format("jdbc") \
    .save()

# Stop the Spark session
spark.stop()
