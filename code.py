from pyspark.sql.functions import col, regexp_extract, concat, lit

# Assuming 'df' is your DataFrame with the original schema
# Define a regex pattern to extract "Execution Date"
execution_date_regex = r"(?i)<td>Execution Date</td><td>(.*?)</td>"

# Extract the "Execution Date" using regexp_extract
df_with_execution_date = df.withColumn("execution_date", regexp_extract(col("additional_features"), execution_date_regex, 1))

# Handle cases where no "Execution Date" is found (set to None if not found)
df_with_execution_date = df_with_execution_date.withColumn(
    "execution_date", 
    col("execution_date").when(col("execution_date") == "", None).otherwise(col("execution_date"))
)

# Now append the "Execution Date" into the "additional_features" column as part of the JSON string
df_with_updated_additional_features = df_with_execution_date.withColumn(
    "additional_features", 
    concat(
        col("additional_features"), 
        lit(', "execution_date": "'), 
        col("execution_date"), 
        lit('"')
    )
)

# Show the updated DataFrame
df_with_updated_additional_features.select("event_id", "additional_features").show(truncate=False)
