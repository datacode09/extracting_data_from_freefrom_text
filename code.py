from pyspark.sql.functions import col, regexp_extract, concat, lit, when

# Step 1: Define a regex pattern to extract "Execution Date" from the "Metadata" field in "additional_features"
execution_date_regex = r"(?i)<td>Execution Date</td><td>(.*?)</td>"

# Step 2: Extract the full "Execution Date" information from the additional_features column
df_with_execution_date = df.withColumn("execution_date_raw", regexp_extract(col("additional_features"), execution_date_regex, 1))

# Step 3: Define a general regex pattern to match valid datetime formats (dd/MM/yyyy, yyyy-MM-dd, etc.)
datetime_regex = r"\b(\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2})\b"

# Step 4: Extract the cleaned datetime info from the raw execution date using the regex pattern
df_with_clean_execution_date = df_with_execution_date.withColumn(
    "execution_date", 
    regexp_extract(col("execution_date_raw"), datetime_regex, 0)
)

# Step 5: Handle cases where no "Execution Date" is found (set to None if not found)
df_with_clean_execution_date = df_with_clean_execution_date.withColumn(
    "execution_date", 
    when(col("execution_date") == "", None).otherwise(col("execution_date"))
)

# Step 6: Now append the cleaned "Execution Date" into the "additional_features" column as part of the JSON string
df_with_updated_additional_features = df_with_clean_execution_date.withColumn(
    "additional_features", 
    concat(
        col("additional_features"), 
        lit(', "execution_date": "'), 
        col("execution_date"), 
        lit('"')
    )
)

# Step 7: Show the updated DataFrame with the cleaned "Execution Date" appended
df_with_updated_additional_features.select("event_id", "additional_features", "execution_date").show(truncate=False)
