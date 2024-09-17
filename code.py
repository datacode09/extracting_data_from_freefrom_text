from pyspark.sql.functions import col, regexp_extract, regexp_replace, concat, lit, when

# Step 1: Define a regex pattern to extract "Execution Date" from the "Metadata" field in "additional_features"
execution_date_regex = r"(?i)<td>Execution Date</td><td>(.*?)</td>"

# Step 2: Extract the full "Execution Date" information from the additional_features column
df_with_execution_date = df.withColumn("execution_date_raw", regexp_extract(col("additional_features"), execution_date_regex, 1))

# Step 3: Clean the execution_date_raw to remove HTML tags (e.g., <b> and </b>) using regexp_replace
df_with_cleaned_execution_date = df_with_execution_date.withColumn(
    "execution_date_clean", 
    regexp_replace(col("execution_date_raw"), r"<[^>]+>", "")  # Remove all HTML tags like <b>, </b>
)

# Step 4: Define a regex pattern to match valid datetime formats (dd/MM/yyyy, yyyy-MM-dd, etc.)
datetime_regex = r"\b(\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2})\b"

# Step 5: Extract valid datetime from the cleaned execution_date using the regex for datetime formats
df_with_final_execution_date = df_with_cleaned_execution_date.withColumn(
    "execution_date", 
    when(col("execution_date_clean").isNotNull() & (col("execution_date_clean") != ""), 
         regexp_extract(col("execution_date_clean"), datetime_regex, 0))  # Extract date only if the field is non-null/non-empty
    .otherwise(None)  # Set to None if no valid date is found
)

# Step 6: Now append the cleaned "Execution Date" into the "additional_features" column as part of the JSON string
df_with_updated_additional_features = df_with_final_execution_date.withColumn(
    "additional_features", 
    when(col("execution_date").isNotNull(),  # Only append if execution_date is not null
        concat(
            col("additional_features"), 
            lit(', "execution_date": "'), 
            col("execution_date"), 
            lit('"')
        )
    ).otherwise(col("additional_features"))  # Leave additional_features unchanged if execution_date is null
)

# Step 7: Drop the intermediate execution_date and execution_date_clean columns as they're no longer needed
df_final = df_with_updated_additional_features.drop("execution_date", "execution_date_clean", "execution_date_raw")

# Step 8: Show the final DataFrame
df_final.select("event_id", "additional_features").show(truncate=False)
