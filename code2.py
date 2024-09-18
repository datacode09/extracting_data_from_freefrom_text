from pyspark.sql.functions import col, from_json, to_json, struct, regexp_extract, regexp_replace, lit, when
from pyspark.sql.types import StringType, StructType, StructField, TimestampType

def extract_and_append_execution_date(df, column_name="additional_features"):
    """
    Extracts the 'Execution Date' from the 'Metadata' field inside the provided column (e.g., 'additional_features'),
    appends the cleaned 'execution_date' as a new field in the JSON object, and serializes it back into the column.

    Parameters:
    df (DataFrame): The input Spark DataFrame containing the column with metadata.
    column_name (str): The name of the column containing the JSON-like metadata (default: 'additional_features').

    Returns:
    DataFrame: A new DataFrame with the 'execution_date' appended as a separate field in the JSON string.
    """

    # Define the schema for the additional_features JSON field based on the fields provided in the image
    schema = StructType([
        StructField("lob", StringType(), True),
        StructField("touch_stop_timestamp", TimestampType(), True),
        StructField("employee_number", StringType(), True),
        StructField("client_number", StringType(), True),
        StructField("eventName", StringType(), True),
        StructField("pyConfirmationNote", StringType(), True),
        StructField("processing_time_seconds", StringType(), True),
        StructField("BacklogStatus", StringType(), True),
        StructField("TATind", StringType(), True),
        StructField("Marker", StringType(), True),
        StructField("emerald_start_index", StringType(), True),
        StructField("emerald_start_date", StringType(), True),
        StructField("CSC", StringType(), True),
        StructField("CSC_bus_day_equiv", StringType(), True),
        StructField("Comment", StringType(), True),
        StructField("DeadlineDateTime", StringType(), True),
        StructField("GoalDateTime", StringType(), True),
        StructField("RejectReason", StringType(), True),
        StructField("SuspendReason", StringType(), True),
        StructField("ValidSkill", StringType(), True),
        StructField("Metadata", StringType(), True),  # Assuming Metadata is a string, and contains the Execution Date
        StructField("pxUrgencyWork", StringType(), True)
    ])

    # Step 1: Parse the `additional_features` column as JSON
    df_parsed = df.withColumn("parsed_json", from_json(col(column_name), schema))
    
    # Step 2: Extract "Execution Date" from the "Metadata" field using regex
    execution_date_regex = r"(?i)<td>Execution Date</td><td>(.*?)</td>"
    df_with_execution_date = df_parsed.withColumn("execution_date_raw", regexp_extract(col("parsed_json.Metadata"), execution_date_regex, 1))
    
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
    
    # Step 6: Add "execution_date" as a new field in the parsed JSON
    df_with_updated_json = df_with_final_execution_date.withColumn(
        "parsed_json", 
        struct(
            col("parsed_json.lob"),                # Preserve the original 'lob' field
            col("parsed_json.touch_stop_timestamp"), # Preserve the original 'touch_stop_timestamp'
            col("parsed_json.employee_number"),
            col("parsed_json.client_number"),
            col("parsed_json.eventName"),
            col("parsed_json.pyConfirmationNote"),
            col("parsed_json.processing_time_seconds"),
            col("parsed_json.BacklogStatus"),
            col("parsed_json.TATind"),
            col("parsed_json.Marker"),
            col("parsed_json.emerald_start_index"),
            col("parsed_json.emerald_start_date"),
            col("parsed_json.CSC"),
            col("parsed_json.CSC_bus_day_equiv"),
            col("parsed_json.Comment"),
            col("parsed_json.DeadlineDateTime"),
            col("parsed_json.GoalDateTime"),
            col("parsed_json.RejectReason"),
            col("parsed_json.SuspendReason"),
            col("parsed_json.ValidSkill"),
            col("parsed_json.Metadata"),           # Preserve the original 'Metadata' field
            col("parsed_json.pxUrgencyWork"),      # Preserve the original 'pxUrgencyWork'
            col("execution_date")                  # Add the new 'execution_date' field
        )
    )
    
    # Step 7: Convert the updated JSON object back into a string and overwrite the original column
    df_final = df_with_updated_json.withColumn(column_name, to_json(col("parsed_json")))
    
    # Step 8: Drop intermediate columns
    df_final = df_final.drop("parsed_json", "execution_date", "execution_date_clean", "execution_date_raw")
    
    return df_final
