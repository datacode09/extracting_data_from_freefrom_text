from pyspark.sql.functions import col, from_json, to_json, struct, regexp_extract, regexp_replace, lit, when
from pyspark.sql.types import StringType, StructType, StructField

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

    # Step 1: Define a regex pattern to extract "Execution Date" from the "Metadata" field in the provided column
    execution_date_regex = r"(?i)<td>Execution Date</td><td>(.*?)</td>"
    
    # Step 2: Extract the full "Execution Date" information from the "Metadata" field
    df_with_execution_date = df.withColumn("execution_date_raw", regexp_extract(col(column_name), execution_date_regex, 1))
    
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
    
    # Step 6: Define the schema for the `additional_features` JSON field
    schema = StructType([
        StructField("lob", StringType(), True),
        StructField("Metadata", StringType(), True)  # Assuming Metadata is a string
    ])

    # Step 7: Parse the `additional_features` column as JSON
    df_parsed = df_with_final_execution_date.withColumn("parsed_json", from_json(col(column_name), schema))
    
    # Step 8: Add "execution_date" as a new field in the parsed JSON
    df_with_updated_json = df_parsed.withColumn(
        "parsed_json", 
        struct(
            col("parsed_json.lob"),                # Preserve the original 'lob' field
            col("parsed_json.Metadata"),           # Preserve the original 'Metadata' field
            col("execution_date")                  # Add the new 'execution_date' field
        )
    )
    
    # Step 9: Convert the updated JSON object back into a string and overwrite the original column
    df_final = df_with_updated_json.withColumn(column_name, to_json(col("parsed_json")))
    
    # Step 10: Drop intermediate columns
    df_final = df_final.drop("parsed_json", "execution_date", "execution_date_clean", "execution_date_raw")
    
    return df_final
