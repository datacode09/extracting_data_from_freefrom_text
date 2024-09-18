def extract_and_append_execution_date(df, column_name="additional_features"):
    """
    Extracts the 'Execution Date' from a JSON-like field in the provided column and appends the cleaned date 
    into the same column as part of the JSON. If no date is found, 'execution_date': null will be added.
    Intermediate columns are removed after processing.

    Parameters:
    df (DataFrame): The input Spark DataFrame containing the column with metadata.
    column_name (str): The name of the column containing the JSON-like metadata (default: 'additional_features').

    Returns:
    DataFrame: A new DataFrame with the 'execution_date' appended to the JSON string in the specified column.
    """
    
    # Step 1: Define a regex pattern to extract "Execution Date" from the "Metadata" field in the provided column
    execution_date_regex = r"(?i)<td>Execution Date</td><td>(.*?)</td>"
    
    # Step 2: Extract the full "Execution Date" information from the provided column
    df_with_execution_date = df.withColumn("execution_date_raw", F.regexp_extract(F.col(column_name), execution_date_regex, 1))
    
    # Step 3: Clean the execution_date_raw to remove HTML tags (e.g., <b> and </b>) using regexp_replace
    df_with_cleaned_execution_date = df_with_execution_date.withColumn(
        "execution_date_clean", 
        F.regexp_replace(F.col("execution_date_raw"), r"<[^>]+>", "")  # Remove all HTML tags like <b>, </b>
    )
    
    # Step 4: Define a regex pattern to match valid datetime formats (dd/MM/yyyy, yyyy-MM-dd, etc.)
    datetime_regex = r"\b(\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2})\b"
    
    # Step 5: Extract valid datetime from the cleaned execution_date using the regex for datetime formats
    df_with_final_execution_date = df_with_cleaned_execution_date.withColumn(
        "execution_date", 
        F.when(F.col("execution_date_clean").isNotNull() & (F.col("execution_date_clean") != ""), 
               F.regexp_extract(F.col("execution_date_clean"), datetime_regex, 0))  # Extract date only if the field is non-null/non-empty
        .otherwise(None)  # Set to None if no valid date is found
    )
    
    # Step 6: Now append the cleaned "Execution Date" into the provided column as part of the JSON string
    # Ensure that even if the execution_date is null, we append it as "execution_date": null
    df_with_updated_additional_features = df_with_final_execution_date.withColumn(
        column_name, 
        F.concat(
            F.col(column_name), 
            F.lit(', "execution_date": '), 
            F.when(F.col("execution_date").isNotNull(), F.concat(F.lit('"'), F.col("execution_date"), F.lit('"')))
             .otherwise(F.lit("null"))
        )
    )
    
    # Step 7: Drop the intermediate execution_date and execution_date_clean columns as they're no longer needed
    df_final = df_with_updated_additional_features.drop("execution_date", "execution_date_clean", "execution_date_raw")
    
    # Return the final DataFrame
    return df_final

# Assuming your DataFrame is named 'df' and the column is 'additional_features'
df_final = extract_and_append_execution_date(df, "additional_features")

# Show the final DataFrame
df_final.select("event_id", "additional_features").show(truncate=False)
