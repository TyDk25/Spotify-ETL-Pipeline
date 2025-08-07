import snowflake.connector


def s3_to_snowflake() -> str:
    """"This function copies data from an S3 bucket to a Snowflake table."""
    with snowflake.connector.connect(
    connection_name="connections"
    ) as conn: 
        curr = conn.cursor()

        curr.execute("USE WAREHOUSE COMPUTE_WH")
        curr.execute("USE DATABASE SPOTIFY_EXTRACTION") 
        curr.execute("USE SCHEMA SPOTIFY_EXTRACTION.SPOTIFY_EXTRACTION_SCHEMA")
        table_name = curr.execute(
            """
        SELECT table_name FROM INFORMATION_SCHEMA.TABLES
        WHERE table_schema = 'SPOTIFY_EXTRACTION_SCHEMA'
        """).fetchone()[0]
    
        curr.execute("""
        TRUNCATE TABLE SPOTIFY_EXTRACTION.SPOTIFY_EXTRACTION_SCHEMA.RECENTLY_PLAYED_TABLE 
            
                     """)
        


        curr.execute(f"""
                    COPY INTO SPOTIFY_EXTRACTION.SPOTIFY_EXTRACTION_SCHEMA.RECENTLY_PLAYED_TABLE FROM s3://dataset-spotify-extraction/dataset.csv
                    STORAGE_INTEGRATION = snowflake_access
                    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' 
                    SKIP_HEADER = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '0x22')
                    """
        )


    return f"Data copied to Snowflake table - {table_name} successfully."


