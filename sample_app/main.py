from spark_client import SparkClient
import os

def main():
    spark_client = SparkClient()
    spark_client.load_dataset('./housing.csv')
    temp_table_name = "californiahousing"
    spark_client.dataset.registerTempTable(temp_table_name)
    output =  spark_client.spark.sql(f"SELECT * from {temp_table_name}")
    output.show()

    s3a_replace_prefix = os.environ.get('S3A_REPLACE_PREFIX', default='s3a://bucket/dir/')
    s3_bucket_s3a_prefix = os.environ.get('S3_BUCKET_S3A_PREFIX', default='s3a://mybucket/router-bucket/')

    output.write.parquet(f"{s3a_replace_prefix}writing-to-lakefs")
    output.write.parquet(f"{s3_bucket_s3a_prefix}writing-directly-to-s3")

if __name__ == "__main__":
    main()