# Sample app - RouterFS + lakeFS

The app will be used to write a Parquet file to two separate servers: S3 bucket and lakeFS server (using S3 gateway).

## Running the app

### Pre-requisites

1. Before running the app, make sure you placed the `hadoop-router-fs-0.1.0-jar-with-dependencies.jar` jar file you [built](../README.md#build-instructions) under your `$SPARK_HOME/jars` directory.
2. `cd sample_app`.
3. Run `pip install -r requirements.txt`.

### Configurations

#### [spark_client.py](spark_client.py)

1. Configure your `lakefs_` and `aws_` properties to include correct information. Alternatively, set the `LAKEFS_` and `AWS_` environment variables as specified in the code.
2. Set (or don't) the `repo_name`, `branch_name` and `path` variables (make sure that if `path` is set, it ends with a `/`).
3. Set (or don't) the `replace_prefix` variable to reflect the mapped prefix. Alternatively, set the `S3A_REPLACE_PREFIX` environment variable as specified in the code. 

#### [main.py](main.py)

1. Set the `s3a_replace_prefix` variable to reflect the mapped prefix. Make sure this is the same value of `replace_prefix` under the [spark_client](spark_client.py) file. Alternatively, set the `S3A_REPLACE_PREFIX` environment variable as specified in the code.
2. Set the `s3_bucket_s3a_prefix` variable to reflect the S3 bucket namespace to which the Parquet file will be written. **This should be a valid and accessible S3 bucket prefix**.

### Run

`spark-submit --packages "org.apache.hadoop:hadoop-aws:<you.hadoop.version>" main.py`

## Result

After running the app you should notice that the same Parquet file was written to two different locations using a single mapping scheme.