from pyspark.sql import SparkSession
import os

class SparkClient:
    def __init__(self):
        self.spark = SparkSession.builder \
            .master("local[*]") \
            .appName("Try RouterFS") \
            .getOrCreate()
        self.dataset = None

        # self.spark.sparkContext.setLogLevel('DEBUG')

        ## Supply your lakeFS endpoint, access key and secret using the environment variables or by changing the following default values
        lakefs_endpoint = os.environ.get('LAKECTL_SERVER_ENDPOINT_URL', default='http://127.0.0.1:8000')
        lakefs_key = os.getenv('LAKEFS_ACCESS_KEY', default='')
        lakefs_secret = os.environ.get('LAKEFS_SECRET_KEY', default='')

        ## Supply your AWS S3 endpoint, access key and secret using the environment variables or by changing the following default values
        aws_s3_endpoint = os.environ.get('S3_ENDPOINT', default='https://s3.us-east-1.amazonaws.com')
        aws_key = os.getenv('AWS_ACCESS_KEY', default='')
        aws_secret = os.environ.get('AWS_SECRET_KEY', default='')

        repo_name = 'router'
        branch_name = 'main'
        path = ''

        self.spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "io.lakefs.routerfs.RouterFileSystem")

        self.spark._jsc.hadoopConfiguration().set(f"fs.s3a.bucket.{repo_name}.path.style.access", "true")
        self.spark._jsc.hadoopConfiguration().set(f"fs.s3a.bucket.{repo_name}.endpoint", lakefs_endpoint)
        self.spark._jsc.hadoopConfiguration().set(f"fs.s3a.bucket.{repo_name}.access.key", lakefs_key)
        self.spark._jsc.hadoopConfiguration().set(f"fs.s3a.bucket.{repo_name}.secret.key", lakefs_secret)


        ## Change this property to the prefix you want to map to the lakeFS prefix
        replace_prefix = os.environ.get('S3A_REPLACE_PREFIX', default='s3a://bucket/dir/')

        self.spark._jsc.hadoopConfiguration().set("routerfs.mapping.s3a.1.replace", replace_prefix)
        self.spark._jsc.hadoopConfiguration().set("routerfs.mapping.s3a.1.with", f"lakefs://{repo_name}/{branch_name}/{path}")
        self.spark._jsc.hadoopConfiguration().set("routerfs.default.fs.s3a", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        self.spark._jsc.hadoopConfiguration().set("fs.lakefs.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        self.spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", aws_s3_endpoint)
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_key)
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret)


    def load_dataset(self, dataset_file_path):
        self.dataset = self.spark.read.csv(dataset_file_path, header=True, sep=",").cache()
        self.dataset.show()
