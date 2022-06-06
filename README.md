# hadoop-router-fs

[RouterFileSystem](src/main/java/io/lakefs/RouterFileSystem.java) is a Hadoop [FileSystem](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/FileSystem.html) 
implementation that transforms URIs at runtime according to provided configurations. It then routes file system operations to 
another Hadoop file system that executes it against the underlying object store. 

## Use-cases 

- Interact with multiple storages side-by-side, without making any changes to your code.    
- Migrate a collection to a new storage location without changing your Spark application code, or breaking it.

## Build instructions 

#### Pre-requisites 
- Install maven 

#### Steps

1. Clone the repo:
    ```shell
    git clone git@github.com:treeverse/hadoop-router-fs.git
    ```

2. Build with maven:
   ```shell
   mvn clean install
   ```

## How to configure RouterFS 

### Configure Spark to use RouterFS

You should instruct Spark to use RouterFS as the file system implementation for the URIs you would like to transform at runtime. To do that, 
add the following property to your Spark configurations: 
```properties
fs.${fromFsScheme}.impl=RouterFileSystem
```

For example, by adding the `fs.s3a.impl=RouterFileSystem` you are instructing Spark to use RouterFS as the file system for any 
URI with `scheme=s3a`.

### Add custom mapping configurations

RouterFS consumes your mapping configurations to understand which paths it needs to modify and how to modify them. It then 
performs a simple prefix replacement accordingly.  
Mapping configurations are Hadoop properties of the following form:
`routerfs.mapping.${fromFsScheme}.${mappingIdx}.(replace|with)=${path-prefix}`  
For a given URI, RouterFS scans the mapping configurations defined for the URI's scheme, searches for the first mapping
configuration that matches the URI prefix, and transforms the URI according to the matching configuration.

#### Notes about mapping configurations:
* Make sure your source prefix ends with a slash when needed.
* Mapping configurations apply in-order, and it is up to you to create non-conflicting configurations.


### Default file system

For each custom mapped scheme you should configure a default file system implementation in case no prefix matching will be found.  
Add the following configuration for the schemes you configured RouteFS to handle.
```properties
routerfs.default.fs.${fromFsScheme}=${the file system you used for this scheme without routerFS}
```
For example, by adding:
```properties
routerfs.default.fs.s3a=S3AFileSystem
```
You are instructing RouterFS to use `S3AFileSystem` for any URI with `scheme=s3a` for which RouterFS did not find
a mapping configuration.

### When no mapping was found

In case RouterFS can't find a matching mapping configuration, it will make sure that it's handled by the [default
file system](#default-file-system) for the URI scheme.

### Example

Given the following mapping configurations:
```properties 
routerfs.mapping.s3a.1.replace=s3a://bucket/dir1/ # mapping src
routerfs.mapping.s3a.1.with=lakefs://repo/main/ # mapping dst
routerfs.mapping.s3a.2.replace=s3a://bucket/dir2/ # mapping src
routerfs.mapping.s3a.2.with=lakefs://example-repo/dev/ # mapping dst
routerfs.default.fs.s3a=S3AFileSystem # default file system implementation for the `s3a` scheme
```

* For the URI `s3a://bucket/dir1/foo.parquet`, RouterFS will perform the next steps:
  1. Scan all `routerfs` mapping configurations that has the `s3a` scheme in their key: `routerfs.mapping.s3a.${mappingIndex}.replace`.
  2. Traverse over these configurations by the order of the priorities specified by `${mappingIdx}` and try to match a prefix of the given URI to the values of those configurations.
  3. When it will reach the `s3a://bucket/dir1/` prefix, RouterFS will replace it with the destination mapping value: `lakefs://repo/main/` to create the resulting URI: `lakefs://repo/main/foo.parquet`.


* For the URI `s3a://bucket/dir3/bar.parquet`, RouterFS will perform the next steps:
  1. As step 1 above.
  2. As step 2 above.
  3. When it will complete the scan and not find any matching prefixes, it will fall back to the [default file system](#default-file-system) implementation (`S3AFileSystem`) and will not amend any changes to the URI.

### Configure File Systems Implementations

The final configuration step is to instruct Spark what file system to use for each URI scheme. You should make sure that you 
add this configuration for any URI scheme you defined a mapping configuration for.
For example, to instruct Spark to use `S3AFileSystem` for any URI with `scheme=lakefs`
```properties
fs.lakefs.impl=S3AFileSystem
```

## Usage

### Run your Spark Application with RouterFS 

After [building](#build-instructions) RouterFS, the build artifact is a jar under the `target` directory. 
You should supply this jar to your Spark application when running the application, or by placing it under your `$SPARK_HOME/jars` directory. 

### Usage with lakeFS 

The current version of RouterFS only works for Spark applications that interact with lakeFS via the [S3 Gateway](https://docs.lakefs.io/integrations/spark.html#access-lakefs-using-the-s3a-gateway). 
That is, you can't use both RouterFS and LakeFSFileSystem together, but we have [concrete plans](https://github.com/treeverse/lakeFS/issues/3058) to make this work.

### `S3AFileSystem`

The current version of RouterFS requires the use of S3AFileSystem's [per-bucket configuration](https://hadoop.apache.org/docs/r2.8.0/hadoop-aws/tools/hadoop-aws/index.html#Configurations_different_S3_buckets) functionality to support multiple mappings that use 
S3AFileSystem as their file system implementation. That means that the compiled Hadoop version should be >= 2.8.0.  
The per-bucket configurations treat the first part of the path (also called the "authority") as the bucket to which we configure the S3A file system property.  
For example, for the following configurations:
```properties
routerfs.mapping.s3a.1.replace=s3a://bucket/dir/
routerfs.mapping.s3a.1.with=lakefs://repo/branch/
routerfs.default.fs.s3a=S3AFileSystem

fs.lakefs.impl=S3AFileSystem

# The following configs will be used when `lakefs://repo/...` will be addressed
fs.s3a.bucket.repo.endpoint=https://lakefs.service
fs.s3a.bucket.repo.access.key=...
fs.s3a.bucket.repo.secret.key=...
...
# The following configs will be used when any non-mapped s3a schemed URIs will be addressed
fs.s3a.endpoint=https://s3.us-east-1.amazonaws.com
fs.s3a.access.key=...
fs.s3a.secret.key=...
```
the configurations that begin with `fs.s3a.bucket.repo` will be used when trying to access `lakefs://repo/<path>`.  
All other `fs.s3a.<conf>` properties will be used for the general case.

### Working example

Please refer to the [sample app](./sample_app/README.md).
