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

#### Build instructions

1. Clone the repo with 
    ```shell
    git clone git@github.com:treeverse/hadoop-router-fs.git
    ```

2. Build with maven
```shell
mvn clean install
```

## How to configure RouterFS 

### Configure Spark to use RouterFS

You should tell Spark to use RouterFS as the file system for URIs you would like to transform at runtime. To do that, 
add the following Spark property to your Spark configurations: 
```properties
fs.${fromFsScheme}.impl=RouterFileSystem
```

For example, by adding the `fs.s3a.impl=RouterFileSystem` you are telling Spark to use RouterFS as the file system for any 
URI with `scheme=s3a`.

### Add custom Mapping Configurations

RouterFS consumes your mapping configurations to understand which paths it needs to modify and how to modify them. It then 
does a simple prefix replacement accordingly. 
Mapping configurations are Spark properties of the following form:
`routerfs.mapping.${fromFsScheme}.${mappingIdx}.${replace/with}=${path-prefix}`

Given a URI, RouterFS looks at the mapping configurations defined for the URI scheme, searches for the first mapping
configuration that matches the URI prefix, and transforms the URI according to the matching configuration.
For example, given the URI `s3a://bucket/dir1/foo.parquet`, and the following mapping configurations:
```properties 
routerfs.mapping.s3a.1.replace=s3a://bucket/dir1/ # mapping src
routerfs.mapping.s3a.1.with=lakefs://repo/main/ # mapping dst
routerfs.mapping.s3a.2.replace=s3a://bucket/dir2/ # mapping src
routerfs.mapping.s3a.2.with=lakefs://example-repo/dev/ # mapping dst
```
RouterFS will match the URI with the first mapping configuration defined for `s3a` scheme and transform it into
`lakefs://repo/main/foo.parquet`.

Notes about mapping configurations:
* Make sure your source prefix ends with a slash when needed.  
* Mapping configurations apply in-order, and it is up to you to create non-conflicting configurations. 

#### When no mapping was found

In case RouterFS can't find a matching mapping configuration, it will make sure that it's handled by the [default 
file system](#default-file-system) for the URI scheme. 

### Configure File Systems 

The final configuration step is to tell Spark what file system to use for each URI scheme. You should make sure that you 
add this configuration for any URI scheme you defined a mapping configuration for.
For example, to tell Spark to use `S3AFileSystem` for any URI with `scheme=s3a`
```properties
fs.lakefs.impl=S3AFileSystem
```

#### Default file system 

Add the following configuration for the scheme you [configured](#configure-spark-to-use-routerfs) RouteFS to handle.
```properties
routerfs.default.fs.${fromFsScheme}=${the file system you used for this scheme without routerFS}
```
For example, by adding:
```properties
routerfs.default.fs.s3a=S3AFileSystem
```
You are telling RouterFS to use `S3AFileSystem` for any URI with `scheme=s3a` for which RouterFS [did not find](#when-no-mapping-was-found)
a mapping configuration. 

At this point, RouterFS only supports a single default file system. 

## Usage

### Run your Spark Application with RouterFS 

After [building](#build-instructions) RouterFS, the build artifact is a jar under the target directory. 
You should supply its jar to your Spark application runtime. 

## Usage with lakeFS 

The current version of RouterFS only works for Spark applications that interact with lakeFS via the [S3 Gateway](https://docs.lakefs.io/integrations/spark.html#access-lakefs-using-the-s3a-gateway). 
That is, you can't use both RouterFS and LakeFSFileSystem together, but we have [concrete plans](https://github.com/treeverse/lakeFS/issues/3058) to make this work.
