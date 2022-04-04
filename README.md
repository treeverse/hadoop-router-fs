# hadoop-router-fs
hadoop-router-fs provides an Hadoop [FileSystem](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/FileSystem.html) implementation that transforms URIs and routes file system operations between Hadoop file systems according to the path URI in the operation.

RouterFS does a simple prefix replacement according to mapping configurations.

## Mapping configuration structure

Mapping configurations are Spark properties of the following form: 
`routerfs.mapping.${toFsScheme}.${mappingIdx}.${replace/with}=${path-prefix}`

Examples:
```properties 
routerfs.mapping.lakefs.1.replace=s3a://bucket/dir/ # mapping src
routerfs.mapping.lakefs.1.with=lakefs://example-repo/dev/ # mapping dst
routerfs.mapping.lakefs.2.replace=s3a://bucket/prefix # mapping src
routerfs.mapping.lakefs.2.with=lakefs://example-repo/dev/prefix # mapping dst
```
Make sure your source prefix ends with a slash when needed. 

#### Default mapping configuration

RouterFS requires a default mapping configuration, the default mapping configuration is also a Spark property of the form: 
`routerfs.mapping.${defaultFsScheme}.${replace/with}=${routerFS-fromFSScheme}`

Example:
```properties
fs.s3a.impl=RouterFileSystem
routerfs.mapping.s3a-default.replace='s3a://'
routerfs.mapping.s3a-default.with='s3a-default://'
```
The default mapping applies last in case routerFS didn't find a matching mapping to your path. RouterFS only supports 
a single default mapping configuration. In case that multiple default configurations are defined, one of them apply 
during the RouterFileSystem lifecycle.  
