package io.lakefs.routerfs.dto;

import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

@Value
@AllArgsConstructor
public class FileSystemPathTuple {
    FileSystem fileSystem;
    Path path;
}
