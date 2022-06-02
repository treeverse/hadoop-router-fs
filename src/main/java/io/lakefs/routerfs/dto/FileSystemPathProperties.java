package io.lakefs.routerfs.dto;

import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.hadoop.fs.FileSystem;

@Value
@AllArgsConstructor
public class FileSystemPathProperties {
    FileSystem fileSystem;
    PathProperties pathProperties;
}
