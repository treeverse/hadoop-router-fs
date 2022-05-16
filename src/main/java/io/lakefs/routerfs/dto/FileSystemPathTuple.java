package io.lakefs.routerfs.dto;

import lombok.Getter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

@Getter
public class FileSystemPathTuple {
    private final FileSystem fileSystem;
    private final Path path;

    public FileSystemPathTuple(FileSystem fileSystem, Path path) {
        this.fileSystem = fileSystem;
        this.path = path;
    }
}
