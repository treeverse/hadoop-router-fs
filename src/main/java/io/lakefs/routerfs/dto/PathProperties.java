package io.lakefs.routerfs.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import org.apache.hadoop.fs.Path;


@Value
@Builder
@AllArgsConstructor
public class PathProperties {
    Path path;
    String srcPrefix;
    String dstPrefix;
}
