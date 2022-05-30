package io.lakefs.routerfs.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.apache.hadoop.fs.Path;


@Getter
@Builder
@AllArgsConstructor
public class PathProperties {
    Path path;
    String srcPrefix;
    String dstPrefix;
}
