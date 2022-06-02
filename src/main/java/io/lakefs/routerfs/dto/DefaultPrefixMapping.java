package io.lakefs.routerfs.dto;

import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor
public class DefaultPrefixMapping {
    String fromScheme;
    String toScheme;
}
