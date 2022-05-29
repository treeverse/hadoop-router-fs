package io.lakefs.routerfs.dto;

import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor
public class DefaultSchemeTranslation {
    String translateFromScheme;
    String translateToScheme;
}
