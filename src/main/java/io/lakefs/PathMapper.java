package io.lakefs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A class that does path mapping between configured pairs of path prefixes, and matches the appropriate path mapping for
 * a converted path.
 */
public class PathMapper {

    public static final Logger LOG = LoggerFactory.getLogger(PathMapper.class);

    protected static final String MAPPING_CONFIG_PREFIX = "routerfs.mapping.";
    protected static final String NON_DEFAULT_MAPPING_CONFIG_PATTERN = "^routerfs\\.mapping\\.(?<mappingScheme>[-a-z0-9_]*)\\.(?<mappingPriority>\\d*)\\.(?<mappingType>replace|with)";
    protected static final String MAPPING_SCHEME_REGEX_GROUP_NAME = "mappingScheme";
    protected static final String MAPPING_PRIORITY_REGEX_GROUP_NAME = "mappingPriority";
    protected static final String MAPPING_TYPE_REGEX_GROUP_NAME = "mappingType";
    protected static final String DEFAULT_MAPPING_CONFIG_PATTERN = "^routerfs\\.mapping\\.(.*default)\\.(replace|with)";
    protected static final int DEFAULT_TO_FS_IDX = 1;
    protected static final int DEFAULT_MAPPING_TYPE_IDX = 2;

    enum MappingConfigType {
        REPLACE,
        WITH
    }

    private List<PathMapping> pathMappings;
    private PathMapping defaultMapping;

    public PathMapper(Configuration conf) throws IOException {
        this.pathMappings = new ArrayList<>();
        loadMappingConfig(conf);
        if (defaultMapping == null) {
            throw new IllegalArgumentException("Missing default mapping configuration, cannot initialize path mapper");
        }
    }

    private void loadMappingConfig(Configuration conf) throws InvalidPropertiesFormatException {
        List<MappingConfig> mappingConfigurations = parseMappingConfig(conf);
        populatePathMappings(mappingConfigurations);

        if (LOG.isDebugEnabled()) {
            logLoadedMappings();
        }
    }

    /**
     * Pair mapping configuration that form a {@link PathMapping}, and populate the pathMapping list.
     * Two mapping configuration are paired together in case they share the same toFsScheme and mappingIdx, but have
     * different {@link MappingConfigType}.
     * e.g. the following two mapping configuration for a path mapping:
     * routerfs.mapping.lakefs.1.replace='^s3a://bucket/prefix'
     * routerfs.mapping.lakefs.1.with='lakefs://example-repo/dev/prefix'
     *
     * @param mappingConfiguration the configurations to create path mapping from
     */
    private void populatePathMappings(List<MappingConfig> mappingConfiguration) {
        List<MappingConfig> srcConfigs = mappingConfiguration.stream()
                .filter(mc -> mc.getType() == MappingConfigType.REPLACE).collect(Collectors.toList());

        for (MappingConfig srcConf : srcConfigs) {
            Optional<MappingConfig> matchingDstConf = mappingConfiguration.stream()
                    .filter(mc -> mc.getToScheme().equals(srcConf.getToScheme()) && mc.getIndex() == srcConf.getIndex()
                            && mc.getType() == MappingConfigType.WITH).findFirst();
            if (!matchingDstConf.isPresent()) {
                LOG.warn("Missing a mapping configuration, expected to find mapping named %s.%d.%s.",
                        srcConf.getToScheme(), srcConf.getIndex(), MappingConfigType.WITH.name());
                continue;
            }
            PathMapping pathMapping = new PathMapping(srcConf, matchingDstConf.get());
            if (srcConf.isDefault()) {
                defaultMapping = pathMapping;
            } else {
                pathMappings.add(pathMapping);
            }
        }
        sortPathMappingsBySchemeAndIdx();
    }

    /**
     * Parses {@link RouterFileSystem} mapping configuration.
     */
    private List<MappingConfig> parseMappingConfig(Configuration conf) throws InvalidPropertiesFormatException {
        List<MappingConfig> mappingConfig = new ArrayList<>();
        for (Map.Entry<String, String> hadoopConf : conf) {
            if (hadoopConf.getKey().startsWith(MAPPING_CONFIG_PREFIX)) {
                MappingConfig mappingConf = parseMappingConf(hadoopConf);
                mappingConfig.add(mappingConf);
                LOG.trace("Loaded and parsed mapping config with key:%s and value:%s", hadoopConf.getKey(), hadoopConf.getValue());
            }
        }
        return mappingConfig;
    }

    /**
     * A method for troubleshooting purposes.
     */
    private void logLoadedMappings() {
        LOG.debug("pathMappings: ");
        for (PathMapping pm: pathMappings) {
            LOG.debug(pm.toString());
        }
        if (defaultMapping != null) {
            LOG.debug("defaultMapping: " + defaultMapping.toString());
        }
    }

    /**
     * Sort the loaded path mappings by scheme and then idx. This is required by the path mapper because
     * mapping configurations are applied in-order.
     */
    private void sortPathMappingsBySchemeAndIdx() {
        Comparator<PathMapping> bySchemeAndIndex = Comparator
                .comparing(PathMapping::getToScheme)
                .thenComparing(PathMapping::getIndex);

        pathMappings = pathMappings.stream()
                .sorted(bySchemeAndIndex)
                .collect(Collectors.toList());
    }

    /**
     * Parses hadoop configurations of the following formats:
     * routerfs.mapping.${toFsScheme}.${mappingIdx}.{replace/with}=value and
     * routerfs.mapping.${defaultToFsScheme}.{replace/with}=value into a {@link MappingConfig}.
     *
     * @param hadoopConf the config to parse
     * @return parsed configuration
     */
    private MappingConfig parseMappingConf(Map.Entry<String, String> hadoopConf) throws InvalidPropertiesFormatException {
        String toFSScheme;
        int mappingIdx = 0;
        MappingConfigType type;
        boolean isDefaultMapping = false;
        String key = hadoopConf.getKey();

        Pattern mappingConfPattern = Pattern.compile(NON_DEFAULT_MAPPING_CONFIG_PATTERN);
        Matcher nonDefaultMatcher = mappingConfPattern.matcher(key);
        boolean nonDefaultMapping = nonDefaultMatcher.find();
        if (nonDefaultMapping) {
            toFSScheme = nonDefaultMatcher.group(MAPPING_SCHEME_REGEX_GROUP_NAME);
            mappingIdx = Integer.parseInt(nonDefaultMatcher.group(MAPPING_PRIORITY_REGEX_GROUP_NAME));
            type = "replace".equals(nonDefaultMatcher.group(MAPPING_TYPE_REGEX_GROUP_NAME))? MappingConfigType.REPLACE :
                    MappingConfigType.WITH;
        } else {
            Pattern defaultPattern = Pattern.compile(DEFAULT_MAPPING_CONFIG_PATTERN);
            Matcher defaultMatcher = defaultPattern.matcher(key);
            boolean defaultMapping = defaultMatcher.find();
            if (!defaultMapping) {
                throw new InvalidPropertiesFormatException("Invalid mapping configuration name " + key);
            }
            isDefaultMapping = true;
            toFSScheme = defaultMatcher.group(DEFAULT_TO_FS_IDX);
            type = "replace".equals(defaultMatcher.group(DEFAULT_MAPPING_TYPE_IDX))? MappingConfigType.REPLACE :
                    MappingConfigType.WITH;
        }
        return new MappingConfig(type , mappingIdx, toFSScheme, hadoopConf.getValue(), isDefaultMapping);
    }

    /**
     * Map path into its desired URI form based on its matching mapping configuration. In case the path does not match
     * any mapping configuration, uses the default mapping.
     *
     * @param origPath the path to map
     * @return a mapped path
     */
    public Path mapPath(Path origPath) {
        PathMapping pathMapping = findAppropriatePathMapping(origPath);
        if (pathMapping == null) {
            LOG.trace("Can't find a matching path mapping for %s, using default mapping", origPath);
            pathMapping = defaultMapping;
        }
        return convertPath(origPath, pathMapping);
    }

    /**
     * Convert path by replacing its prefix with the dst prefix.
     *
     * @param path path to convert
     * @param pathMapping the path mapping that matches the path and according to which the path is converted
     * @return a converted path
     */
    private Path convertPath(Path path, PathMapping pathMapping) {
        String str = path.toString();
        String convertedPath = str.replaceFirst(pathMapping.getSrcPrefix().getValue(), pathMapping.getDstPrefix().getValue());
        LOG.trace("Converted % to % using path mapping %s", path, convertedPath,  pathMapping);
        return new Path(convertedPath);
    }

    private PathMapping findAppropriatePathMapping(Path path) {
        Optional<PathMapping> appropriateMap = pathMappings.stream()
                .filter(pm -> pm.isAppropriateMapping(path)).findFirst();
        return appropriateMap.isPresent() ? appropriateMap.get() : null;
    }

    /**
     * PathMapping represents a pair of mapping configurations, that define source and destination prefixes to
     * be replaced.
     */
    private static class PathMapping {

        private MappingConfig srcPrefix;
        private MappingConfig dstPrefix;
        private int index;
        private String toScheme;

        public PathMapping(MappingConfig srcPrefix, MappingConfig dstPrefix) {
            this.srcPrefix = srcPrefix;
            this.dstPrefix = dstPrefix;
            if (!srcPrefix.getToScheme().equals(dstPrefix.getToScheme())) {
                LOG.error("src and dst schemes must match, cannot create PathMapping. src:"
                        + srcPrefix.getToScheme() + " dst: " + dstPrefix.getToScheme());
            }
            this.toScheme = srcPrefix.getToScheme();
            if (srcPrefix.getIndex() != dstPrefix.getIndex()) {
                LOG.error("src and dst indices must match, cannot create PathMapping. src:"
                        + srcPrefix.getIndex() + " dst: " + dstPrefix.getIndex());
            }
            this.index = srcPrefix.getIndex();
        }

        /**
         * Checks if this path mapping is appropriate for a {@link Path}.
         * A path mapping considered appropriate for a path if the path starts with the path mapping's source prefix.
         *
         * @param p the path to check whether a mapping is appropriate for
         * @return true if an appropriate, false otherwise
         */
        public boolean isAppropriateMapping(Path p) {
            String str = p.toString();
            return str.startsWith(srcPrefix.getValue());
        }

        public MappingConfig getSrcPrefix() {
            return srcPrefix;
        }

        public MappingConfig getDstPrefix() {
            return dstPrefix;
        }

        public String getToScheme() {
            return toScheme;
        }

        public int getIndex() {
            return index;
        }

        @Override
        public String toString() {
            return "srcPrefix=" + this.srcPrefix + " dstPrefix=" + this.dstPrefix;
        }
    }


    /**
     * A class that represents a parsed Path mapping configurations supported by RouterFileSystem.
     * Mapping configuration form is: routerfs.mapping.${toFsScheme}.${mappingIdx}.${replace/with}=value.
     * Default mapping form is: routerfs.mapping.${defaultToFsScheme}.{replace/with}=value
     */
    private static class MappingConfig {
        private MappingConfigType type;
        private int index;
        private String toScheme;
        private String value;
        private boolean isDefault;

        public MappingConfig(MappingConfigType type, int index, String toScheme, String value, boolean isDefault) {
            this.type = type;
            this.index = index;
            this.toScheme = toScheme;
            this.value = value;
            this.isDefault = isDefault;
        }

        @Override
        public String toString() {
            return "toScheme=" + this.toScheme + " type=" + this.type.name() + " index=" + this.index + " value=" +
                    this.value;
        }

        public String getToScheme() {
            return toScheme;
        }

        public int getIndex() {
            return index;
        }

        public MappingConfigType getType() {
            return type;
        }

        public String getValue() {
            return value;
        }

        public boolean isDefault() {
            return isDefault;
        }
    }
}