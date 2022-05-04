package io.lakefs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
    protected static final String URI_SCHEME_SEPARATOR = "://";


    enum MappingConfigType {
        REPLACE,
        WITH
    }

    private List<PathMapping> pathMappings;
    private PathMapping defaultMapping;

    public PathMapper(Configuration conf, String defaultFromScheme, String defaultToScheme) throws IOException {
        Objects.requireNonNull(defaultFromScheme);
        Objects.requireNonNull(defaultToScheme);
        this.pathMappings = new ArrayList<>();
        defaultMapping = createDefaultMapping(defaultFromScheme, defaultToScheme);
        loadMappingConfig(conf);
    }

    private PathMapping createDefaultMapping(String defaultFromScheme, String defaultToScheme) {
        MappingConfig srcMapping = new MappingConfig(MappingConfigType.REPLACE, null, defaultFromScheme, defaultFromScheme + URI_SCHEME_SEPARATOR);
        MappingConfig dstMapping = new MappingConfig(MappingConfigType.WITH, null, defaultToScheme, defaultToScheme + URI_SCHEME_SEPARATOR);
        return new PathMapping(srcMapping, dstMapping, true);
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
     * Two mapping configuration are paired together in case they share the same fromFsScheme and mappingIdx, but have
     * different {@link MappingConfigType}.
     * e.g. the following two mapping configuration for a path mapping:
     * routerfs.mapping.s3a.1.replace='s3a://bucket/'
     * routerfs.mapping.s3a.1.with='lakefs://example-repo/dev/'
     *
     * @param mappingConfiguration the configurations to create path mapping from
     */
    private void populatePathMappings(List<MappingConfig> mappingConfiguration) {
        List<MappingConfig> srcConfigs = mappingConfiguration.stream()
                .filter(mc -> mc.getType() == MappingConfigType.REPLACE).collect(Collectors.toList());

        for (MappingConfig srcConf : srcConfigs) {
            Optional<MappingConfig> matchingDstConf = mappingConfiguration.stream()
                    .filter(mc -> mc.getFromScheme().equals(srcConf.getFromScheme()) && mc.getPriority() == srcConf.getPriority()
                            && mc.getType() == MappingConfigType.WITH).findFirst();
            if (!matchingDstConf.isPresent()) {
                LOG.warn("Missing a mapping configuration, expected to find mapping named %s.%d.%s.",
                        srcConf.getFromScheme(), srcConf.getPriority(), MappingConfigType.WITH.name());
                continue;
            }
            PathMapping pathMapping = new PathMapping(srcConf, matchingDstConf.get());
            pathMappings.add(pathMapping);
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
                .comparing(PathMapping::getFromScheme)
                .thenComparing(PathMapping::getPriority);

        pathMappings = pathMappings.stream()
                .sorted(bySchemeAndIndex)
                .collect(Collectors.toList());
    }

    /**
     * Parses hadoop configurations of the following form:
     * routerfs.mapping.${fromFsScheme}.${mappingIdx}.{replace/with}=value and
     *
     * @param hadoopConf the config to parse
     * @return parsed configuration
     */
    private MappingConfig parseMappingConf(Map.Entry<String, String> hadoopConf) throws InvalidPropertiesFormatException {
        String fromFSScheme;
        int mappingPriority = 0;
        MappingConfigType type;
        String key = hadoopConf.getKey();

        Pattern pattern = Pattern.compile(NON_DEFAULT_MAPPING_CONFIG_PATTERN);
        Matcher matcher = pattern.matcher(key);
        if (matcher.find()) {
            fromFSScheme = matcher.group(MAPPING_SCHEME_REGEX_GROUP_NAME);
            mappingPriority = Integer.parseInt(matcher.group(MAPPING_PRIORITY_REGEX_GROUP_NAME));
            type = "replace".equals(matcher.group(MAPPING_TYPE_REGEX_GROUP_NAME))? MappingConfigType.REPLACE :
                    MappingConfigType.WITH;
            if (type == MappingConfigType.REPLACE && !hadoopConf.getValue().startsWith(fromFSScheme)) {
                throw new InvalidPropertiesFormatException("Invalid hadoop conf: " + hadoopConf + " mapping src value " +
                        "should match its fromScheme");
            }
            return new MappingConfig(type , mappingPriority, fromFSScheme, hadoopConf.getValue());
        }
        throw new InvalidPropertiesFormatException("Error parsing mapping config: " + hadoopConf);
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
        private int priority;
        private String fromScheme;
        private boolean defaultMapping;

        public PathMapping(MappingConfig srcPrefix, MappingConfig dstPrefix) {
            this(srcPrefix, dstPrefix, false);
        }

        public PathMapping(MappingConfig srcPrefix, MappingConfig dstPrefix, boolean defaultMapping) {
            this.defaultMapping = defaultMapping;
            this.srcPrefix = srcPrefix;
            this.dstPrefix = dstPrefix;
            if (!srcPrefix.getFromScheme().equals(dstPrefix.getFromScheme())) {
                LOG.error("src and dst schemes must match, cannot create PathMapping. src:"
                        + srcPrefix.getFromScheme() + " dst: " + dstPrefix.getFromScheme());
            }
            this.fromScheme = srcPrefix.getFromScheme();
            if (!this.defaultMapping) {
                if (srcPrefix.getPriority() != dstPrefix.getPriority()) {
                    LOG.error("src and dst indices must match, cannot create PathMapping. src:"
                            + srcPrefix.getPriority() + " dst: " + dstPrefix.getPriority());
                }
                this.priority = srcPrefix.getPriority();
            }
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

        public String getFromScheme() {
            return fromScheme;
        }

        public int getPriority() {
            return priority;
        }

        @Override
        public String toString() {
            return "srcPrefix=" + this.srcPrefix + " dstPrefix=" + this.dstPrefix;
        }
    }

    /**
     * A class that represents a parsed Path mapping configurations supported by RouterFileSystem.
     * Mapping configuration form is: routerfs.mapping.${fromFsScheme}.${mappingIdx}.${replace/with}=value.
     */
    private static class MappingConfig {
        private MappingConfigType type;
        private Integer priority;
        private String fromScheme;
        private String value;

        public MappingConfig(@Nonnull MappingConfigType type, @Nullable Integer priority, @Nonnull String fromScheme,
                             @Nonnull String value) {
            this.type = type;
            this.priority = priority;
            this.fromScheme = fromScheme;
            this.value = value;
        }

        @Override
        public String toString() {
            return "toScheme=" + this.fromScheme + " type=" + this.type.name() + " index=" + this.priority + " value=" +
                    this.value;
        }

        public String getFromScheme() {
            return fromScheme;
        }

        public int getPriority() {
            return priority;
        }

        public MappingConfigType getType() {
            return type;
        }

        public String getValue() {
            return value;
        }
    }
}