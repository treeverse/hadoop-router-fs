package io.lakefs.routerfs;

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
    protected static final int CONFIG_PAIR = 2;



    enum MappingConfigType {
        SOURCE,
        DEST
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
        MappingConfig srcMapping = new MappingConfig(MappingConfigType.SOURCE, null, defaultFromScheme, defaultFromScheme + URI_SCHEME_SEPARATOR);
        MappingConfig dstMapping = new MappingConfig(MappingConfigType.DEST, null, defaultToScheme, defaultToScheme + URI_SCHEME_SEPARATOR);
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
     * Find pairs of source and dest configurations and create a {@link PathMapping} for each.
     * e.g. the following two mapping configurations will form a single PathMapping:
     * routerfs.mapping.s3a.1.replace='s3a://bucket/'
     * routerfs.mapping.s3a.1.with='lakefs://example-repo/dev/'
     *
     * @param mappingConfiguration the configurations to create path mapping from
     */
    private void populatePathMappings(List<MappingConfig> mappingConfiguration) throws InvalidPropertiesFormatException {
        Map<String, Map<Integer, List<MappingConfig>>> scheme2priority2ConfigPair = new HashMap<>();
        for (MappingConfig conf : mappingConfiguration) {
            String fromScheme = conf.getFromScheme();
            int priority = conf.getPriority();
            Map<Integer, List<MappingConfig>> priority2ConfigPair = scheme2priority2ConfigPair.get(fromScheme);
            if (priority2ConfigPair == null) {
                priority2ConfigPair = new HashMap();
                scheme2priority2ConfigPair.put(fromScheme, priority2ConfigPair);
            }
            List<MappingConfig> configPair = priority2ConfigPair.get(priority);
            if (configPair == null) {
                configPair = new ArrayList<>();
                priority2ConfigPair.put(priority, configPair);
            }
            configPair.add(conf);

            // Create path mapping when we've collected a pair.
            if (configPair.size() == CONFIG_PAIR) {
                // The order in which we've added the configurations is unknown, therefore, we need to find the src and dst
                // configs indices.
                int srcIndex = 0;
                int dstIndex = 1;
                if (configPair.get(0).getType() == MappingConfigType.DEST) {
                    srcIndex = 1;
                    dstIndex = 0;
                }
                PathMapping pathMapping = new PathMapping(configPair.get(srcIndex), configPair.get(dstIndex));
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
                .comparing(PathMapping::getFromScheme)
                .thenComparing(PathMapping::getPriority);

        pathMappings = pathMappings.stream()
                .sorted(bySchemeAndIndex)
                .collect(Collectors.toList());
    }

    /**
     * Parses hadoop configurations of the following form:
     * routerfs.mapping.${fromFsScheme}.${mappingIdx}.{replace/with}=prefix and
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
            type = "replace".equals(matcher.group(MAPPING_TYPE_REGEX_GROUP_NAME))? MappingConfigType.SOURCE :
                    MappingConfigType.DEST;
            if (type == MappingConfigType.SOURCE && !hadoopConf.getValue().startsWith(fromFSScheme)) {
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
        String convertedPath = str.replaceFirst(pathMapping.getSrcConfig().getPrefix(), pathMapping.getDstConfig().getPrefix());
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

        private MappingConfig srcConfig;
        private MappingConfig dstConfig;
        private int priority;
        private String fromScheme;
        private boolean defaultMapping;

        public PathMapping(MappingConfig srcPrefix, MappingConfig dstPrefix) {
            this(srcPrefix, dstPrefix, false);
        }

        public PathMapping(MappingConfig srcConfig, MappingConfig dstConfig, boolean defaultMapping) {
            this.defaultMapping = defaultMapping;
            this.srcConfig = srcConfig;
            this.dstConfig = dstConfig;
            if (!srcConfig.getFromScheme().equals(dstConfig.getFromScheme())) {
                LOG.error("src and dst schemes must match, cannot create PathMapping. src:"
                        + srcConfig.getFromScheme() + " dst: " + dstConfig.getFromScheme());
            }
            this.fromScheme = srcConfig.getFromScheme();
            if (!this.defaultMapping) {
                if (srcConfig.getPriority() != dstConfig.getPriority()) {
                    LOG.error("src and dst indices must match, cannot create PathMapping. src:"
                            + srcConfig.getPriority() + " dst: " + dstConfig.getPriority());
                }
                this.priority = srcConfig.getPriority();
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
            return str.startsWith(srcConfig.getPrefix());
        }

        public MappingConfig getSrcConfig() {
            return srcConfig;
        }

        public MappingConfig getDstConfig() {
            return dstConfig;
        }

        public String getFromScheme() {
            return fromScheme;
        }

        public int getPriority() {
            return priority;
        }

        @Override
        public String toString() {
            return "srcPrefix=" + this.srcConfig + " dstPrefix=" + this.dstConfig;
        }
    }

    /**
     * A class that represents a parsed Path mapping configurations supported by RouterFileSystem.
     * Mapping configuration form is: routerfs.mapping.${fromFsScheme}.${mappingIdx}.${replace/with}=prefix.
     */
    private static class MappingConfig {
        private MappingConfigType type;
        private Integer priority;
        private String fromScheme;
        private String prefix;

        public MappingConfig(@Nonnull MappingConfigType type, @Nullable Integer priority, @Nonnull String fromScheme,
                             @Nonnull String prefix) {
            this.type = type;
            this.priority = priority;
            this.fromScheme = fromScheme;
            this.prefix = prefix;
        }

        @Override
        public String toString() {
            return "toScheme=" + this.fromScheme + " type=" + this.type.name() + " index=" + this.priority + " value=" +
                    this.prefix;
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

        public String getPrefix() {
            return prefix;
        }
    }
}
