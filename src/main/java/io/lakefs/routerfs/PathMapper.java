package io.lakefs.routerfs;

import io.lakefs.routerfs.dto.DefaultPrefixMapping;
import io.lakefs.routerfs.dto.PathProperties;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.InvalidPathException;
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
    private final List<PathMapping> defaultMappings;

    public PathMapper(Configuration conf, @NonNull List<DefaultPrefixMapping> defaultPrefixMappings) throws IOException {
        this.pathMappings = new ArrayList<>();
        if (defaultPrefixMappings.isEmpty()) {
            throw new IllegalArgumentException("Provided default filesystems mapping is empty");
        }
        this.defaultMappings = createDefaultMapping(defaultPrefixMappings);
        loadMappingConfig(conf);
    }

    private static List<PathMapping> createDefaultMapping(List<DefaultPrefixMapping> defaultPrefixMappings) {
        return defaultPrefixMappings.stream()
                .map(defaultPrefixMapping -> {
                    MappingConfig srcMapping = new MappingConfig.MappingConfigBuilder()
                            .type(MappingConfigType.SOURCE)
                            .groupScheme(defaultPrefixMapping.getFromScheme())
                            .prefix(defaultPrefixMapping.getFromScheme() + URI_SCHEME_SEPARATOR)
                            .build();

                    MappingConfig dstMapping = new MappingConfig.MappingConfigBuilder()
                            .type(MappingConfigType.DEST)
                            .groupScheme(defaultPrefixMapping.getToScheme())
                            .prefix(defaultPrefixMapping.getToScheme() + URI_SCHEME_SEPARATOR)
                            .build();
                    return new PathMapping(srcMapping, dstMapping, true);
                })
                .collect(Collectors.toList());
    }

    private void loadMappingConfig(Configuration conf) throws InvalidPropertiesFormatException {
        List<MappingConfig> mappingConfigurations = parseMappingConfig(conf);
        this.pathMappings = populatePathMappings(mappingConfigurations);

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
    private List<PathMapping> populatePathMappings(List<MappingConfig> mappingConfiguration) throws InvalidPropertiesFormatException {
        List<PathMapping> pathMappings = new ArrayList<>();
        Map<String, Map<Integer, List<MappingConfig>>> scheme2priority2ConfigPair = new HashMap<>();
        for (MappingConfig conf : mappingConfiguration) {
            String groupScheme = conf.getGroupScheme();
            int priority = conf.getPriority();
            Map<Integer, List<MappingConfig>> priority2ConfigPair = scheme2priority2ConfigPair.computeIfAbsent(groupScheme, k -> new HashMap<>());
            List<MappingConfig> configPair = priority2ConfigPair.computeIfAbsent(priority, k -> new ArrayList<>());
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
        return sortPathMappingsBySchemeAndIdx(pathMappings);
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
                LOG.trace("Loaded and parsed mapping config with key:{} and value:{}", hadoopConf.getKey(), hadoopConf.getValue());
            }
        }
        return mappingConfig;
    }

    /**
     * A method for troubleshooting purposes.
     */
    private void logLoadedMappings() {
        this.pathMappings.forEach(pathMapping -> LOG.debug("pathMappings: {}", pathMapping));
        this.defaultMappings.forEach(defaultMapping -> LOG.debug("defaultMapping: {}", defaultMapping));
    }

    /**
     * Sort the loaded path mappings by scheme and then idx. This is required by the path mapper because
     * mapping configurations are applied in-order.
     *
     * @param pathMappings list of PathMappings to sort
     * @return sorted list of PathMapping (by scheme and then priority)
     */
    private List<PathMapping> sortPathMappingsBySchemeAndIdx(List<PathMapping> pathMappings) {
        Comparator<PathMapping> bySchemeAndIndex = Comparator
                .comparing(PathMapping::getGroupScheme)
                .thenComparing(PathMapping::getPriority);

        return pathMappings.stream()
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
        int mappingPriority;
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
    public PathProperties mapPath(Path origPath) {
        PathMapping pathMapping = findAppropriatePathMapping(origPath)
                .orElseGet(() -> findDefaultPathMapping(origPath).orElse(null));
        if (pathMapping == null) {
            LOG.trace("Can't find a matching path mapping nor default path mapping for {}", origPath);
            throw new InvalidPathException(origPath.toUri().toString());
        }
        Path dstPath = convertPath(origPath, pathMapping);
        return PathProperties.builder()
                .srcPrefix(pathMapping.getSrcConfig().getPrefix())
                .dstPrefix(pathMapping.getDstConfig().getPrefix())
                .path(dstPath)
                .build();
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
        LOG.trace("Converted {} to {} using path mapping {}", path, convertedPath,  pathMapping);
        return new Path(convertedPath);
    }

    private Optional<PathMapping> findAppropriatePathMapping(Path path) {
        return this.pathMappings.stream()
                .filter(pm -> pm.isAppropriateMapping(path))
                .findFirst();
    }

    private Optional<PathMapping> findDefaultPathMapping(Path path) {
        return this.defaultMappings.stream()
                .filter(defaultMapping -> path.toString().startsWith(defaultMapping.getGroupScheme()))
                .findFirst();
    }

    /**
     * PathMapping represents a pair of mapping configurations, that define source and destination prefixes to
     * be replaced.
     */
    private static class PathMapping {

        @Getter private final MappingConfig srcConfig;
        @Getter private final MappingConfig dstConfig;
        @Getter private int priority = -1;
        @Getter private final String groupScheme;

        public PathMapping(MappingConfig srcPrefix, MappingConfig dstPrefix) {
            this(srcPrefix, dstPrefix, false);
        }

        public PathMapping(MappingConfig srcConfig, MappingConfig dstConfig, boolean defaultMapping) {
            this.srcConfig = srcConfig;
            this.dstConfig = dstConfig;
            this.groupScheme = srcConfig.getGroupScheme();

            if (!defaultMapping) {
                if (!srcConfig.getGroupScheme().equals(dstConfig.getGroupScheme())) {
                    LOG.error("src and dst schemes must match, cannot create PathMapping. src: {}, dst: {}", srcConfig.getGroupScheme(), dstConfig.getGroupScheme());
                    throw new IllegalArgumentException();
                }
                if (srcConfig.getPriority() != dstConfig.getPriority()) {
                    LOG.error("src and dst indices must match, cannot create PathMapping. src: {}, dst: {}", srcConfig.getPriority(), dstConfig.getPriority());
                    throw new IllegalArgumentException();
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
            return str.startsWith(this.srcConfig.getPrefix());
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
    @Getter
    @Builder
    private static class MappingConfig {
        private final MappingConfigType type;
        private int priority = -1;
        private final String groupScheme;
        private final String prefix;

        public MappingConfig(@Nonnull MappingConfigType type, @Nullable Integer priority, @Nonnull String groupScheme,
                             @Nonnull String prefix) {
            this.type = type;
            if (priority != null) {
                this.priority = priority;
            }
            this.groupScheme = groupScheme;
            this.prefix = prefix;
        }

        @Override
        public String toString() {
            return "toScheme=" + this.groupScheme + " type=" + this.type.name() + " index=" + this.priority + " value=" +
                    this.prefix;
        }
    }
}
