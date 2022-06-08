package io.lakefs.routerfs;

import io.lakefs.routerfs.dto.DefaultPrefixMapping;
import io.lakefs.routerfs.dto.FileSystemPathProperties;
import io.lakefs.routerfs.dto.PathProperties;
import lombok.NoArgsConstructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@NoArgsConstructor
public class RouterFileSystem extends FileSystem {

    private static final Logger LOG = LoggerFactory.getLogger(RouterFileSystem.class);
    private static final String DEFAULT_FS_SCHEME_REGEX_GROUP_NAME = "groupScheme";
    private static final String DEFAULT_FS_CONF_PATTERN = String.format("^routerfs\\.default\\.fs\\.(?<%s>[-a-z0-9_]*)", DEFAULT_FS_SCHEME_REGEX_GROUP_NAME);
    private static final String DEFAULT_FS_IMPL_PATTERN = String.format("^fs\\.(?<%s>[-a-z0-9_]*)\\.impl", DEFAULT_FS_SCHEME_REGEX_GROUP_NAME);
    private static final String DEFAULT_FS_SCHEME_SUFFIX = "-default";
    private static final String DEFAULT_FS_CONF_PREFIX = "routerfs.default.fs";
    private static final String FS_IMPL_KEY_FORMAT = "fs.%s.impl";

    private PathMapper pathMapper;
    private Path workingDirectory;
    private URI uri;

    public RouterFileSystem(PathMapper pathMapper) {
        this.pathMapper = pathMapper;
    }

    /**
     * Returns a URI whose scheme and authority identify this FileSystem.
     */
    @Override
    public URI getUri() {
        return this.uri;
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        // Find RouterFs' default file system configurations, and create a hadoop configuration that maps a new scheme to
        // the corresponding default filesystem. e.g., the method converts a configuration of the form
        // routerfs.default.fs.s3a=S3AFileSystem into fs.s3a-default.impl=S3AFileSystem.
        Map<String, String> defaultFsConfs = getDefaultFsConf(conf);
        List<DefaultPrefixMapping> defaultSchemePairsSchemeTranslation = new ArrayList<>();
        for(String defaultKey: defaultFsConfs.keySet()) {
            Pattern pattern = Pattern.compile(DEFAULT_FS_CONF_PATTERN);
            Matcher matcher = pattern.matcher(defaultKey);
            if (matcher.find()) {
                String defaultFromScheme = matcher.group(DEFAULT_FS_SCHEME_REGEX_GROUP_NAME);
                String defaultToScheme = defaultFromScheme + DEFAULT_FS_SCHEME_SUFFIX;
                String fsDefaultImplKey = String.format(FS_IMPL_KEY_FORMAT, defaultToScheme);
                String fsDefaultImplValue = defaultFsConfs.get(defaultKey);
                conf.set(fsDefaultImplKey, fsDefaultImplValue);
                defaultSchemePairsSchemeTranslation.add(new DefaultPrefixMapping(defaultFromScheme, defaultToScheme));
                LOG.debug("initialize: adding \"{} = {}\" default mapping to the configurations", fsDefaultImplKey, fsDefaultImplValue);
            }
        }
        setConf(conf);
        super.initialize(name, conf);
        if (this.pathMapper == null) {
            this.pathMapper = new PathMapper(conf, defaultSchemePairsSchemeTranslation);
        }
        this.workingDirectory = new Path(name);
        LOG.debug("initialize: setting working directory to {}", this.workingDirectory);
        this.uri = name;
    }

    private Map<String, String> getDefaultFsConf(Configuration conf) {
        Pattern defaultMappingPattern = Pattern.compile(DEFAULT_FS_CONF_PATTERN);
        Pattern implMappingPattern = Pattern.compile(DEFAULT_FS_IMPL_PATTERN);
        Set<String> mappedImplSchemes = new HashSet<>();
        Set<String> defaultSchemes = new HashSet<>();
        Map<String, String> defaultConf = new HashMap<>();
        for (Map.Entry<String, String> hadoopConf : conf) {
            String confKey = hadoopConf.getKey();
            Matcher defaultConfKeyMatcher = defaultMappingPattern.matcher(confKey);
            if (confKey.startsWith(DEFAULT_FS_CONF_PREFIX) && defaultConfKeyMatcher.find()) {
                defaultConf.put(hadoopConf.getKey(), hadoopConf.getValue());
                defaultSchemes.add(defaultConfKeyMatcher.group(DEFAULT_FS_SCHEME_REGEX_GROUP_NAME));
            }
            else {
                Matcher implConfKeyMatcher = implMappingPattern.matcher(confKey);
                if (implConfKeyMatcher.find()) {
                    String confValue = hadoopConf.getValue();
                    if (confValue.equals(this.getClass().getCanonicalName())) {
                        mappedImplSchemes.add(implConfKeyMatcher.group(DEFAULT_FS_SCHEME_REGEX_GROUP_NAME));
                    }
                }
            }
        }
        validateDefaultMappings(defaultSchemes, mappedImplSchemes);
        return defaultConf;
    }

    private void validateDefaultMappings(Set<String> defaultSchemes, Set<String> mappedImplSchemes) {
        if (defaultSchemes.isEmpty()) {
            throw new IllegalArgumentException("No default filesystem configurations were specified");
        }
        if (!defaultSchemes.containsAll(mappedImplSchemes)) {
            throw new IllegalArgumentException("There are missing default mappings configurations");
        }
    }

    /**
     * Opens an FSDataInputStream at the indicated Path.
     *
     * @param f          the file name to open
     * @param bufferSize the size of the buffer to be used.
     */
    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        FileSystemPathProperties fileSystemPathProperties = generateFSPathProperties(f);
        PathProperties pathProperties = fileSystemPathProperties.getPathProperties();
        logTranslatedPaths("open", f, pathProperties.getPath());
        return fileSystemPathProperties.getFileSystem().open(pathProperties.getPath(), bufferSize);
    }

    /**
     * Create an FSDataOutputStream at the indicated Path with write-progress
     * reporting.
     *
     * @param f           the file name to open
     * @param permission
     * @param overwrite   if a file with this name already exists, then if true,
     *                    the file will be overwritten, and if false an error will be thrown.
     * @param bufferSize  the size of the buffer to be used.
     * @param replication required block replication for the file.
     * @param blockSize
     * @param progress
     * @throws IOException
     * @see #setPermission(Path, FsPermission)
     */
    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
                                     boolean overwrite, int bufferSize,
                                     short replication, long blockSize,
                                     Progressable progress) throws IOException {
        FileSystemPathProperties fileSystemPathProperties = generateFSPathProperties(f);
        PathProperties pathProperties = fileSystemPathProperties.getPathProperties();
        logTranslatedPaths("create", f, pathProperties.getPath());
        return fileSystemPathProperties.getFileSystem().create(pathProperties.getPath(), permission, overwrite, bufferSize, replication, blockSize, progress);
    }

    /**
     * Append to an existing file (optional operation).
     *
     * @param f          the existing file to be appended.
     * @param bufferSize the size of the buffer to be used.
     * @param progress   for reporting progress if it is not null.
     * @throws IOException
     */
    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        FileSystemPathProperties fileSystemPathProperties = generateFSPathProperties(f);
        PathProperties pathProperties = fileSystemPathProperties.getPathProperties();
        logTranslatedPaths("append", f, pathProperties.getPath());
        return fileSystemPathProperties.getFileSystem().append(pathProperties.getPath(), bufferSize, progress);
    }

    /**
     * Renames Path src to Path dst.  Can take place on local fs
     * or remote DFS.
     *
     * @param src path to be renamed
     * @param dst new path after rename
     * @return true if rename is successful
     * @throws IOException on failure
     */
    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        FileSystemPathProperties srcFileSystemPathProperties = generateFSPathProperties(src);
        FileSystemPathProperties dstFileSystemPathProperties = generateFSPathProperties(dst);
        URI srcFileSystemURI = srcFileSystemPathProperties.getFileSystem().getUri();
        URI dstFileSystemURI = dstFileSystemPathProperties.getFileSystem().getUri();
        LOG.debug("rename: was called with source path {} and destination path {}, source translated path is {}, destination translated path is {} ",
                src,
                dst,
                srcFileSystemPathProperties.getPathProperties().getPath(),
                dstFileSystemPathProperties.getPathProperties().getPath()
        );
        if (!srcFileSystemURI.equals(dstFileSystemURI)) {
            LOG.warn("rename: cannot rename between different underlying FileSystems");
            return false;
        }
        PathProperties srcPathProperties = srcFileSystemPathProperties.getPathProperties();
        PathProperties dstPathProperties = dstFileSystemPathProperties.getPathProperties();
        return srcFileSystemPathProperties.getFileSystem().rename(srcPathProperties.getPath(), dstPathProperties.getPath());
    }

    /**
     * Delete a file.
     *
     * @param f         the path to delete.
     * @param recursive if path is a directory and set to
     *                  true, the directory is deleted else throws an exception. In
     *                  case of a file the recursive can be set to either true or false.
     * @return true if delete is successful else false.
     * @throws IOException
     */
    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        FileSystemPathProperties fileSystemPathProperties = generateFSPathProperties(f);
        /*
        Potential Bug:
            If `recursive == true`, the recursive process may cause the underlying filesystems to change.
            For example, if "s3a://bucket/dir1/" is mapped to "fs1://bla/", and "s3a://bucket/dir1/dir2/" is mapped to
            "fs2://blah/" then the recursive process will fail if the path points to "s3a://bucket/" or "s3a://bucket/dir1/"
         */
        PathProperties pathProperties = fileSystemPathProperties.getPathProperties();
        logTranslatedPaths("delete", f, pathProperties.getPath());
        return fileSystemPathProperties.getFileSystem().delete(pathProperties.getPath(), recursive);
    }

    /**
     * List the statuses of the files/directories in the given path if the path is
     * a directory.
     *
     * @param f given path
     * @return the statuses of the files/directories in the given patch
     * @throws FileNotFoundException when the path does not exist;
     *                               IOException see specific implementation
     */
    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        FileSystemPathProperties fileSystemPathProperties = generateFSPathProperties(f);
        PathProperties pathProperties = fileSystemPathProperties.getPathProperties();
        logTranslatedPaths("listStatus", f, pathProperties.getPath());
        FileStatus[] fileStatusResults = fileSystemPathProperties.getFileSystem().listStatus(pathProperties.getPath());
        LOG.trace("listStatus: retrieved the following file statuses: {}", (Object[]) fileStatusResults);
        return Arrays.stream(fileStatusResults)
                .map(fileStatus -> switchFileStatusPathPrefix(fileStatus, pathProperties.getDstPrefix(), pathProperties.getSrcPrefix()))
                .toArray(FileStatus[]::new);
    }

    /**
     * Set the current working directory for the given file system. All relative
     * paths will be resolved relative to it.
     *
     * @param new_dir
     */
    @Override
    public void setWorkingDirectory(Path new_dir) {
        try {
            FileSystemPathProperties fileSystemPathProperties = generateFSPathProperties(new_dir);
            PathProperties pathProperties = fileSystemPathProperties.getPathProperties();
            logTranslatedPaths("setWorkingDirectory", new_dir, pathProperties.getPath());
            fileSystemPathProperties.getFileSystem().setWorkingDirectory(pathProperties.getPath());
            this.workingDirectory = new_dir;
        } catch (IOException e) {
            LOG.error("setWorkingDirectory: failed setting a working directory with exception:\n{}", e.getLocalizedMessage());
        }
    }

    /**
     * Get the current working directory for the given file system
     *
     * @return the directory pathname
     */
    @Override
    public Path getWorkingDirectory() {
        return this.workingDirectory;
    }

    /**
     * Make the given file and all non-existent parents into
     * directories. Has the semantics of Unix 'mkdir -p'.
     * Existence of the directory hierarchy is not an error.
     *
     * @param f          path to create
     * @param permission to apply to f
     */
    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        FileSystemPathProperties fileSystemPathProperties = generateFSPathProperties(f);
        PathProperties pathProperties = fileSystemPathProperties.getPathProperties();
        logTranslatedPaths("mkdirs", f, pathProperties.getPath());
        return fileSystemPathProperties.getFileSystem().mkdirs(pathProperties.getPath(), permission);
    }

    /**
     * Return a file status object that represents the path.
     *
     * @param f The path we want information from
     * @return a FileStatus object
     * @throws FileNotFoundException when the path does not exist;
     *                               IOException see specific implementation
     */
    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        FileSystemPathProperties fileSystemPathProperties = generateFSPathProperties(f);
        PathProperties pathProperties = fileSystemPathProperties.getPathProperties();
        logTranslatedPaths("getFileStatus", f, pathProperties.getPath());
        FileStatus fileStatus = fileSystemPathProperties.getFileSystem().getFileStatus(pathProperties.getPath());
        return switchFileStatusPathPrefix(fileStatus, pathProperties.getDstPrefix(), pathProperties.getSrcPrefix());
    }

    private FileSystemPathProperties generateFSPathProperties(Path p) throws IOException {
        p = createSchemedPath(p);
        PathProperties pathProperties = this.pathMapper.mapPath(p);
        Path mappedPath = pathProperties.getPath();
        FileSystem fs = mappedPath.getFileSystem(getConf());
        return new FileSystemPathProperties(fs, pathProperties);
    }

    private Path createSchemedPath(Path p) {
        String pathScheme = p.toUri() != null ? p.toUri().getScheme() : null;
        if (pathScheme == null || pathScheme.isEmpty()) {
            return new Path(getWorkingDirectory(), p);
        }
        return p;
    }

    /**
     * This method translate a given FileStatus path's prefix to another prefix.
     * This is used to translate paths to their original form.
     * We need this functionality because RouterFileSystem has methods that return converted paths (from source uri to
     * the configured mapping uri), like "listStatus", that are recursively traversed by it. This causes unwanted behavior
     * as RouterFS might not be familiar with the converted paths. Therefore, RouterFileSystem will return the original
     * path using this function.
     * @param fileStatus the fileStatus that contain the path we're converting.
     * @param fromPrefix the fileStatus path's prefix we wish to change.
     * @param toPrefix the prefix we want to change to.
     * @return a fileStatus with the changed path's prefix.
     */
    private static FileStatus switchFileStatusPathPrefix(FileStatus fileStatus, String fromPrefix, String toPrefix) {
        String mappedUri = fileStatus.getPath().toString();
        if (!mappedUri.startsWith(fromPrefix)) {
            throw new InvalidPathException(String.format("Path %s doesn't start with 'fromPrefix' \"%s\"", mappedUri, fromPrefix));
        }
        String srcUri = mappedUri.replaceFirst(fromPrefix, toPrefix);
        Path path = new Path(srcUri);
        fileStatus.setPath(path);
        LOG.trace("switchFileStatusPathPrefix: replaced file status with path URI \"{}\" with path URI \"{}\"", mappedUri, srcUri);
        return fileStatus;
    }

    private void logTranslatedPaths(String methodName, Path srcPath, Path dstPath) {
        LOG.trace("{}: path {} converted to {}", methodName, srcPath, dstPath);
    }
}
