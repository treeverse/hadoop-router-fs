package io.lakefs.routerfs;

import io.lakefs.routerfs.dto.FileSystemPathTuple;
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
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@NoArgsConstructor
public class RouterFileSystem extends FileSystem {

    public static final Logger LOG = LoggerFactory.getLogger(RouterFileSystem.class);
    private static final String DEFAULT_FS_CONF_PATTEREN = "^routerfs\\.default\\.fs\\.(?<fromScheme>[-a-z0-9_]*)";
    private static final String DEFAULT_FS_SCHEME_REGEX_GROUP_NAME = "fromScheme";
    private static final String DEFAULT_FS_SCHEME_SUFFIX = "-default";
    private static final String DEFAULT_FS_CONF_PREFIX = "routerfs.default.fs";

    private PathMapper pathMapper;

    private Path workingDirectory;

    public RouterFileSystem(PathMapper pathMapper) {
        this.pathMapper = pathMapper;
    }

    /**
     * Returns a URI whose scheme and authority identify this FileSystem.
     */
    @Override
    public URI getUri() {
        return getWorkingDirectory().toUri();
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        // Find RouterFs' default file system configuration, and create a hadoop configuration that maps a new scheme to
        // the default filesystem. e.g., the method converts a configuration of the form
        // routerfs.default.fs.s3a=S3AFileSystem into fs.s3a-default.impl=S3AFileSystem.
        Map.Entry<String, String> defaultFsConf = getDefaultFsConf(conf);
        Pattern pattern = Pattern.compile(DEFAULT_FS_CONF_PATTEREN);
        Matcher matcher = pattern.matcher(defaultFsConf.getKey());
        String defaultFromScheme = null;
        String defaultToScheme = null;
        if (matcher.find()) {
            defaultFromScheme = matcher.group(DEFAULT_FS_SCHEME_REGEX_GROUP_NAME);
            defaultToScheme = defaultFromScheme + DEFAULT_FS_SCHEME_SUFFIX;
        }
        conf.set("fs." + defaultToScheme + ".impl", defaultFsConf.getValue());
        setConf(conf);
        super.initialize(name, conf);
        if(this.pathMapper == null) {
            this.pathMapper = new PathMapper(conf, defaultFromScheme, defaultToScheme);
        }
        this.workingDirectory = new Path(name);
    }

    private Map.Entry<String, String> getDefaultFsConf(Configuration conf) {
        for (Map.Entry<String, String> hadoopConf : conf) {
            if (hadoopConf.getKey().startsWith(DEFAULT_FS_CONF_PREFIX)) {
                return hadoopConf;
            }
        }
        throw new IllegalArgumentException("Missing default file system configuration");
    }

    /**
     * Opens an FSDataInputStream at the indicated Path.
     *
     * @param f          the file name to open
     * @param bufferSize the size of the buffer to be used.
     */
    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        FileSystemPathTuple tuple = generateFSPathTuple(f);
        return tuple.getFileSystem().open(tuple.getPath(), bufferSize);
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
        FileSystemPathTuple tuple = generateFSPathTuple(f);
        return tuple.getFileSystem().create(tuple.getPath(), permission, overwrite, bufferSize, replication, blockSize, progress);
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
        FileSystemPathTuple tuple = generateFSPathTuple(f);
        return tuple.getFileSystem().append(tuple.getPath(), bufferSize, progress);
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
        FileSystemPathTuple srcTuple = generateFSPathTuple(src);
        FileSystemPathTuple dstTuple = generateFSPathTuple(dst);
        if(!srcTuple.getFileSystem().equals(dstTuple.getFileSystem())) {
            LOG.warn("Cannot rename between different underlying FileSystems");
            return false;
        }
        return srcTuple.getFileSystem().rename(srcTuple.getPath(), this.pathMapper.mapPath(dst));
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
        FileSystemPathTuple tuple = generateFSPathTuple(f);
        return tuple.getFileSystem().delete(tuple.getPath(), recursive);
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
        FileSystemPathTuple tuple = generateFSPathTuple(f);
        return tuple.getFileSystem().listStatus(tuple.getPath());
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
            FileSystemPathTuple tuple = generateFSPathTuple(new_dir);
            tuple.getFileSystem().setWorkingDirectory(tuple.getPath());
            this.workingDirectory = new_dir;
        } catch (IOException e) {
            LOG.error("Failed setting a working directory with exception:\n{}", e.getLocalizedMessage());
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
        FileSystemPathTuple tuple = generateFSPathTuple(f);
        return tuple.getFileSystem().mkdirs(tuple.getPath(), permission);
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
        FileSystemPathTuple tuple = generateFSPathTuple(f);
        return tuple.getFileSystem().getFileStatus(tuple.getPath());
    }

    private FileSystemPathTuple generateFSPathTuple(Path p) throws IOException {
        Path mappedPath = this.pathMapper.mapPath(p);
        FileSystem fs = mappedPath.getFileSystem(getConf());
        return new FileSystemPathTuple(fs, mappedPath);
    }
}
