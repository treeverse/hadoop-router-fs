package io.lakefs.routerfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.URI;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class RouterFileSystemTest {
    private static final String DEFAULT_FS_CONF_KEY = "routerfs.default.fs.def";
    private static final String DEFAULT_FS_IMPL_KEY = "fs.def.impl";
    private static final Path PATH = new Path("some://path/");
    private static final URI uri = URI.create("scheme://authority");
    private static final int BUFFER_SIZE = 3;
    private static Configuration configuration;

    private RouterFileSystem routerFileSystemUnderTest;

    @Mock private PathMapper mockPathMapper;
    @Mock private Path mockPath;
    @Mock private FileSystem mockFileSystem;

    @BeforeEach
    private void setUp() throws IOException {
        lenient().when(this.mockPathMapper.mapPath(any())).thenReturn(this.mockPath);
        this.routerFileSystemUnderTest = new RouterFileSystem(this.mockPathMapper);
        configuration = generateConfiguration();
        lenient().when(this.mockPath.getFileSystem(configuration)).thenReturn(this.mockFileSystem);
        this.routerFileSystemUnderTest.initialize(uri, configuration);
    }

    @Test
    public void testInitializeWithMissingDefaultMappingFail() {
        configuration.set("fs.somescheme.impl", routerFileSystemUnderTest.getClass().getCanonicalName());
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            routerFileSystemUnderTest.initialize(uri, configuration);
        });
        assertTrue(exception.getMessage().contains("There are missing default mappings configurations"));
    }

    @Test
    public void testInitializeWithNoDefaultMappingFail() {
        configuration.unset(DEFAULT_FS_CONF_KEY);
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            routerFileSystemUnderTest.initialize(uri, configuration);
        });
        assertTrue(exception.getMessage().contains("No default filesystem configurations were specified"));
    }

    @Test
    public void testOpen(@Mock FSDataInputStream mockFSDataInputStream) throws IOException {
        when(this.mockFileSystem.open(this.mockPath, BUFFER_SIZE)).thenReturn(mockFSDataInputStream);
        FSDataInputStream result = this.routerFileSystemUnderTest.open(PATH, BUFFER_SIZE);
        assertEquals(mockFSDataInputStream, result);
        verify(this.mockFileSystem, times(1)).open(this.mockPath, BUFFER_SIZE);
    }

    @Test
    public void testCreate(@Mock FsPermission mockPermission, @Mock Progressable mockProgressable, @Mock FSDataOutputStream mockFSDataOutputStream) throws IOException {
        short replication = 1000;
        long blockSize = 20000L;
        boolean overwrite = true;
        when(this.mockFileSystem.create(this.mockPath, mockPermission, overwrite, BUFFER_SIZE, replication, blockSize, mockProgressable)).thenReturn(mockFSDataOutputStream);
        FSDataOutputStream result = this.routerFileSystemUnderTest.create(PATH, mockPermission, overwrite, BUFFER_SIZE, replication, blockSize, mockProgressable);
        assertEquals(mockFSDataOutputStream, result);
        verify(this.mockFileSystem, times(1)).create(this.mockPath, mockPermission, overwrite, BUFFER_SIZE, replication, blockSize, mockProgressable);
    }

    @Test
    public void testAppend(@Mock Progressable mockProgressable, @Mock FSDataOutputStream mockFSDataOutputStream) throws IOException {
        when(this.mockFileSystem.append(this.mockPath, BUFFER_SIZE, mockProgressable)).thenReturn(mockFSDataOutputStream);
        FSDataOutputStream result = this.routerFileSystemUnderTest.append(PATH, BUFFER_SIZE, mockProgressable);
        assertEquals(mockFSDataOutputStream, result);
        verify(this.mockFileSystem, times(1)).append(this.mockPath, BUFFER_SIZE, mockProgressable);
    }

    @Test
    public void testRenameTheSameFileSystemTrue() throws IOException {
        URI uri = URI.create("scheme://path");
        when(this.mockPathMapper.mapPath(any())).thenReturn(this.mockPath, this.mockPath);
        when(this.mockFileSystem.rename(this.mockPath, this.mockPath)).thenReturn(true);
        when(this.mockFileSystem.getUri()).thenReturn(uri);
        Path p2 = new Path("some://other-path/");
        boolean result = this.routerFileSystemUnderTest.rename(PATH, p2);
        assertTrue(result);
        verify(this.mockFileSystem, times(1)).rename(this.mockPath, this.mockPath);
    }

    @Test
    public void testRenameDifferentFileSystemsFalse() throws IOException {
        URI uri = URI.create("scheme://path");
        URI uri2 = URI.create("otherscheme://otherpath");
        when(this.mockFileSystem.getUri()).thenReturn(uri, uri2);
        when(this.mockPathMapper.mapPath(any())).thenReturn(this.mockPath, this.mockPath);
        Path p2 = new Path("some://other-path/");
        boolean result = this.routerFileSystemUnderTest.rename(PATH, p2);
        assertFalse(result);
        verify(this.mockFileSystem, never()).rename(any(), any());
    }

    @Test
    public void testDelete() throws IOException {
        boolean recursive = true;
        when(this.mockFileSystem.delete(this.mockPath, recursive)).thenReturn(true);
        boolean result = this.routerFileSystemUnderTest.delete(PATH, recursive);
        assertTrue(result);
        verify(this.mockFileSystem, times(1)).delete(this.mockPath, recursive);
    }

    @Test
    public void testListStatus(@Mock FileStatus mockFileStatus) throws IOException {
        FileStatus[] mockFileStatuses = {mockFileStatus};
        when(this.mockFileSystem.listStatus(this.mockPath)).thenReturn(mockFileStatuses);
        FileStatus[] statusResults = this.routerFileSystemUnderTest.listStatus(PATH);
        assertEquals(mockFileStatuses, statusResults);
        verify(this.mockFileSystem, times(1)).listStatus(this.mockPath);
    }

    @Test
    public void testMkdirs(@Mock FsPermission mockFsPermission) throws IOException {
        boolean success = true;
        when(this.mockFileSystem.mkdirs(this.mockPath, mockFsPermission)).thenReturn(success);
        boolean result = this.routerFileSystemUnderTest.mkdirs(PATH, mockFsPermission);
        assertTrue(result);
        verify(this.mockFileSystem, times(1)).mkdirs(this.mockPath, mockFsPermission);
    }

    @Test
    public void testGetFileStatus(@Mock FileStatus mockFileStatus) throws IOException {
        when(this.mockFileSystem.getFileStatus(this.mockPath)).thenReturn(mockFileStatus);
        FileStatus result = this.routerFileSystemUnderTest.getFileStatus(PATH);
        assertEquals(mockFileStatus, result);
        verify(this.mockFileSystem, times(1)).getFileStatus(this.mockPath);
    }

    private Configuration generateConfiguration() {
        Configuration config = new Configuration();
        config.set(DEFAULT_FS_CONF_KEY, "doesntMatter");
        config.set(DEFAULT_FS_IMPL_KEY, routerFileSystemUnderTest.getClass().getCanonicalName());
        return config;
    }
}