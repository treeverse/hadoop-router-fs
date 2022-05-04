package io.lakefs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.*;

@RunWith(value = Parameterized.class)
public class PathMapperTest {

    private PathMapper pathMapper;

    private String testName;
    private Map<String, String> mappingConfig;
    private Map<String, String> pathToExpected;
    private Class<? extends Exception> expectedException;

    public PathMapperTest(String testName, Map<String, String> mappingConfig, Map<String, String> pathToExpected,
                          Class<? extends Exception> expectedException) {
        this.testName = testName;
        this.mappingConfig = mappingConfig;
        this.pathToExpected = pathToExpected;
        this.expectedException = expectedException;
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"Mapping config values are directories", new HashMap<String, String>() {{
                    put("routerfs.mapping.gcs.1.replace", "s3a://bucket/");
                    put("routerfs.mapping.gcs.1.with", "gcs://bar/");
                    put("routerfs.mapping.s3a-default.replace", "s3a://");
                    put("routerfs.mapping.s3a-default.with", "s3a-default://");
                }}, new HashMap<String, String>() {{
                    put("s3a://bucket/foo/a", "gcs://bar/foo/a");
                }}, null},

                {"Mapping config values are not directories", new HashMap<String, String>() {{
                    put("routerfs.mapping.lakefs.1.replace", "s3a://bucket/team");
                    put("routerfs.mapping.lakefs.1.with", "lakefs://example-repo/team");
                    put("routerfs.mapping.s3a-default.replace", "s3a://");
                    put("routerfs.mapping.s3a-default.with", "s3a-default://");
                }}, new HashMap<String, String>() {{
                    put("s3a://bucket/team1/file1", "lakefs://example-repo/team1/file1");
                    put("s3a://bucket/team2/file2", "lakefs://example-repo/team2/file2");
                }}, null},

                {"Only path prefix is replaced", new HashMap<String, String>() {{
                    put("routerfs.mapping.lakefs.1.replace", "s3a://bucket/");
                    put("routerfs.mapping.lakefs.1.with", "lakefs://example-repo/b1/");
                    put("routerfs.mapping.s3a-default.replace", "s3a://");
                    put("routerfs.mapping.s3a-default.with", "s3a-default://");
                }}, new HashMap<String, String>() {{
                    put("s3a://bucket/bucket/a.txt", "lakefs://example-repo/b1/bucket/a.txt");
                }}, null},

                {"Mapping configs apply in order", new HashMap<String, String>() {{
                    put("routerfs.mapping.lakefs.2.replace", "s3a://bucket/foo/");
                    put("routerfs.mapping.lakefs.2.with", "lakefs://example-repo/b2/");
                    put("routerfs.mapping.lakefs.1.replace", "s3a://bucket/");
                    put("routerfs.mapping.lakefs.1.with", "lakefs://example-repo/b1/");
                    put("routerfs.mapping.s3a-default.replace", "s3a://");
                    put("routerfs.mapping.s3a-default.with", "s3a-default://");
                }}, new HashMap<String, String>() {{
                    put("s3a://bucket/foo/a.txt", "lakefs://example-repo/b1/foo/a.txt");
                }}, null},

                {"Mapping into two file systems except for default",  new HashMap<String, String>() {{
                    put("routerfs.mapping.gcs.1.replace", "s3a://bucket1/");
                    put("routerfs.mapping.gcs.1.with", "gcs://bucket1/");
                    put("routerfs.mapping.lakefs.1.replace", "s3a://bucket2/");
                    put("routerfs.mapping.lakefs.1.with", "lakefs://example-repo/b1/");
                    put("routerfs.mapping.s3a-default.replace", "s3a://");
                    put("routerfs.mapping.s3a-default.with", "s3a-default://");
                }}, new HashMap<String, String>() {{
                    put("s3a://bucket1/foo/a.txt", "gcs://bucket1/foo/a.txt");
                    put("s3a://bucket2/b.txt", "lakefs://example-repo/b1/b.txt");
                }}, null},

                {"src mapping prefix is a URI scheme",  new HashMap<String, String>() {{
                    put("routerfs.mapping.gcs.1.replace", "gcs://");
                    put("routerfs.mapping.gcs.1.with", "s3a://bucket1/");
                    put("routerfs.mapping.s3a-default.replace", "s3a://");
                    put("routerfs.mapping.s3a-default.with", "s3a-default://");
                }}, new HashMap<String, String>() {{
                    put("gcs://a.txt" , "s3a://bucket1/a.txt");
                }}, null},

                {"dst mapping prefix is a URI scheme", new HashMap<String, String>() {{
                    put("routerfs.mapping.gcs.1.replace", "s3a://bucket/boo/");
                    put("routerfs.mapping.gcs.1.with", "gcs://");
                    put("routerfs.mapping.s3a-default.replace", "s3a://");
                    put("routerfs.mapping.s3a-default.with", "s3a-default://");
                }}, new HashMap<String, String>() {{
                    put("s3a://bucket/boo/a.txt", "gcs://a.txt");
                }}, null},

                {"dst and src mapping prefixes are URI schemes", new HashMap<String, String>() {{
                    put("routerfs.mapping.gcs.1.replace", "minio://");
                    put("routerfs.mapping.gcs.1.with", "gcs://");
                    put("routerfs.mapping.s3a-default.replace", "s3a://");
                    put("routerfs.mapping.s3a-default.with", "s3a-default://");
                }}, new HashMap<String, String>() {{
                    put("minio://a.txt", "gcs://a.txt");
                }}, null},

                {"Fallback to default Mapping", new HashMap<String, String>() {{
                    put("routerfs.mapping.lakefs.1.replace", "s3a://bucket/foo/");
                    put("routerfs.mapping.lakefs.1.with", "lakefs://example-repo/b1/");
                    put("routerfs.mapping.s3a-default.replace", "s3a://");
                    put("routerfs.mapping.s3a-default.with", "s3a-default://");
                }}, new HashMap<String, String>() {{
                    put("s3a://bucket/bar/a.txt", "s3a-default://bucket/bar/a.txt");
                    put("s3a://a.txt", "s3a-default://a.txt");
                }}, null},

                {"Invalid mapping config index", new HashMap<String, String>() {{
                    put("routerfs.mapping.lakefs.notAnInt.replace", "s3a://bucket");
                    put("routerfs.mapping.lakefs.1.with", "lakefs://example-repo/b1");}},
                        null, InvalidPropertiesFormatException.class},

                {"Invalid mapping config type", new HashMap<String, String>() {{
                    put("routerfs.mapping.lakefs.1.notAMappingConfType", "s3a://bucket");
                    put("routerfs.mapping.lakefs.1.with", "lakefs://example-repo/b1");}},
                        null, InvalidPropertiesFormatException.class},

                {"Missing default mapping configuration", new HashMap<String, String>() {{
                    put("routerfs.mapping.lakefs.1.replace", "s3a://bucket");
                    put("routerfs.mapping.lakefs.1.with", "lakefs://example-repo/b1");}},
                        null, IllegalArgumentException.class},

                {"Invalid mapping config fs scheme", new HashMap<String, String>() {{
                    put("routerfs.mapping.#@.1.replace", "s3a://bucket");
                    put("routerfs.mapping.#@.1.with", "#@://boo");}},
                        null, InvalidPropertiesFormatException.class},

        });
    }

    @Test
    public void testMapPath() throws Exception {
        if (expectedException != null) {
            thrown.expect(expectedException);
        }
        prepareTest(mappingConfig);
        for (Map.Entry<String, String> pair : pathToExpected.entrySet()) {
            Path actual = pathMapper.mapPath(new Path(pair.getKey()));
            Assert.assertEquals(pair.getValue(), actual.toString());
        }
    }

    private void prepareTest(Map<String, String> mappingConfig) throws IOException {
        Configuration conf = new Configuration();
        for (Map.Entry<String, String> mc : mappingConfig.entrySet()) {
            conf.set(mc.getKey(), mc.getValue());
        }
        pathMapper = new PathMapper(conf);
    }
}