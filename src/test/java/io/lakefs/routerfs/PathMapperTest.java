package io.lakefs.routerfs;

import io.lakefs.routerfs.dto.DefaultSchemeTranslation;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@RunWith(value = Parameterized.class)
public class PathMapperTest {
    private PathMapper pathMapper;

    private String testName;
    private Map<String, String> mappingConfig;
    private Map<String, String> pathToExpected;
    private List<DefaultSchemeTranslation> defaultSchemeTranslations;
    private Class<? extends Exception> expectedException;

    public PathMapperTest(String testName,
                          Map<String, String> mappingConfig,
                          List<DefaultSchemeTranslation> defaultSchemeTranslations,
                          Map<String, String> pathToExpected,
                          Class<? extends Exception> expectedException) {
        this.testName = testName;
        this.mappingConfig = mappingConfig;
        this.pathToExpected = pathToExpected;
        this.expectedException = expectedException;
        this.defaultSchemeTranslations = defaultSchemeTranslations;
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"Mapping config values are directories", new HashMap<String, String>() {{
                    put("routerfs.mapping.s3a.1.replace", "s3a://bucket/");
                    put("routerfs.mapping.s3a.1.with", "gcs://bar/");
                }}, Collections.singletonList(new DefaultSchemeTranslation("s3a", "s3a-default")),
                        new HashMap<String, String>() {{
                    put("s3a://bucket/foo/a", "gcs://bar/foo/a");
                }}, null},

                {"Mapping config values are not directories", new HashMap<String, String>() {{
                    put("routerfs.mapping.s3a.1.replace", "s3a://bucket/team");
                    put("routerfs.mapping.s3a.1.with", "lakefs://example-repo/team");
                }}, Collections.singletonList(new DefaultSchemeTranslation("s3a", "s3a-default")),
                        new HashMap<String, String>() {{
                    put("s3a://bucket/team1/file1", "lakefs://example-repo/team1/file1");
                    put("s3a://bucket/team2/file2", "lakefs://example-repo/team2/file2");
                }}, null},

                {"Only path prefix is replaced", new HashMap<String, String>() {{
                    put("routerfs.mapping.s3a.1.replace", "s3a://bucket/");
                    put("routerfs.mapping.s3a.1.with", "lakefs://example-repo/b1/");
                }}, Collections.singletonList(new DefaultSchemeTranslation("s3a", "s3a-default")),
                        new HashMap<String, String>() {{
                    put("s3a://bucket/bucket/a.txt", "lakefs://example-repo/b1/bucket/a.txt");
                }}, null},

                {"Mapping configs apply in order", new HashMap<String, String>() {{
                    put("routerfs.mapping.s3a.2.replace", "s3a://bucket/foo/");
                    put("routerfs.mapping.s3a.2.with", "lakefs://example-repo/b2/");
                    put("routerfs.mapping.s3a.1.replace", "s3a://bucket/");
                    put("routerfs.mapping.s3a.1.with", "lakefs://example-repo/b1/");
                }}, Collections.singletonList(new DefaultSchemeTranslation("s3a", "s3a-default")),
                        new HashMap<String, String>() {{
                    put("s3a://bucket/foo/a.txt", "lakefs://example-repo/b1/foo/a.txt");
                }}, null},

                {"Mapping into two file systems except for default",  new HashMap<String, String>() {{
                    put("routerfs.mapping.s3a.1.replace", "s3a://bucket1/");
                    put("routerfs.mapping.s3a.1.with", "gcs://bucket1/");
                    put("routerfs.mapping.s3a.2.replace", "s3a://bucket2/");
                    put("routerfs.mapping.s3a.2.with", "lakefs://example-repo/b1/");
                }}, Collections.singletonList(new DefaultSchemeTranslation("s3a", "s3a-default")),
                        new HashMap<String, String>() {{
                    put("s3a://bucket1/foo/a.txt", "gcs://bucket1/foo/a.txt");
                    put("s3a://bucket2/b.txt", "lakefs://example-repo/b1/b.txt");
                }}, null},

                {"Mapping into two file systems except for default, and mappings apply in order",  new HashMap<String, String>() {{
                    put("routerfs.mapping.s3a.3.replace", "s3a://bucket1/");
                    put("routerfs.mapping.s3a.3.with", "gcs://bucket3");
                    put("routerfs.mapping.s3a.2.replace", "s3a://bucket2/");
                    put("routerfs.mapping.s3a.2.with", "lakefs://example-repo/b1/");
                    put("routerfs.mapping.s3a.1.replace", "s3a://bucket1/");
                    put("routerfs.mapping.s3a.1.with", "gcs://bucket1/");
                }}, Collections.singletonList(new DefaultSchemeTranslation("s3a", "s3a-default")),
                        new HashMap<String, String>() {{
                    put("s3a://bucket1/foo/a.txt", "gcs://bucket1/foo/a.txt");
                    put("s3a://bucket2/b.txt", "lakefs://example-repo/b1/b.txt");
                }}, null},


                {"src mapping prefix is a URI scheme",  new HashMap<String, String>() {{
                    put("routerfs.mapping.gcs.1.replace", "gcs://");
                    put("routerfs.mapping.gcs.1.with", "s3a://bucket1/");
                }}, Collections.singletonList(new DefaultSchemeTranslation("s3a", "s3a-default")),
                        new HashMap<String, String>() {{
                    put("gcs://a.txt" , "s3a://bucket1/a.txt");
                }}, null},

                {"dst mapping prefix is a URI scheme", new HashMap<String, String>() {{
                    put("routerfs.mapping.s3a.1.replace", "s3a://bucket/boo/");
                    put("routerfs.mapping.s3a.1.with", "gcs://");
                }}, Collections.singletonList(new DefaultSchemeTranslation("s3a", "s3a-default")),
                        new HashMap<String, String>() {{
                    put("s3a://bucket/boo/a.txt", "gcs://a.txt");
                }}, null},

                {"dst and src mapping prefixes are URI schemes", new HashMap<String, String>() {{
                    put("routerfs.mapping.minio.1.replace", "minio://");
                    put("routerfs.mapping.minio.1.with", "gcs://");
                }},
                        Collections.singletonList(new DefaultSchemeTranslation("s3a", "s3a-default"))
                        , new HashMap<String, String>() {{
                    put("minio://a.txt", "gcs://a.txt");
                }}, null},

                {"Fallback to default Mapping", new HashMap<String, String>() {{
                    put("routerfs.mapping.s3a.1.replace", "s3a://bucket/foo/");
                    put("routerfs.mapping.s3a.1.with", "lakefs://example-repo/b1/");
                    put("routerfs.mapping.s3b.1.replace", "s3b://bucket/foo/");
                    put("routerfs.mapping.s3b.1.with", "lakefs://example-repo/b2/");
                    put("routerfs.mapping.s3c.1.replace", "s3c://bucket/foo/");
                    put("routerfs.mapping.s3c.1.with", "lakefs://example-repo/b3/");
                }}, Stream.of(
                        new DefaultSchemeTranslation("s3a", "s3a-default"),
                        new DefaultSchemeTranslation("s3b", "s3b-default"),
                        new DefaultSchemeTranslation("s3c", "s3c-default")
                ).collect(Collectors.toList()),
                        new HashMap<String, String>() {{
                            put("s3a://bucket/bar/a.txt", "s3a-default://bucket/bar/a.txt");
                            put("s3a://a.txt", "s3a-default://a.txt");
                            put("s3b://bucket/bar/a.txt", "s3b-default://bucket/bar/a.txt");
                            put("s3b://a.txt", "s3b-default://a.txt");
                            put("s3c://bucket/bar/a.txt", "s3c-default://bucket/bar/a.txt");
                            put("s3c://a.txt", "s3c-default://a.txt");
                }}, null},

                {"No default mapping fallback", new HashMap<String, String>() {{
                    put("routerfs.mapping.s3a.1.replace", "s3a://bucket/foo/");
                    put("routerfs.mapping.s3a.1.with", "lakefs://example-repo/b1/");
                    put("routerfs.mapping.s3b.1.replace", "s3b://bucket/foo/");
                    put("routerfs.mapping.s3b.1.with", "lakefs://example-repo/b2/");
                    put("routerfs.mapping.s3c.1.replace", "s3c://bucket/foo/");
                    put("routerfs.mapping.s3c.1.with", "lakefs://example-repo/b3/");
                }}, Collections.singletonList(
                        new DefaultSchemeTranslation("s3d", "s3d-default")
                ),
                        new HashMap<String, String>() {
                            {
                                put("s3a://bucket/bar/a.txt", null);
                                put("s3b://bucket/bar/a.txt", null);
                                put("s3c://bucket/bar/a.txt", null);
                            }}, InvalidPathException.class},

                {"Invalid mapping config index", new HashMap<String, String>() {{
                    put("routerfs.mapping.s3a.notAnInt.replace", "s3a://bucket");
                    put("routerfs.mapping.s3a.1.with", "lakefs://example-repo/b1");}},
                        Collections.singletonList(new DefaultSchemeTranslation("s3a", "s3a-default")),
                        null, InvalidPropertiesFormatException.class},

                {"Invalid mapping config type", new HashMap<String, String>() {{
                    put("routerfs.mapping.s3a.1.notAMappingConfType", "s3a://bucket");
                    put("routerfs.mapping.s3a.1.with", "lakefs://example-repo/b1");}},
                        Collections.singletonList(new DefaultSchemeTranslation("s3a", "s3a-default")),
                        null, InvalidPropertiesFormatException.class},

                {"Missing default defaultFromScheme", new HashMap<String, String>() {{
                    put("routerfs.mapping.s3a.1.replace", "s3a://bucket");
                    put("routerfs.mapping.s3a.1.with", "lakefs://example-repo/b1");}},
                        Collections.singletonList(new DefaultSchemeTranslation(null, "s3a-default")),
                        null, NullPointerException.class},

                {"Missing default defaultToScheme", new HashMap<String, String>() {{
                    put("routerfs.mapping.s3a.1.replace", "s3a://bucket");
                    put("routerfs.mapping.s3a.1.with", "lakefs://example-repo/b1");}},
                        Collections.singletonList(new DefaultSchemeTranslation("s3a", null)),
                        null, NullPointerException.class},

                {"Invalid mapping config fs scheme", new HashMap<String, String>() {{
                    put("routerfs.mapping.#@.1.replace", "#@://bucket");
                    put("routerfs.mapping.#@.1.with", "s3a://boo");}},
                        Collections.singletonList(new DefaultSchemeTranslation("s3a", "s3a-default")),
                        null, InvalidPropertiesFormatException.class},

                {"Invalid mapping source config", new HashMap<String, String>() {{
                    put("routerfs.mapping.lakefs.1.replace", "s3a://bucket");
                    put("routerfs.mapping.lakefs.1.with", "lakefs://boo");}},
                        Collections.singletonList(new DefaultSchemeTranslation("s3a", "s3a-default")),
                        null, InvalidPropertiesFormatException.class},

                {"Empty schemes translation list", new HashMap<String, String>() {{
                    put("routerfs.mapping.lakefs.1.replace", "s3a://bucket");
                    put("routerfs.mapping.lakefs.1.with", "lakefs://boo");}},
                        Collections.emptyList(),
                        null, IllegalArgumentException.class},
        });
    }

    @Test
    public void testMapPath() throws Exception {
        log.info("Starting test: {}", testName);
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
        pathMapper = new PathMapper(conf, defaultSchemeTranslations);
    }
}
