/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.file.VirtualFile;
import org.teiid.translator.TranslatorException;

/**
 * this was initially written against the full set of hadoop jars. using the shaded client it won't work
 * see https://issues.apache.org/jira/browse/HADOOP-15924?page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel&focusedCommentId=16685144#comment-16685144
 * an integration test was created instead.
 * this can still be used, but requires setting up an hdfs instance.  See IntegrationTestHdfs.main
 */
@Ignore
@SuppressWarnings("nls")
public class TestHdfsConnection {

    private static HdfsConnection CONNECTION;

    @BeforeClass
    public static void setUp() throws Exception {
        CONNECTION = new HdfsConnection(new HdfsConnectionFactory(new HdfsConfiguration() {

            @Override
            public String getResourcePath() {
                return null;
            }

            @Override
            public String getFsUri() {
                return "hdfs://localhost:9000";
            }
        }));
    }

    @Test
    public void testAdd() throws TranslatorException {
        addFile("folder1/hello");
        VirtualFile[] virtualFiles = CONNECTION.getFiles("folder1/hello");
        assertEquals("The test fails", "hello", virtualFiles[0].getName());
    }

    private void addFile(String name) throws TranslatorException {
        InputStream inputStream = new ByteArrayInputStream("hello world".getBytes(Charset.forName("UTF-8")));
        CONNECTION.add(inputStream, name);
    }

    @Test
    public void testDeleteFile() throws TranslatorException {
        String file = "main/hello";
        assertFalse(CONNECTION.remove(file));
        addFile(file);
        assertTrue(CONNECTION.remove(file));
        VirtualFile[] virtualFiles = CONNECTION.getFiles(file);
        assertNull(virtualFiles);
    }

    @Test
    public void testSearch() throws TranslatorException {
        CONNECTION.remove("/user/aditya/afile.txt");
        CONNECTION.remove("/user/aditya/bfile.txt");
        CONNECTION.remove("/user/aditya/otherfile");
        VirtualFile[] virtualFiles = CONNECTION.getFiles("/user/aditya/*.txt");
        assertTrue(virtualFiles == null || virtualFiles.length == 0);
        addFile("/user/aditya/afile.txt");
        addFile("/user/aditya/bfile.txt");
        addFile("/user/aditya/otherfile");
        //only text files
        virtualFiles = CONNECTION.getFiles("/user/aditya/*.txt");
        assertEquals(2, virtualFiles.length);
        assertFalse(virtualFiles[0].isDirectory());
        assertTrue(virtualFiles[0].getPath().startsWith("/user/aditya/"));
        //list directory
        virtualFiles = CONNECTION.getFiles("/user/aditya/");
        assertEquals(3, virtualFiles.length);

        virtualFiles = CONNECTION.getFiles("/*/a*");
        assertEquals(1, virtualFiles.length);
        assertTrue(virtualFiles[0].isDirectory());

        virtualFiles = CONNECTION.getFiles("/*/*/*other*");
        assertEquals(1, virtualFiles.length);
        assertFalse(virtualFiles[0].isDirectory());
    }

    @AfterClass
    public static void teardown() throws Exception{
        if (CONNECTION != null) {
            CONNECTION.close();
        }
    }
}
