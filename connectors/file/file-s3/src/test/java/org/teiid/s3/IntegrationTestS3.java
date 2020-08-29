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
package org.teiid.s3;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.translator.TranslatorException;

import com.amazonaws.SdkClientException;

/**
 * @See https://docs.min.io/docs/minio-docker-quickstart-guide.html to launch a backend for this test
 */
@SuppressWarnings("nls")
@Ignore
public class IntegrationTestS3 {

    private  S3Connection s3Connection;

    @Before
    public  void setUp() throws TranslatorException {
        S3Configuration config = new S3Configuration() {

            @Override
            public String getSseKey() {
                return null;
            }

            @Override
            public String getSseAlgorithm() {
                return null;
            }

            @Override
            public String getSecretKey() {
                return "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
            }

            @Override
            public String getRegion() {
                return null;
            }

            @Override
            public String getEndpoint() {
                return "http://localhost:9000";
            }

            @Override
            public String getBucket() {
                return "bucket";
            }

            @Override
            public String getAccessKey() {
                return "AKIAIOSFODNN7EXAMPLE";
            }
        };
        S3ConnectionFactory s3ConnectionFactory = new S3ConnectionFactory(config);
        if (!s3ConnectionFactory.getS3Client().doesBucketExistV2("bucket")) {
            s3ConnectionFactory.getS3Client().createBucket("bucket");
        }
        s3Connection = new S3Connection(s3ConnectionFactory.getS3Config(), s3ConnectionFactory.getS3Client());
    }

    @Test
    public void testSearch() throws SdkClientException, TranslatorException {
        s3Connection.add(new ByteArrayInputStream(new byte[0]), "some/directory/file");
        s3Connection.add(new ByteArrayInputStream(new byte[0]), "some/directory/file.txt");
        s3Connection.add(new ByteArrayInputStream(new byte[0]), "some/directory/file2.txt");
        s3Connection.add(new ByteArrayInputStream(new byte[0]), "some/directory/x/filex.txt");
        s3Connection.add(new ByteArrayInputStream(new byte[0]), "some/directory/*/filex.txt");
        s3Connection.add(new ByteArrayInputStream(new byte[0]), "some/otherdir/file3.txt");
        s3Connection.add(new ByteArrayInputStream(new byte[0]), "some/nested/directory/file4.txt");

        org.teiid.file.VirtualFile[] files = s3Connection.getFiles("some");
        assertEquals(0, files.length);

        files = s3Connection.getFiles("some/directory/file");
        assertEquals(1, files.length);
        assertEquals("some/directory/file", files[0].getPath());

        files = s3Connection.getFiles("some/directory");
        assertEquals(3, files.length);

        files = s3Connection.getFiles("some/directory/");
        assertEquals(3, files.length);

        files = s3Connection.getFiles("some/directory*");
        assertEquals(0, files.length);

        files = s3Connection.getFiles("some/directory/*");
        assertEquals(3, files.length);
        assertEquals("some/directory/file", files[0].getPath());

        //exact match
        files = s3Connection.getFiles("some/directory/file.txt");
        assertEquals(1, files.length);
        assertEquals("some/directory/file.txt", files[0].getPath());

        //escaped * match
        files = s3Connection.getFiles("some/directory/**/filex.txt");
        assertEquals(1, files.length);
        assertEquals("some/directory/*/filex.txt", files[0].getPath());

        //search across siblings
        files = s3Connection.getFiles("some/*/*.txt");
        assertEquals(3, files.length);

        //search nested
        files = s3Connection.getFiles("*/nested/*/*.txt");
        assertEquals(1, files.length);
        assertEquals("file4.txt", files[0].getName());
    }

    @After
    public void close() throws Exception {
        s3Connection.close();
    }

}
