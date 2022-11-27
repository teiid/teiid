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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.file.VirtualFile;
import org.teiid.translator.TranslatorException;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

@SuppressWarnings("nls")
public class TestS3Connection {

    private  S3Connection s3Connection;

    private AmazonS3Client amazonS3Client = Mockito.mock(AmazonS3Client.class);

    @Before
    public  void setUp() {
        s3Connection = new S3Connection(Mockito.mock(S3Configuration.class), amazonS3Client);
    }

    @Test
    public void testAdd() throws SdkClientException, TranslatorException {
          InputStream inputStream = Mockito.mock(InputStream.class);
          s3Connection.add(inputStream, "");
    }

    @Test
    public void testDeleteFile() throws TranslatorException {
        Assert.assertFalse(s3Connection.remove("sl"));
    }

    @Test
    public void testGetFilesAndConvert() throws TranslatorException {
        ObjectListing objectListing = Mockito.mock(ObjectListing.class);
        Mockito.when(amazonS3Client.listObjects(Mockito.any(ListObjectsRequest.class))).thenReturn(objectListing);
        List<S3ObjectSummary> objectSummaryList = new ArrayList<>();
        S3ObjectSummary summary1 = Mockito.mock(S3ObjectSummary.class);
        S3ObjectSummary summary2 = Mockito.mock(S3ObjectSummary.class);
        Mockito.when(summary1.getKey()).thenReturn("folder1/folder2/file");
        Mockito.when(summary2.getKey()).thenReturn("folder1/folder2/file1");
        objectSummaryList.add(summary1);
        objectSummaryList.add(summary2);
        Mockito.when(objectListing.getObjectSummaries()).thenReturn(objectSummaryList);
        VirtualFile[] virtualFiles = s3Connection.getFiles("folder1/folder2");
        Assert.assertEquals(2, virtualFiles.length);
        virtualFiles = s3Connection.getFiles("folder1/folder2/");
        Assert.assertEquals(2, virtualFiles.length);
    }

    @Test
    public void testMatchString() {
        Assert.assertTrue(matchString("folder1/sample", "folder1/samp*"));
        Assert.assertTrue(matchString("dddd", "dd*d"));
        Assert.assertTrue(matchString("folder1/sample", "folder1/*le"));
        Assert.assertFalse(matchString("folder1/sample", "folder1/**le"));
        Assert.assertTrue(matchString("folder1/*le", "folder1/**le"));
        Assert.assertFalse(matchString("folder1/sample", "folder1/san*"));
        Assert.assertTrue(matchString("year=2020/month=January/week=1/day=Monday", "year=2020/*/*/day=Monday"));
        Assert.assertTrue(matchString("year=2020/month=January/week=1/day=Monday", "year=2020/month=*/*/day=Monday"));
        Assert.assertTrue(matchString("year=2020/month=January/week=1/day=Monday", "year=2020/*/*/*"));
        Assert.assertFalse(matchString("year=2020/month=January/week=1/day=Monday", "year=2020/*/*/day=Mondays"));
        Assert.assertFalse(matchString("year=2020/month=January/week=1/day=Monday/page=1", "year=2020/*/*/*"));
        Assert.assertTrue(matchString("year=2020/month=January/week=1/day=Monday", "year=2020/month=*Jan*/*/day=Mo*day"));
        Assert.assertFalse(matchString("year=2020/month=January/jkl=fyz/week=1/day=Monday", "year=2020/month=*/*/day=Monday"));
        Assert.assertFalse(matchString("year=2020/month=January/week=1/day=Mondays", "year=2020/*/*/day=Monday"));
    }

    @After
    public void close() throws Exception {
        s3Connection.close();
    }

    protected boolean matchString(String key, String remainingPath) {
        Pattern pattern = s3Connection.convertPathToPattern(remainingPath);
        return pattern.matcher(key).matches();
    }

}
