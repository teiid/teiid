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

import org.teiid.file.VirtualFile;
import org.teiid.file.VirtualFileConnection;
import org.teiid.translator.TranslatorException;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SSECustomerKey;

public class S3Connection implements VirtualFileConnection {

    private static final String STAR = "\\*"; //$NON-NLS-1$
    private static final String SLASH = "/"; //$NON-NLS-1$

    private final S3Configuration s3Config;
    private final AmazonS3 s3Client;

    public S3Connection(S3Configuration s3Config, AmazonS3 s3Client) {
        this.s3Config = s3Config;
        this.s3Client = s3Client;
    }

    @Override
    public VirtualFile[] getFiles(String s) throws TranslatorException {
        if (s == null) {
            return null;
        }
        if(s3Client.doesObjectExist(s3Config.getBucket(), s)){
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                    .withBucketName(s3Config.getBucket())
                    .withPrefix(s);
            ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);
            return new VirtualFile[] {new S3VirtualFile(s3Client, objectListing.getObjectSummaries().get(0), s3Config)};
        }
        if(isDirectory(s)){
            if(!s.isEmpty() && !s.endsWith(SLASH)) {
                s += SLASH;
            }
            return convert(s);
        }
        String parentPath = ""; //$NON-NLS-1$
        if(s.contains(SLASH)) {
            parentPath = s.substring(0, s.lastIndexOf(SLASH) + 1);
        }

        if(!isDirectory(parentPath)) {
            return null;
        }
        if(s.contains(STAR))
        {
            return globSearch(parentPath, s);
        }
        return null;
    }

    private VirtualFile[] globSearch(String parentPath, String s) {
        ArrayList<S3VirtualFile> s3VirtualFiles = new ArrayList<>();
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(s3Config.getBucket())
                .withPrefix(parentPath)
                .withDelimiter(SLASH);
        ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);
        while(objectListing != null) {
            for(S3ObjectSummary s3ObjectSummary : objectListing.getObjectSummaries()) {
                if(!s3ObjectSummary.getKey().endsWith(SLASH)) {
                    if(matchString(s3ObjectSummary.getKey(), s)) {
                        s3VirtualFiles.add(new S3VirtualFile(s3Client, s3ObjectSummary, s3Config));
                    }
                }
            }
            if(!objectListing.isTruncated()) {
                break;
            }
            objectListing = s3Client.listNextBatchOfObjects(objectListing);
        }
        return s3VirtualFiles.toArray(new VirtualFile[s3VirtualFiles.size()]);
    }

    protected boolean matchString(String key, String pattern) {
        String[] stringArray = pattern.split(STAR);
        int prevIndex = 0;
        for(String sub: stringArray){
            int index = key.indexOf(sub, prevIndex);
            int indexOfNextSlash = key.indexOf(SLASH, prevIndex);
            if(index != -1 && (indexOfNextSlash == -1 || indexOfNextSlash >= index)){
                prevIndex = index + sub.length();
            } else {
                return false;
            }
        }
        if(key.indexOf(SLASH, prevIndex) != -1){
            return false;
        }
        if(!pattern.endsWith("*") && !key.endsWith(stringArray[stringArray.length - 1])){
            return false;
        }
        return true;
    }

    private VirtualFile[] convert(String s) {
        ArrayList<S3VirtualFile> s3VirtualFiles = new ArrayList<>();
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(s3Config.getBucket())
                .withPrefix(s)
                .withDelimiter(SLASH);
        ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);
        while(objectListing != null) {
            for(S3ObjectSummary s3ObjectSummary : objectListing.getObjectSummaries()) {
                if(!s3ObjectSummary.getKey().endsWith(SLASH)) {
                    s3VirtualFiles.add(new S3VirtualFile(s3Client,s3ObjectSummary, s3Config));
                }
            }
            if(!objectListing.isTruncated()) {
                break;
            }
            objectListing = s3Client.listNextBatchOfObjects(objectListing);
        }
        return s3VirtualFiles.toArray(new VirtualFile[s3VirtualFiles.size()]);
    }

    private boolean isDirectory(String s) {
        if(!s.endsWith(SLASH)) {
            s += SLASH;
        }
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(s3Config.getBucket())
                .withPrefix(s);
        ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);
        return objectListing.getObjectSummaries().size() > 0;
    }

    @Override
    public void add(InputStream in, String path) throws TranslatorException {
        add(in, path, null);
    }

    @Override
    public void add(InputStream inputStream, String s, FileMetadata fileMetadata)
            throws TranslatorException {
        ObjectMetadata metadata = new ObjectMetadata();
        try{
            if (fileMetadata != null) {
                Long size = fileMetadata.size();
                if (size != null) {
                    metadata.setContentLength(size);
                }
            }
            PutObjectRequest request = new PutObjectRequest(s3Config.getBucket(), s, inputStream, metadata);
            if(s3Config.getSseKey() != null) {
                request.withSSECustomerKey(new SSECustomerKey(s3Config.getSseKey()).withAlgorithm(s3Config.getSseAlgorithm()));
            }
            s3Client.putObject(request);
        } catch (SdkClientException e){
            throw new TranslatorException(e);
        }
    }

    @Override
    public boolean remove(String s) throws TranslatorException {
        try{
            //delete does not check for existence
            if (!s3Client.doesObjectExist(s3Config.getBucket(), s)) {
                return false;
            }
            s3Client.deleteObject(s3Config.getBucket(), s);
        } catch (SdkClientException e){
            return false;
        }
        return true;
    }

    @Override
    public void close() {

    }
}
