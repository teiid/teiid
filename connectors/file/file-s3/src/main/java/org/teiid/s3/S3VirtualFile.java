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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.teiid.file.VirtualFile;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SSECustomerKey;

public class S3VirtualFile implements VirtualFile {

    AmazonS3 s3Client;
    S3ObjectSummary summary;
    S3Configuration s3Config;

    public S3VirtualFile(AmazonS3 s3Client, S3ObjectSummary s3ObjectSummary, S3Configuration s3Config) {
        this.s3Client = s3Client;
        this.summary = s3ObjectSummary;
        this.s3Config = s3Config;
    }

    @Override
    public String getPath() {
        String result = summary.getKey();
        if (isDirectory()) {
            return result.substring(0, result.length() - 1);
        }
        return result;
    }

    @Override
    public boolean isDirectory() {
        return summary.getKey().endsWith(S3Connection.SLASH);
    }

    @Override
    public String getName() {
        String path = getPath();
        int index = path.lastIndexOf('/');
        if (index != -1) {
            return path.substring(index + 1);
        }
        return path;
    }

    @Override
    public InputStream openInputStream(boolean b) throws IOException {
        GetObjectRequest request = new GetObjectRequest(summary.getBucketName(), summary.getKey());
        if(s3Config.getSseKey() != null) {
            request.withSSECustomerKey(new SSECustomerKey(s3Config.getSseKey()).withAlgorithm(s3Config.getSseAlgorithm()));
        }
        S3Object object = s3Client.getObject(request);
        return object.getObjectContent();
    }

    @Override
    public OutputStream openOutputStream(boolean b) throws IOException {
        throw new IOException("Output stream is not supported for use in s3.");
    }

    @Override
    public long getLastModified() {
        return summary.getLastModified().getTime();
    }

    @Override
    public long getCreationTime() {
        // no mechanism for creation time provided
        return summary.getLastModified().getTime();
    }

    @Override
    public long getSize() {
        return summary.getSize();
    }
}
