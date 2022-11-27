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
package org.teiid.resource.adapter.s3;

import java.util.Objects;

import javax.resource.ResourceException;

import org.teiid.core.BundleUtil;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;
import org.teiid.resource.spi.ResourceConnection;
import org.teiid.s3.S3Configuration;
import org.teiid.s3.S3Connection;
import org.teiid.s3.S3ConnectionFactory;
import org.teiid.translator.TranslatorException;

import com.amazonaws.services.s3.AmazonS3;

public class S3ManagedConnectionFactory extends BasicManagedConnectionFactory implements S3Configuration {

    private static final long serialVersionUID = -1L;

    public static final BundleUtil UTIL = BundleUtil.getBundleUtil(S3ManagedConnectionFactory.class);

    private static class S3ResourceConnection extends S3Connection implements ResourceConnection {

        public S3ResourceConnection(S3Configuration config, AmazonS3 client) {
            super(config, client);
        }

    }

    private String accessKey;
    private String secretKey;
    private String bucket;
    private String region;
    private String sseAlgorithm = "AES256"; //$NON-NLS-1$
    private String sseKey;   // base64-encoded key
    private String endpoint;

    @Override
    public BasicConnectionFactory<ResourceConnection> createConnectionFactory()
            throws ResourceException {
        S3ConnectionFactory s3ConnectionFactory;
        try {
            s3ConnectionFactory = new S3ConnectionFactory(S3ManagedConnectionFactory.this);

            return new BasicConnectionFactory<ResourceConnection>() {

                @Override
                public ResourceConnection getConnection() throws ResourceException {
                    return new S3ResourceConnection(s3ConnectionFactory.getS3Config(), s3ConnectionFactory.getS3Client());
                }
            };
        } catch (TranslatorException e) {
            throw new ResourceException(e);
        }
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getSseAlgorithm() {
        return sseAlgorithm;
    }

    public void setSseAlgorithm(String sseAlgorithm) {
        this.sseAlgorithm = sseAlgorithm;
    }

    public String getSseKey() {
        return sseKey;
    }

    public void setSseKey(String sseKey) {
        this.sseKey = sseKey;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public int hashCode() {
        return Objects.hash(endpoint, bucket);
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof S3ManagedConnectionFactory)) {
            return false;
        }
        S3ManagedConnectionFactory other = (S3ManagedConnectionFactory) obj;
        return Objects.equals(accessKey, other.accessKey)
                && Objects.equals(secretKey, other.secretKey)
                && Objects.equals(bucket, other.bucket)
                && Objects.equals(endpoint, other.endpoint)
                && Objects.equals(region, other.region)
                && Objects.equals(sseAlgorithm, other.sseAlgorithm)
                && Objects.equals(sseKey, other.sseKey);
    }

}
