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

import org.teiid.translator.TranslatorException;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class S3ConnectionFactory {

    private AmazonS3 s3Client;
    private S3Configuration s3Config;

    public S3ConnectionFactory(S3Configuration s3Config) throws TranslatorException {
        this.s3Config = s3Config;
        if(s3Config.getBucket() == null) {
            throw new TranslatorException("Bucket can't be null.");
        }
        if(s3Config.getAccessKey() == null) {
            throw new TranslatorException("Access key can't be null.");
        }
        if(s3Config.getSecretKey() == null) {
            throw new TranslatorException("Secret key can't be null.");
        }
        AWSCredentials credentials = new BasicAWSCredentials(s3Config.getAccessKey(), s3Config.getSecretKey());
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSignerOverride("AWSS3V4SignerType");
        AmazonS3ClientBuilder standard = AmazonS3ClientBuilder
                .standard();
        if (s3Config.getEndpoint() != null) {
            standard = standard.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s3Config.getEndpoint(), s3Config.getRegion()));
        } else if (s3Config.getRegion() != null) {
            standard = standard.withRegion(s3Config.getRegion());
        }
        s3Client = standard.withPathStyleAccessEnabled(true)
                .withClientConfiguration(clientConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();
    }

    public void close() {
        s3Client.shutdown();
    }

    public AmazonS3 getS3Client() {
        return s3Client;
    }

    public S3Configuration getS3Config() {
        return s3Config;
    }
}
