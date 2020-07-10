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

package org.teiid.translator.simpledb.api;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import org.teiid.translator.TranslatorException;

public class SimpleDBConnectionFactory {
    private BaseSimpleDBConfiguration simpleDBConfig;
    private AmazonSimpleDB simpleDBClient;

    public SimpleDBConnectionFactory(BaseSimpleDBConfiguration simpleDBConfig) throws TranslatorException {
        this.simpleDBConfig = simpleDBConfig;
        if(simpleDBConfig.getAccessKey() == null) {
            throw new TranslatorException("Access key can't be null.");
        }
        if(simpleDBConfig.getSecretAccessKey() == null) {
            throw new TranslatorException("Secret key can't be null.");
        }

        AWSCredentials credentials = new BasicAWSCredentials(simpleDBConfig.getAccessKey(), simpleDBConfig.getSecretAccessKey());
        simpleDBClient = AmazonSimpleDBClient
                            .builder()
                            .standard()
                            .withCredentials(new AWSStaticCredentialsProvider(credentials))
                            .build();
    }

    public AmazonSimpleDB getS3Client() {
        return simpleDBClient;
    }

    public BaseSimpleDBConfiguration getS3Config() {
        return simpleDBConfig;
    }
}
