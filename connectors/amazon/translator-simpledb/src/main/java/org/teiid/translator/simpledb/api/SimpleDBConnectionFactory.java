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

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.TranslatorException;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;

public class SimpleDBConnectionFactory {
    private final BaseSimpleDBConfiguration simpleDBConfig;
    private final AWSStaticCredentialsProvider awsStaticCredentialsProvider;
    private AmazonSimpleDB simpleDBClient;

    public SimpleDBConnectionFactory(BaseSimpleDBConfiguration simpleDBConfig) throws TranslatorException {
        this.simpleDBConfig = simpleDBConfig;
        if(simpleDBConfig.getAccessKey() == null) {
            throw new TranslatorException("Access key can't be null.");
        }
        if(simpleDBConfig.getSecretKey() == null) {
            throw new TranslatorException("Secret key can't be null.");
        }

        AWSCredentials credentials = new BasicAWSCredentials(simpleDBConfig.getAccessKey(), simpleDBConfig.getSecretKey());
        awsStaticCredentialsProvider = new AWSStaticCredentialsProvider(credentials);
        try {
            getSimpleDBClient();
        } catch (SdkClientException e) {
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Failed to make initial simpledb connection", e); //$NON-NLS-1$
        }
    }

    public AmazonSimpleDB getSimpleDBClient() {
        if (simpleDBClient == null) {
            synchronized (this) {
                if (simpleDBClient == null) {
                    simpleDBClient = AmazonSimpleDBClient
                            .builder()
                            .withCredentials(awsStaticCredentialsProvider)
                            .build();
                }
            }
        }
        return simpleDBClient;
    }

    public BaseSimpleDBConfiguration getSimpleDBConfig() {
        return simpleDBConfig;
    }
}
