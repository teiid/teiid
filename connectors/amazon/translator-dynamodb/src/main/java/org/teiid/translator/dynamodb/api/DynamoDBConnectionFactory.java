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
package org.teiid.translator.dynamodb.api;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import org.teiid.translator.TranslatorException;

public class DynamoDBConnectionFactory {
    private final DynamoDBConfiguration dynamoDBConfiguration;
    private final AWSStaticCredentialsProvider awsStaticCredentialsProvider;
    private AmazonDynamoDB dynamoDBClient;


    public DynamoDBConnectionFactory(DynamoDBConfiguration dynamoDBConfiguration) throws TranslatorException {
        this.dynamoDBConfiguration = dynamoDBConfiguration;

        if(dynamoDBConfiguration.getAccessKey() == null) {
            throw new TranslatorException("Access key can't be null.");
        }
        if(dynamoDBConfiguration.getSecretKey() == null) {
            throw new TranslatorException("Secret key can't be null");
        }

        AWSCredentials credentials = new BasicAWSCredentials(dynamoDBConfiguration.getAccessKey(), dynamoDBConfiguration.getSecretKey());
        awsStaticCredentialsProvider = new AWSStaticCredentialsProvider(credentials);


    }

    public AmazonDynamoDB getDynamoDBClient() {
        if (dynamoDBClient == null) {
            synchronized (this) {
                if (dynamoDBClient == null) {
                    dynamoDBClient = AmazonDynamoDBClient
                            .builder()
                            .withCredentials(awsStaticCredentialsProvider)
                            .build();
                }
            }
        }
        return dynamoDBClient;
    }

    public DynamoDBConfiguration getDynamoDBConfiguration() {
        return this.dynamoDBConfiguration;
    }
}
