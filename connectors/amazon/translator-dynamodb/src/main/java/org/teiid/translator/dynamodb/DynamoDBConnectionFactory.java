package org.teiid.translator.dynamodb;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
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
