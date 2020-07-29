package org.teiid.translator.dynamodb;

import org.teiid.resource.api.ConnectionFactory;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.Translator;
import org.teiid.translator.dynamodb.api.DynamoDBConnection;

@Translator(name = "dynamodb", description = "Translator for Dynamo DB")
public class DynamoDBExecutionFactory extends ExecutionFactory<ConnectionFactory, DynamoDBConnection> {

}
