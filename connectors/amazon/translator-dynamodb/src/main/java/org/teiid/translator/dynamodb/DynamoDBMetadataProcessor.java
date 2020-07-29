package org.teiid.translator.dynamodb;

import org.teiid.metadata.MetadataFactory;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.dynamodb.api.DynamoDBConnection;

public class DynamoDBMetadataProcessor implements MetadataProcessor<DynamoDBConnection> {
    @Override
    public void process(MetadataFactory metadataFactory, DynamoDBConnection connection) throws TranslatorException {

    }
}
