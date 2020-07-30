package org.teiid.translator.dynamodb.execution;

import org.teiid.language.LanguageObject;
import org.teiid.language.visitor.SQLStringVisitor;

public class DynamoDBSQLVisitor extends SQLStringVisitor {

    public static String getSQLString(LanguageObject obj) {
        DynamoDBSQLVisitor visitor = new DynamoDBSQLVisitor();
        visitor.append(obj);
        return visitor.toString();
    }
}
