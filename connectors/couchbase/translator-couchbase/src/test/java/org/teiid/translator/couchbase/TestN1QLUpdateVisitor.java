package org.teiid.translator.couchbase;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.teiid.language.Command;
import org.teiid.translator.TranslatorException;

public class TestN1QLUpdateVisitor extends TestVisitor {
    
    private void helpTest(String sql, String key) throws TranslatorException {
        Command command = translationUtility.parseCommand(sql);

        N1QLUpdateVisitor visitor = TRANSLATOR.getN1QLUpdateVisitor();
        visitor.append(command);
        String actual = visitor.toString();
        
        if(PRINT_TO_CONSOLE.booleanValue()) {
            System.out.println(actual);
        }
        
        if(REPLACE_EXPECTED.booleanValue()) {
            N1QL.put(key.toString(), actual);
        }
        
        assertEquals(key, N1QL.getProperty(key, ""), actual);
    }
    
    @Test
    public void testInsert() throws TranslatorException  {
        
        String sql = "INSERT INTO Customer (documentID, ID, Name, type) values ('customer-1', 'Customer_12346', 'Kylin Soong', 'Customer')";
        helpTest(sql, "N1QL1401");
    }

}
