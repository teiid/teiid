/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.translator.jdbc;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.dqp.internal.datamgr.ExecutionContextImpl;
import org.teiid.dqp.internal.datamgr.TestDeleteImpl;
import org.teiid.dqp.internal.datamgr.TestInsertImpl;
import org.teiid.dqp.internal.datamgr.TestProcedureImpl;
import org.teiid.dqp.internal.datamgr.TestQueryImpl;
import org.teiid.dqp.internal.datamgr.TestUpdateImpl;
import org.teiid.dqp.internal.datamgr.TstLanguageBridgeFactory;
import org.teiid.language.LanguageObject;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;

/**
 */
public class TestSQLConversionVisitor {

    public static final ExecutionContext context = new ExecutionContextImpl("VDB",  //$NON-NLS-1$
                                                                            1, 
                                                                            "Payload",  //$NON-NLS-1$
                                                                            "ConnectionID",   //$NON-NLS-1$
                                                                            "Connector", //$NON-NLS-1$
                                                                            "RequestID",  //$NON-NLS-1$ 
                                                                            "PartID",  //$NON-NLS-1$ 
                                                                            "ExecCount");     //$NON-NLS-1$ 
    
    private static JDBCExecutionFactory TRANSLATOR; 
    
    @BeforeClass public static void oneTimeSetup() throws TranslatorException {
    	TRANSLATOR = new JDBCExecutionFactory();
    	TRANSLATOR.setTrimStrings(true);
    	TRANSLATOR.setUseBindVariables(false);
    	TRANSLATOR.start();
    }
    
    public String getTestVDB() {
        return TranslationHelper.PARTS_VDB;
    }
    
    public void helpTestVisitor(String vdb, String input, String expectedOutput) {
        helpTestVisitor(vdb, input, expectedOutput, false);
    }
    
    public void helpTestVisitor(String vdb, String input, String expectedOutput, boolean usePreparedStatement) {
    	JDBCExecutionFactory trans = new JDBCExecutionFactory();
    	trans.setUseBindVariables(usePreparedStatement);
        try {
			trans.start();
	        TranslationHelper.helpTestVisitor(vdb, input, expectedOutput, trans);
		} catch (TranslatorException e) {
			throw new RuntimeException(e);
		}
    }
    
    public static final RuntimeMetadata metadata = TstLanguageBridgeFactory.metadataFactory;

    private String getStringWithContext(LanguageObject obj) throws TranslatorException {
    	JDBCExecutionFactory env = new JDBCExecutionFactory();
    	env.setUseCommentsInSourceQuery(true);
    	env.setUseBindVariables(false);
        env.start();
        
        SQLConversionVisitor visitor = env.getSQLConversionVisitor();
        visitor.setExecutionContext(context);
        visitor.append(obj);
        return visitor.toString();
    }  
    
    @Test public void testSimple() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testAliasInSelect() {
        helpTestVisitor(getTestVDB(),
            "select part_name as x from parts", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME AS x FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testAliasedGroup() {
        helpTestVisitor(getTestVDB(),
            "select y.part_name from parts y", //$NON-NLS-1$
            "SELECT y.PART_NAME FROM PARTS AS y"); //$NON-NLS-1$
    }

    @Test public void testAliasedGroupAndElement() {
        helpTestVisitor(getTestVDB(),
            "select y.part_name AS z from parts y", //$NON-NLS-1$
            "SELECT y.PART_NAME AS z FROM PARTS AS y"); //$NON-NLS-1$
    }

    @Test public void testLiteralString() {
        helpTestVisitor(getTestVDB(),
            "select 'x' from parts", //$NON-NLS-1$
            "SELECT 'x' FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testLiteralInteger() {
        helpTestVisitor(getTestVDB(),
            "select 5 from parts", //$NON-NLS-1$
            "SELECT 5 FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testLiteralFloat() {
        helpTestVisitor(getTestVDB(),
            "select 5.2 from parts", //$NON-NLS-1$
            "SELECT 5.2 FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testLiteralLowFloat() {
        helpTestVisitor(getTestVDB(),
            "select 0.012 from parts", //$NON-NLS-1$
            "SELECT 0.012 FROM PARTS"); //$NON-NLS-1$
    }
    
    @Test public void testLiteralLowFloat2() {
        helpTestVisitor(getTestVDB(),
            "select 0.00012 from parts", //$NON-NLS-1$
            "SELECT 0.00012 FROM PARTS"); //$NON-NLS-1$
    }    
    
    @Test public void testLiteralHighFloat() {
        helpTestVisitor(getTestVDB(),
            "select 12345.123 from parts", //$NON-NLS-1$
            "SELECT 12345.123 FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testLiteralHighFloat2() {
        helpTestVisitor(getTestVDB(),
            "select 1234567890.1234567 from parts", //$NON-NLS-1$
            "SELECT 1234567890.1234567 FROM PARTS"); //$NON-NLS-1$
    }
    
    @Test public void testLiteralBoolean() {
        helpTestVisitor(getTestVDB(),
            "select {b'true'}, {b'false'} from parts", //$NON-NLS-1$
            "SELECT 1, 0 FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testLiteralDate() {
        helpTestVisitor(getTestVDB(),
            "select {d '2003-12-31'} from parts", //$NON-NLS-1$
            "SELECT {d '2003-12-31'} FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testLiteralTime() {
        helpTestVisitor(getTestVDB(),
            "select {t '23:59:59'} from parts", //$NON-NLS-1$
            "SELECT {t '23:59:59'} FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testLiteralNull() {
        helpTestVisitor(getTestVDB(),
            "select null from parts", //$NON-NLS-1$
            "SELECT NULL FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testLiteralTimestamp() {
        helpTestVisitor(getTestVDB(),
            "select {ts '2003-12-31 23:59:59.123'} from parts", //$NON-NLS-1$
            "SELECT {ts '2003-12-31 23:59:59.123'} FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testSQL89Join() {
        helpTestVisitor(getTestVDB(),
            "select p.part_name from parts p, supplier_parts s where p.part_id = s.part_id", //$NON-NLS-1$
            "SELECT p.PART_NAME FROM PARTS AS p, SUPPLIER_PARTS AS s WHERE p.PART_ID = s.PART_ID"); //$NON-NLS-1$
    }

    @Test public void testSQL92Join() {
        helpTestVisitor(getTestVDB(),
            "select p.part_name from parts p join supplier_parts s on p.part_id = s.part_id", //$NON-NLS-1$
            "SELECT p.PART_NAME FROM PARTS AS p INNER JOIN SUPPLIER_PARTS AS s ON p.PART_ID = s.PART_ID"); //$NON-NLS-1$
    }

    @Test public void testSelfJoin() {
        helpTestVisitor(getTestVDB(),
            "select p.part_name from parts p join parts p2 on p.part_id = p2.part_id", //$NON-NLS-1$
            "SELECT p.PART_NAME FROM PARTS AS p INNER JOIN PARTS AS p2 ON p.PART_ID = p2.PART_ID"); //$NON-NLS-1$
    }

    @Test public void testRightOuterJoin() {
        helpTestVisitor(getTestVDB(),
            "select p.part_name from parts p right join supplier_parts s on p.part_id = s.part_id", //$NON-NLS-1$
            "SELECT p.PART_NAME FROM SUPPLIER_PARTS AS s LEFT OUTER JOIN PARTS AS p ON p.PART_ID = s.PART_ID"); //$NON-NLS-1$
    }

    @Test public void testLeftOuterJoin() {
        helpTestVisitor(getTestVDB(),
            "select p.part_name from parts p left join supplier_parts s on p.part_id = s.part_id", //$NON-NLS-1$
            "SELECT p.PART_NAME FROM PARTS AS p LEFT OUTER JOIN SUPPLIER_PARTS AS s ON p.PART_ID = s.PART_ID"); //$NON-NLS-1$
    }

    @Test public void testFullOuterJoin() {
        helpTestVisitor(getTestVDB(),
            "select p.part_name from parts p full join supplier_parts s on p.part_id = s.part_id", //$NON-NLS-1$
            "SELECT p.PART_NAME FROM PARTS AS p FULL OUTER JOIN SUPPLIER_PARTS AS s ON p.PART_ID = s.PART_ID"); //$NON-NLS-1$
    }

    @Test public void testCompare1() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id = 'x'", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID = 'x'"); //$NON-NLS-1$
    }

    @Test public void testCompare2() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id <> 'x'", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID <> 'x'"); //$NON-NLS-1$
    }

    @Test public void testCompare3() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id < 'x'", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID < 'x'"); //$NON-NLS-1$
    }

    @Test public void testCompare4() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id <= 'x'", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID <= 'x'"); //$NON-NLS-1$
    }

    @Test public void testCompare5() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id > 'x'", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID > 'x'"); //$NON-NLS-1$
    }

    @Test public void testCompare6() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id >= 'x'", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID >= 'x'"); //$NON-NLS-1$
    }

    @Test public void testIn1() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id in ('x')", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID = 'x'"); //$NON-NLS-1$
    }

    @Test public void testIn2() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id in ('x', 'y')", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID IN ('x', 'y')"); //$NON-NLS-1$
    }

    @Test public void testIn3() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id not in ('x', 'y')", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID NOT IN ('x', 'y')"); //$NON-NLS-1$
    }

    @Test public void testIsNull1() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id is null", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID IS NULL"); //$NON-NLS-1$
    }

    @Test public void testIsNull2() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id is not null", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID IS NOT NULL"); //$NON-NLS-1$
    }

    @Test public void testInsertNull() {
        helpTestVisitor(getTestVDB(),
            "insert into parts (part_id, part_name, part_color, part_weight) values ('a', null, 'c', 'd')", //$NON-NLS-1$
            "INSERT INTO PARTS (PART_ID, PART_NAME, PART_COLOR, PART_WEIGHT) VALUES ('a', NULL, 'c', 'd')"); //$NON-NLS-1$
    }

    @Test public void testUpdateNull() {
        helpTestVisitor(getTestVDB(),
            "update parts set part_weight = null where part_color = 'b'", //$NON-NLS-1$
            "UPDATE PARTS SET PART_WEIGHT = NULL WHERE PARTS.PART_COLOR = 'b'"); //$NON-NLS-1$
    }

    @Test public void testUpdateWhereNull() {
        helpTestVisitor(getTestVDB(),
            "update parts set part_weight = 'a' where part_weight = null", //$NON-NLS-1$
            "UPDATE PARTS SET PART_WEIGHT = 'a' WHERE NULL <> NULL"); //$NON-NLS-1$
    }

    @Test public void testPreparedStatementCreationWithUpdate() {
        helpTestVisitor(getTestVDB(),
                        "update parts set part_weight = 'a' || 'b' where part_weight < 50/10", //$NON-NLS-1$
                        "UPDATE PARTS SET PART_WEIGHT = ? WHERE PARTS.PART_WEIGHT < ?", //$NON-NLS-1$
                        true); 
    }
    
    @Test public void testPreparedStatementCreationWithInsert() {
        helpTestVisitor(getTestVDB(),
                        "insert into parts (part_weight) values (50/10)", //$NON-NLS-1$
                        "INSERT INTO PARTS (PART_WEIGHT) VALUES (?)", //$NON-NLS-1$
                        true); 
    }
    
    @Test public void testPreparedStatementCreationWithSelect() {
        helpTestVisitor(getTestVDB(),
                        "select part_name from parts where part_id not in ('x' || 'a', 'y' || 'b') and part_weight < '6'", //$NON-NLS-1$
                        "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID NOT IN (?, ?) AND PARTS.PART_WEIGHT < '6'", //$NON-NLS-1$
                        true); 
    }
    
    @Test public void testPreparedStatementCreationWithLike() {
        helpTestVisitor(getTestVDB(),
                        "select part_name from parts where part_name like '%foo' || '_'", //$NON-NLS-1$
                        "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_NAME LIKE ?", //$NON-NLS-1$
                        true); 
    }
    
    /**
     * ideally this should not happen, but to be on the safe side 
     * only the right side should get replaced
     */
    public void defer_testPreparedStatementCreationWithLeftConstant() {
        helpTestVisitor(getTestVDB(),
                        "select part_name from parts where 'x' = 'y'", //$NON-NLS-1$
                        "SELECT PARTS.PART_NAME FROM PARTS WHERE 1 = ?", //$NON-NLS-1$
                        true); 
    }
    
    /**
     * In the future, functions can be made smarter about which of their literal arguments
     * either are (or are not) eligible to be bind variables 
     */
    @Test public void testPreparedStatementCreationWithFunction() {
        helpTestVisitor(getTestVDB(),
                        "select part_name from parts where concat(part_name, 'x') = concat('y', part_weight)", //$NON-NLS-1$
                        "SELECT PARTS.PART_NAME FROM PARTS WHERE concat(PARTS.PART_NAME, 'x') = concat('y', PARTS.PART_WEIGHT)", //$NON-NLS-1$
                        true); 
    }
    
    @Test public void testPreparedStatementCreationWithCase() {
        helpTestVisitor(getTestVDB(),
                        "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_WEIGHT = CASE WHEN PARTS.PART_NAME='a' || 'b' THEN 'b' ELSE 'c' END", //$NON-NLS-1$
                        "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_WEIGHT = CASE WHEN PARTS.PART_NAME = ? THEN 'b' ELSE 'c' END", //$NON-NLS-1$
                        true); 
    }

    @Test public void testVisitIDeleteWithComment() throws Exception {
        String expected = "DELETE /*teiid sessionid:ConnectionID, requestid:RequestID.PartID*/ FROM g1 WHERE 100 >= 200 AND 500 < 600"; //$NON-NLS-1$
        assertEquals(expected, getStringWithContext(TestDeleteImpl.example()));
    }

    @Test public void testVisitIInsertWithComment() throws Exception {
        String expected = "INSERT /*teiid sessionid:ConnectionID, requestid:RequestID.PartID*/ INTO g1 (e1, e2, e3, e4) VALUES (1, 2, 3, 4)"; //$NON-NLS-1$
        assertEquals(expected, getStringWithContext(TestInsertImpl.example("g1"))); //$NON-NLS-1$
    }  
    
    @Test public void testVisitISelectWithComment() throws Exception {
        String expected = "SELECT /*teiid sessionid:ConnectionID, requestid:RequestID.PartID*/ g1.e1, g1.e2, g1.e3, g1.e4 FROM g1, g2 AS myAlias, g3, g4 WHERE 100 >= 200 AND 500 < 600 GROUP BY g1.e1, g1.e2, g1.e3, g1.e4 HAVING 100 >= 200 AND 500 < 600 ORDER BY g1.e1, g1.e2 DESC, g1.e3, g1.e4 DESC"; //$NON-NLS-1$
        assertEquals(expected, getStringWithContext(TestQueryImpl.example(false)));
        expected = "SELECT /*teiid sessionid:ConnectionID, requestid:RequestID.PartID*/ DISTINCT g1.e1, g1.e2, g1.e3, g1.e4 FROM g1, g2 AS myAlias, g3, g4 WHERE 100 >= 200 AND 500 < 600 GROUP BY g1.e1, g1.e2, g1.e3, g1.e4 HAVING 100 >= 200 AND 500 < 600 ORDER BY g1.e1, g1.e2 DESC, g1.e3, g1.e4 DESC"; //$NON-NLS-1$
        assertEquals(expected, getStringWithContext(TestQueryImpl.example(true)));
    }
    
    @Test public void testVisitIUpdateWithComment() throws Exception {
        String expected = "UPDATE /*teiid sessionid:ConnectionID, requestid:RequestID.PartID*/ g1 SET e1 = 1, e2 = 1, e3 = 1, e4 = 1 WHERE 1 = 1"; //$NON-NLS-1$
        assertEquals(expected, getStringWithContext(TestUpdateImpl.example()));
    }    
    
    @Test public void testVisitIProcedureWithComment() throws Exception {
        String expected = "{ /*teiid sessionid:ConnectionID, requestid:RequestID.PartID*/  call sq3(?,?)}"; //$NON-NLS-1$
        assertEquals(expected, getStringWithContext(TestProcedureImpl.example()));
    }  
    
    @Test public void testTrimStrings() throws Exception {
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, "select stringkey from bqt1.smalla", "SELECT rtrim(SmallA.StringKey) FROM SmallA", TRANSLATOR); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testNestedSetQuery() throws Exception {
    	String input = "select part_id id FROM parts UNION ALL (select part_name FROM parts UNION select part_id FROM parts)"; //$NON-NLS-1$
        String output = "SELECT rtrim(PARTS.PART_ID) AS id FROM PARTS UNION ALL (SELECT PARTS.PART_NAME FROM PARTS UNION SELECT rtrim(PARTS.PART_ID) FROM PARTS)"; //$NON-NLS-1$
          
        TranslationHelper.helpTestVisitor(TranslationHelper.PARTS_VDB,
            input, 
            output, TRANSLATOR);
    }
    
    @Test public void testNestedSetQuery1() throws Exception {
    	String input = "select part_id id FROM parts UNION (select part_name FROM parts EXCEPT select part_id FROM parts)"; //$NON-NLS-1$
        String output = "SELECT rtrim(PARTS.PART_ID) AS id FROM PARTS UNION (SELECT PARTS.PART_NAME FROM PARTS EXCEPT SELECT rtrim(PARTS.PART_ID) FROM PARTS)"; //$NON-NLS-1$
          
        TranslationHelper.helpTestVisitor(TranslationHelper.PARTS_VDB,
            input, 
            output, TRANSLATOR);
    }
    
    @Test public void testNestedSetQuery2() throws Exception {
    	String input = "select part_id id FROM parts UNION select part_name FROM parts EXCEPT select part_id FROM parts"; //$NON-NLS-1$
        String output = "SELECT rtrim(PARTS.PART_ID) AS id FROM PARTS UNION SELECT PARTS.PART_NAME FROM PARTS EXCEPT SELECT rtrim(PARTS.PART_ID) FROM PARTS"; //$NON-NLS-1$
          
        TranslationHelper.helpTestVisitor(TranslationHelper.PARTS_VDB,
            input, 
            output, TRANSLATOR);
    }
    
    @Test public void testNestedSetQuery3() throws Exception {
    	String input = "select part_id id FROM parts UNION (select part_name FROM parts Union ALL select part_id FROM parts)"; //$NON-NLS-1$
        String output = "SELECT rtrim(PARTS.PART_ID) AS id FROM PARTS UNION SELECT PARTS.PART_NAME FROM PARTS UNION SELECT rtrim(PARTS.PART_ID) FROM PARTS"; //$NON-NLS-1$
          
        TranslationHelper.helpTestVisitor(TranslationHelper.PARTS_VDB,
            input, 
            output, TRANSLATOR);
    }
    
    @Test public void testOrderByUnrelated() throws Exception {
    	String input = "select part_id id FROM parts order by part_name"; //$NON-NLS-1$
        String output = "SELECT rtrim(PARTS.PART_ID) AS id FROM PARTS ORDER BY PARTS.PART_NAME"; //$NON-NLS-1$
          
        TranslationHelper.helpTestVisitor(TranslationHelper.PARTS_VDB,
            input, 
            output, TRANSLATOR);
    }

}
