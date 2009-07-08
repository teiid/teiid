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

package com.metamatrix.connector.jdbc.extension;

import java.util.Properties;

import junit.framework.TestCase;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.jdbc.JDBCPropertyNames;
import org.teiid.connector.jdbc.translator.SQLConversionVisitor;
import org.teiid.connector.jdbc.translator.TranslatedCommand;
import org.teiid.connector.jdbc.translator.Translator;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.ILanguageObject;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;
import org.teiid.dqp.internal.datamgr.impl.ExecutionContextImpl;
import org.teiid.dqp.internal.datamgr.impl.FakeExecutionContextImpl;
import org.teiid.dqp.internal.datamgr.language.LanguageBridgeFactory;
import org.teiid.dqp.internal.datamgr.language.TestDeleteImpl;
import org.teiid.dqp.internal.datamgr.language.TestInsertImpl;
import org.teiid.dqp.internal.datamgr.language.TestProcedureImpl;
import org.teiid.dqp.internal.datamgr.language.TestSelectImpl;
import org.teiid.dqp.internal.datamgr.language.TestUpdateImpl;
import org.teiid.dqp.internal.datamgr.language.TstLanguageBridgeFactory;
import org.teiid.metadata.index.VDBMetadataFactory;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.cdk.api.TranslationUtility;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.rewriter.QueryRewriter;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.From;
import com.metamatrix.query.sql.lang.GroupBy;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.Select;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.GroupSymbol;

/**
 */
public class TestSQLConversionVisitor extends TestCase {

    public static final ExecutionContext context = new ExecutionContextImpl("VDB",  //$NON-NLS-1$
                                                                            "Version",  //$NON-NLS-1$
                                                                            "User",  //$NON-NLS-1$ 
                                                                            "Payload",  //$NON-NLS-1$
                                                                            "ExecutionPayload",  //$NON-NLS-1$            
                                                                            "ConnectionID",   //$NON-NLS-1$
                                                                            "Connector",
                                                                            "RequestID", "PartID", "ExecCount");    
    /**
     * Constructor for TestSQLConversionVisitor.
     * @param name
     */
    public TestSQLConversionVisitor(String name) {
        super(name);
    }

    public String getTestVDB() {
        return UnitTestUtil.getTestDataPath() + "/partssupplier/PartsSupplier.vdb"; //$NON-NLS-1$
    }
    
    public ICommand helpTranslate(String vdbFileName, String sql) {
        TranslationUtility util = new TranslationUtility(vdbFileName);
        return util.parseCommand(sql);
    }

    public void helpTestVisitor(String vdb, String input, String expectedOutput) {
        helpTestVisitor(vdb, input, expectedOutput, false);
    }
    
    public void helpTestVisitor(String vdb, String input, String expectedOutput, boolean useMetadata) {
        helpTestVisitor(vdb, input, expectedOutput, useMetadata, false);
    }
    
    public void helpTestVisitor(String vdb, String input, String expectedOutput, boolean useMetadata, boolean usePreparedStatement) {
        // Convert from sql to objects
        ICommand obj = helpTranslate(vdb, input);

        try {
			helpTestVisitorWithCommand(expectedOutput, obj, useMetadata, usePreparedStatement);
		} catch (ConnectorException e) {
			throw new RuntimeException(e);
		}    	
    }
    public static final RuntimeMetadata metadata = TstLanguageBridgeFactory.metadataFactory;

    private String getStringWithContext(ILanguageObject obj) throws ConnectorException {
        Properties props = new Properties();      
        props.setProperty(JDBCPropertyNames.USE_COMMENTS_SOURCE_QUERY, Boolean.TRUE.toString());
        Translator trans = new Translator();
        trans.initialize(EnvironmentUtility.createEnvironment(props, false));
        SQLConversionVisitor visitor = trans.getSQLConversionVisitor();
        visitor.setExecutionContext(context);
        visitor.append(obj);
        return visitor.toString();
    }    
    
    /** 
     * @param expectedOutput
     * @param obj
     * @throws ConnectorException 
     * @since 4.2
     */
    private void helpTestVisitorWithCommand(String expectedOutput,
                                            ICommand obj,
                                            boolean useMetadata,
                                            boolean usePreparedStatement) throws ConnectorException {
        // Apply function replacement
        Translator trans = new Translator();
        Properties p = new Properties();
        if (usePreparedStatement) {
        	p.setProperty(JDBCPropertyNames.USE_BIND_VARIABLES, Boolean.TRUE.toString());
        }
        trans.initialize(EnvironmentUtility.createEnvironment(p, false));
        
        TranslatedCommand tc = new TranslatedCommand(new FakeExecutionContextImpl(), trans);
        tc.translateCommand(obj);

        assertEquals("Did not get correct sql", expectedOutput, tc.getSql());             //$NON-NLS-1$
    }
        
    public void testSimple() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS"); //$NON-NLS-1$
    }

    public void testAliasInSelect() {
        helpTestVisitor(getTestVDB(),
            "select part_name as x from parts", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME AS x FROM PARTS"); //$NON-NLS-1$
    }

    public void testAliasedGroup() {
        helpTestVisitor(getTestVDB(),
            "select y.part_name from parts y", //$NON-NLS-1$
            "SELECT y.PART_NAME FROM PARTS AS y"); //$NON-NLS-1$
    }

    public void testAliasedGroupAndElement() {
        helpTestVisitor(getTestVDB(),
            "select y.part_name AS z from parts y", //$NON-NLS-1$
            "SELECT y.PART_NAME AS z FROM PARTS AS y"); //$NON-NLS-1$
    }

    public void testLiteralString() {
        helpTestVisitor(getTestVDB(),
            "select 'x' from parts", //$NON-NLS-1$
            "SELECT 'x' FROM PARTS"); //$NON-NLS-1$
    }

    public void testLiteralInteger() {
        helpTestVisitor(getTestVDB(),
            "select 5 from parts", //$NON-NLS-1$
            "SELECT 5 FROM PARTS"); //$NON-NLS-1$
    }

    public void testLiteralFloat() {
        helpTestVisitor(getTestVDB(),
            "select 5.2 from parts", //$NON-NLS-1$
            "SELECT 5.2 FROM PARTS"); //$NON-NLS-1$
    }

    public void testLiteralLowFloat() {
        helpTestVisitor(getTestVDB(),
            "select 0.012 from parts", //$NON-NLS-1$
            "SELECT 0.012 FROM PARTS"); //$NON-NLS-1$
    }
    
    public void testLiteralLowFloat2() {
        helpTestVisitor(getTestVDB(),
            "select 0.00012 from parts", //$NON-NLS-1$
            "SELECT 0.00012 FROM PARTS"); //$NON-NLS-1$
    }    
    
    public void testLiteralHighFloat() {
        helpTestVisitor(getTestVDB(),
            "select 12345.123 from parts", //$NON-NLS-1$
            "SELECT 12345.123 FROM PARTS"); //$NON-NLS-1$
    }

    public void testLiteralHighFloat2() {
        helpTestVisitor(getTestVDB(),
            "select 1234567890.1234567 from parts", //$NON-NLS-1$
            "SELECT 1234567890.1234567 FROM PARTS"); //$NON-NLS-1$
    }
    
    public void testLiteralBoolean() {
        helpTestVisitor(getTestVDB(),
            "select {b'true'}, {b'false'} from parts", //$NON-NLS-1$
            "SELECT 1, 0 FROM PARTS"); //$NON-NLS-1$
    }

    public void testLiteralDate() {
        helpTestVisitor(getTestVDB(),
            "select {d'2003-12-31'} from parts", //$NON-NLS-1$
            "SELECT {d'2003-12-31'} FROM PARTS"); //$NON-NLS-1$
    }

    public void testLiteralTime() {
        helpTestVisitor(getTestVDB(),
            "select {t'23:59:59'} from parts", //$NON-NLS-1$
            "SELECT {t'23:59:59'} FROM PARTS"); //$NON-NLS-1$
    }

    public void testLiteralNull() {
        helpTestVisitor(getTestVDB(),
            "select null from parts", //$NON-NLS-1$
            "SELECT NULL FROM PARTS"); //$NON-NLS-1$
    }

    public void testLiteralTimestamp() {
        helpTestVisitor(getTestVDB(),
            "select {ts'2003-12-31 23:59:59.123'} from parts", //$NON-NLS-1$
            "SELECT {ts'2003-12-31 23:59:59.123'} FROM PARTS"); //$NON-NLS-1$
    }

    public void testSQL89Join() {
        helpTestVisitor(getTestVDB(),
            "select p.part_name from parts p, supplier_parts s where p.part_id = s.part_id", //$NON-NLS-1$
            "SELECT p.PART_NAME FROM PARTS AS p, SUPPLIER_PARTS AS s WHERE p.PART_ID = s.PART_ID"); //$NON-NLS-1$
    }

    public void testSQL92Join() {
        helpTestVisitor(getTestVDB(),
            "select p.part_name from parts p join supplier_parts s on p.part_id = s.part_id", //$NON-NLS-1$
            "SELECT p.PART_NAME FROM PARTS AS p INNER JOIN SUPPLIER_PARTS AS s ON p.PART_ID = s.PART_ID"); //$NON-NLS-1$
    }

    public void testSelfJoin() {
        helpTestVisitor(getTestVDB(),
            "select p.part_name from parts p join parts p2 on p.part_id = p2.part_id", //$NON-NLS-1$
            "SELECT p.PART_NAME FROM PARTS AS p INNER JOIN PARTS AS p2 ON p.PART_ID = p2.PART_ID"); //$NON-NLS-1$
    }

    public void testRightOuterJoin() {
        helpTestVisitor(getTestVDB(),
            "select p.part_name from parts p right join supplier_parts s on p.part_id = s.part_id", //$NON-NLS-1$
            "SELECT p.PART_NAME FROM SUPPLIER_PARTS AS s LEFT OUTER JOIN PARTS AS p ON p.PART_ID = s.PART_ID"); //$NON-NLS-1$
    }

    public void testLeftOuterJoin() {
        helpTestVisitor(getTestVDB(),
            "select p.part_name from parts p left join supplier_parts s on p.part_id = s.part_id", //$NON-NLS-1$
            "SELECT p.PART_NAME FROM PARTS AS p LEFT OUTER JOIN SUPPLIER_PARTS AS s ON p.PART_ID = s.PART_ID"); //$NON-NLS-1$
    }

    public void testFullOuterJoin() {
        helpTestVisitor(getTestVDB(),
            "select p.part_name from parts p full join supplier_parts s on p.part_id = s.part_id", //$NON-NLS-1$
            "SELECT p.PART_NAME FROM PARTS AS p FULL OUTER JOIN SUPPLIER_PARTS AS s ON p.PART_ID = s.PART_ID"); //$NON-NLS-1$
    }

    public void testCompare1() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id = 'x'", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID = 'x'"); //$NON-NLS-1$
    }

    public void testCompare2() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id <> 'x'", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID <> 'x'"); //$NON-NLS-1$
    }

    public void testCompare3() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id < 'x'", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID < 'x'"); //$NON-NLS-1$
    }

    public void testCompare4() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id <= 'x'", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID <= 'x'"); //$NON-NLS-1$
    }

    public void testCompare5() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id > 'x'", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID > 'x'"); //$NON-NLS-1$
    }

    public void testCompare6() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id >= 'x'", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID >= 'x'"); //$NON-NLS-1$
    }

    public void testIn1() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id in ('x')", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID = 'x'"); //$NON-NLS-1$
    }

    public void testIn2() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id in ('x', 'y')", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID IN ('x', 'y')"); //$NON-NLS-1$
    }

    public void testIn3() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id not in ('x', 'y')", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID NOT IN ('x', 'y')"); //$NON-NLS-1$
    }

    public void testIsNull1() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id is null", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID IS NULL"); //$NON-NLS-1$
    }

    public void testIsNull2() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id is not null", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID IS NOT NULL"); //$NON-NLS-1$
    }

    public void testInsertNull() {
        helpTestVisitor(getTestVDB(),
            "insert into parts (part_id, part_name, part_color, part_weight) values ('a', null, 'c', 'd')", //$NON-NLS-1$
            "INSERT INTO PARTS (PART_ID, PART_NAME, PART_COLOR, PART_WEIGHT) VALUES ('a', NULL, 'c', 'd')"); //$NON-NLS-1$
    }

    public void testUpdateNull() {
        helpTestVisitor(getTestVDB(),
            "update parts set part_weight = null where part_color = 'b'", //$NON-NLS-1$
            "UPDATE PARTS SET PART_WEIGHT = NULL WHERE PARTS.PART_COLOR = 'b'"); //$NON-NLS-1$
    }

    public void testUpdateWhereNull() {
        helpTestVisitor(getTestVDB(),
            "update parts set part_weight = 'a' where part_weight = null", //$NON-NLS-1$
            "UPDATE PARTS SET PART_WEIGHT = 'a' WHERE NULL <> NULL"); //$NON-NLS-1$
    }

    public void testGroupByWithFunctions() throws Exception {
        QueryMetadataInterface metadata = VDBMetadataFactory.getVDBMetadata(getTestVDB());       
    	        
        Select select = new Select();
        select.addSymbol(new ElementSymbol("part_name")); //$NON-NLS-1$
        From from = new From();
        from.addGroup(new GroupSymbol("parts")); //$NON-NLS-1$
        GroupBy groupBy = new GroupBy();
        Function function = new Function("concat", new Expression[] {new ElementSymbol("part_id"), new Constant("a")});  //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
        groupBy.addSymbol(function);
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setGroupBy(groupBy);

        QueryResolver.resolveCommand(query, metadata);
        Command command = QueryRewriter.rewrite(query, null, metadata, null);

        ICommand result =  new LanguageBridgeFactory(metadata).translate(command);

        helpTestVisitorWithCommand("SELECT PARTS.PART_NAME FROM PARTS GROUP BY concat(PARTS.PART_ID, 'a')", result, 
            false, //$NON-NLS-1$
            false);
    }
    
    public void testPreparedStatementCreationWithUpdate() {
        helpTestVisitor(getTestVDB(),
                        "update parts set part_weight = 'a' where part_weight < 5", //$NON-NLS-1$
                        "UPDATE PARTS SET PART_WEIGHT = ? WHERE PARTS.PART_WEIGHT < ?",
                        false,
                        true); //$NON-NLS-1$
    }
    
    public void testPreparedStatementCreationWithInsert() {
        helpTestVisitor(getTestVDB(),
                        "insert into parts (part_weight) values (5)", //$NON-NLS-1$
                        "INSERT INTO PARTS (PART_WEIGHT) VALUES (?)",
                        false,
                        true); //$NON-NLS-1$
    }
    
    public void testPreparedStatementCreationWithSelect() {
        helpTestVisitor(getTestVDB(),
                        "select part_name from parts where part_id not in ('x', 'y') and part_weight < 6", //$NON-NLS-1$
                        "SELECT PARTS.PART_NAME FROM PARTS WHERE (PARTS.PART_ID NOT IN (?, ?)) AND (PARTS.PART_WEIGHT < ?)",
                        false,
                        true); //$NON-NLS-1$
    }
    
    public void testPreparedStatementCreationWithLike() {
        helpTestVisitor(getTestVDB(),
                        "select part_name from parts where part_name like '%foo'", //$NON-NLS-1$
                        "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_NAME LIKE ?",
                        false,
                        true); //$NON-NLS-1$
    }
    
    /**
     * ideally this should not happen, but to be on the safe side 
     * only the right side should get replaced
     */
    public void testPreparedStatementCreationWithLeftConstant() {
        helpTestVisitor(getTestVDB(),
                        "select part_name from parts where 'x' = 'y'", //$NON-NLS-1$
                        "SELECT PARTS.PART_NAME FROM PARTS WHERE 1 = ?",
                        false,
                        true); //$NON-NLS-1$
    }
    
    /**
     * In the future, functions can be made smarter about which of their literal arguments
     * either are (or are not) eligible to be bind variables 
     */
    public void testPreparedStatementCreationWithFunction() {
        helpTestVisitor(getTestVDB(),
                        "select part_name from parts where concat(part_name, 'x') = concat('y', part_weight)", //$NON-NLS-1$
                        "SELECT PARTS.PART_NAME FROM PARTS WHERE concat(PARTS.PART_NAME, 'x') = concat('y', PARTS.PART_WEIGHT)",
                        false,
                        true); //$NON-NLS-1$
    }
    
    public void testPreparedStatementCreationWithCase() {
        helpTestVisitor(getTestVDB(),
                        "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_WEIGHT = CASE WHEN PARTS.PART_NAME='a' THEN 'b' ELSE 'c' END", //$NON-NLS-1$
                        "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_WEIGHT = CASE WHEN PARTS.PART_NAME = ? THEN 'b' ELSE 'c' END",
                        false,
                        true); //$NON-NLS-1$
    }

    public void testVisitIDeleteWithComment() throws Exception {
        String expected = "DELETE /*teiid sessionid:ConnectionID, requestid:RequestID.PartID*/ FROM g1 WHERE (100 >= 200) AND (500 < 600)"; //$NON-NLS-1$
        assertEquals(expected, getStringWithContext(TestDeleteImpl.example()));
    }

    public void testVisitIInsertWithComment() throws Exception {
        String expected = "INSERT /*teiid sessionid:ConnectionID, requestid:RequestID.PartID*/ INTO g1 (e1, e2, e3, e4) VALUES (1, 2, 3, 4)"; //$NON-NLS-1$
        assertEquals(expected, getStringWithContext(TestInsertImpl.example("g1"))); //$NON-NLS-1$
    }  
    
    public void testVisitISelectWithComment() throws Exception {
        String expected = "SELECT /*teiid sessionid:ConnectionID, requestid:RequestID.PartID*/ g1.e1, g1.e2, g1.e3, g1.e4"; //$NON-NLS-1$
        assertEquals(expected, getStringWithContext(TestSelectImpl.example(false)));
        expected = "SELECT /*teiid sessionid:ConnectionID, requestid:RequestID.PartID*/ DISTINCT g1.e1, g1.e2, g1.e3, g1.e4"; //$NON-NLS-1$
        assertEquals(expected, getStringWithContext(TestSelectImpl.example(true)));
    }
    
    public void testVisitIUpdateWithComment() throws Exception {
        String expected = "UPDATE /*teiid sessionid:ConnectionID, requestid:RequestID.PartID*/ g1 SET e1 = 1, e2 = 1, e3 = 1, e4 = 1 WHERE 1 = 1"; //$NON-NLS-1$
        assertEquals(expected, getStringWithContext(TestUpdateImpl.example()));
    }    
    
    public void testVisitIProcedureWithComment() throws Exception {
        String expected = "{ /*teiid sessionid:ConnectionID, requestid:RequestID.PartID*/  call sq3(?,?)}"; //$NON-NLS-1$
        assertEquals(expected, getStringWithContext(TestProcedureImpl.example()));
    }     
}
