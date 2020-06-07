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

package org.teiid.translator.google;

import static org.junit.Assert.*;

import java.util.LinkedHashMap;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.cdk.CommandBuilder;
import org.teiid.dqp.internal.datamgr.LanguageBridgeFactory;
import org.teiid.language.Command;
import org.teiid.language.Delete;
import org.teiid.language.Expression;
import org.teiid.language.Insert;
import org.teiid.language.Select;
import org.teiid.language.Update;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.google.api.GoogleSpreadsheetConnection;
import org.teiid.translator.google.api.SpreadsheetOperationException;
import org.teiid.translator.google.api.metadata.Column;
import org.teiid.translator.google.api.metadata.SpreadsheetColumnType;
import org.teiid.translator.google.api.metadata.SpreadsheetInfo;
import org.teiid.translator.google.api.metadata.Util;
import org.teiid.translator.google.api.metadata.Worksheet;
import org.teiid.translator.google.visitor.SpreadsheetDeleteVisitor;
import org.teiid.translator.google.visitor.SpreadsheetInsertVisitor;
import org.teiid.translator.google.visitor.SpreadsheetSQLVisitor;
import org.teiid.translator.google.visitor.SpreadsheetUpdateVisitor;

/**
 * Tests transformation from Teiid Query to worksheet Query.
 *
 * @author fnguyen
 *
 */
@SuppressWarnings("nls")
public class TestSQLtoSpreadsheetQuery {
    static SpreadsheetInfo people;

    @BeforeClass
    public static void createSpreadSheetInfo() {
        people=  new SpreadsheetInfo();
        Worksheet worksheet = people.createWorksheet("PeopleList");
        worksheet.setHeaderEnabled(true);
        for (int i = 1; i <= 4; i++) {
            Column newCol = new Column();
            newCol.setAlphaName(Util.convertColumnIDtoString(i));
            newCol.setLabel(newCol.getAlphaName());
            worksheet.addColumn(newCol.getAlphaName(), newCol);
        }
        worksheet.getColumns().get("C").setDataType(SpreadsheetColumnType.NUMBER);
        worksheet.getColumns().get("D").setDataType(SpreadsheetColumnType.BOOLEAN);
    }

    private QueryMetadataInterface dummySpreadsheetMetadata() throws Exception {
        GoogleSpreadsheetConnection conn = Mockito.mock(GoogleSpreadsheetConnection.class);

        Mockito.stub(conn.getSpreadsheetInfo()).toReturn(people);

        MetadataFactory factory = new MetadataFactory("", 1, "", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), "");
        GoogleMetadataProcessor processor = new GoogleMetadataProcessor();
        processor.process(factory, conn);
        return new TransformationMetadata(null, new CompositeMetadataStore(factory.asMetadataStore()), null, RealMetadataFactory.SFM.getSystemFunctions(), null);
    }

    public Command getCommand(String sql) throws Exception {
        CommandBuilder builder = new CommandBuilder(dummySpreadsheetMetadata());
        return builder.getCommand(sql);
    }

    private void testConversion(String sql, String expectedSpreadsheetQuery) throws Exception{
        Select select = (Select)getCommand(sql);

        SpreadsheetSQLVisitor spreadsheetVisitor = new SpreadsheetSQLVisitor(people);
        spreadsheetVisitor.translateSQL(select);
        assertEquals(expectedSpreadsheetQuery, spreadsheetVisitor.getTranslatedSQL());
    }

    private void testUpdateConversion(String sql, String expectedCriteria) throws Exception {
        Update update = (Update)getCommand(sql);
        SpreadsheetUpdateVisitor spreadsheetVisitor = new SpreadsheetUpdateVisitor(people);
        spreadsheetVisitor.visit(update);
        assertEquals(expectedCriteria, spreadsheetVisitor.getCriteriaQuery());
    }

    private void testDeleteConversion(String sql, String expectedCriteria) throws Exception {
        Delete delete = (Delete)getCommand(sql);
        SpreadsheetDeleteVisitor spreadsheetVisitor = new SpreadsheetDeleteVisitor(people);
        spreadsheetVisitor.visit(delete);
        assertEquals(expectedCriteria, spreadsheetVisitor.getCriteriaQuery());
    }

    private SpreadsheetSQLVisitor getVisitorAndTranslateSQL(String sql) throws Exception{
        Select select = (Select)getCommand(sql);
        SpreadsheetSQLVisitor spreadsheetVisitor = new SpreadsheetSQLVisitor(people);
        spreadsheetVisitor.translateSQL(select);
        return spreadsheetVisitor;
    }
    private void testVisitorValues(SpreadsheetSQLVisitor visitor,String worksheetTitle, Integer limitValue, Integer offsetvalue) {
        assertEquals(worksheetTitle, visitor.getWorksheet().getTitle());
        assertEquals(limitValue, visitor.getLimitValue());
        assertEquals(offsetvalue, visitor.getOffsetValue());
    }

    @Test
    public void testSelectFrom1() throws Exception {
        testConversion("select A,B from PeopleList", "SELECT A, B");
        testConversion("select C from PeopleList", "SELECT C");
        testConversion("select * from PeopleList", "SELECT A, B, C, D");
        testConversion("select A,B from PeopleList where A like '%car%' AND A NOT like '_car_'", "SELECT A, B WHERE A LIKE '%car%' AND (A NOT LIKE '_car_' AND A IS NOT NULL)");
        testConversion("select A,B from PeopleList where A='car'", "SELECT A, B WHERE A = 'car'");
        testConversion("select A,B from PeopleList where A >1  and B='bike'", "SELECT A, B WHERE A > '1' AND B = 'bike'");
        testConversion("select A,B from PeopleList where A<1 or B <> 'bike'", "SELECT A, B WHERE (A < '1' AND A IS NOT NULL) OR (B <> 'bike' AND B IS NOT NULL)");
        testConversion("select A,B from PeopleList limit 2", "SELECT A, B");
        testConversion("select A,B from PeopleList offset 2 row", "SELECT A, B");
        testConversion("select A,B from PeopleList limit 2,2", "SELECT A, B");
        testConversion("select max(A),B from PeopleList group by B", "SELECT MAX(A), B GROUP BY B");
        testConversion("select A,B from PeopleList where B like 'Filip%' order by B desc", "SELECT A, B WHERE B LIKE 'Filip%' ORDER BY B DESC");
        testConversion("select A,B from PeopleList where B like 'Filip%' order by B asc", "SELECT A, B WHERE B LIKE 'Filip%' ORDER BY B");
        testConversion("select A,B from PeopleList where B like 'Filip%' order by B asc", "SELECT A, B WHERE B LIKE 'Filip%' ORDER BY B");
        testConversion("select ucase(A),lower(B) from PeopleList", "SELECT upper(A), lower(B)");
    }
    @Test
    public void testUpdateCriteria() throws Exception {
        testUpdateConversion("update PeopleList set A=1 where C>1","c > 1.0");
        testUpdateConversion("update PeopleList set A=1 where C=10.5","c = 10.5");
        testUpdateConversion("update PeopleList set A=1 where C <= 1000 and C !=5","(c <= 1000.0 AND c <> \"\") AND (c <> 5.0 AND c <> \"\")");
        testUpdateConversion("update PeopleList set A=1 where C >= 50 or C <=60.1","c >= 50.0 OR (c <= 60.1 AND c <> \"\")");
        testUpdateConversion("update PeopleList set A=1 where A = 'car'","a = \"car\"");
    }

    @Test
    public void testDeleteCriteria() throws Exception {
        testDeleteConversion("delete from PeopleList where C > 1","c > 1.0");
        testDeleteConversion("delete from PeopleList where C=10.5","c = 10.5");
        testDeleteConversion("delete from PeopleList where C <= 1000 and C !=5","(c <= 1000.0 AND c <> \"\") AND (c <> 5.0 AND c <> \"\")");
        testDeleteConversion("delete from PeopleList where C >= 50 or C <=60.1","c >= 50.0 OR (c <= 60.1 AND c <> \"\")");
        testDeleteConversion("delete from PeopleList where A = 'car'","a = \"car\"");
        testDeleteConversion("delete from PeopleList where D = true","d = true");
    }

    @Test public void testLiterals() throws Exception {
        helpTestExpression("1", "1");
        helpTestExpression("true", "TRUE");
        helpTestExpression("null", "NULL");
        helpTestExpression("{d '2001-02-02'}", "date \"2001-02-02\"");
        helpTestExpression("{t '02:23:34'}", "timeofday \"02:23:34\"");
        helpTestExpression("{ts '2012-03-04 02:23:34.10001'}", "datetime \"2012-03-04 02:23:34.100\"");
    }

    private void helpTestExpression(String expression, String expected)
            throws QueryParserException {
        LanguageBridgeFactory lbf = new LanguageBridgeFactory(RealMetadataFactory.example1Cached());
        Expression ex = lbf.translate(QueryParser.getQueryParser().parseExpression(expression));
        SpreadsheetSQLVisitor spreadsheetVisitor = new SpreadsheetSQLVisitor(people);
        spreadsheetVisitor.translateSQL(ex);
        assertEquals(expected, spreadsheetVisitor.getTranslatedSQL());
    }

    @Test
    public void testSelectVisitorValues() throws Exception {
        SpreadsheetSQLVisitor visitor=getVisitorAndTranslateSQL("select * from PeopleList where A = 'car' limit 2");
        testVisitorValues(visitor, "PeopleList",2,null);

        visitor=getVisitorAndTranslateSQL("select * from PeopleList where A = 'car' offset 5 row");
        testVisitorValues(visitor, "PeopleList",Integer.MAX_VALUE,5);

        visitor=getVisitorAndTranslateSQL("select A,B from PeopleList where B like 'Filip%' order by B desc");
        testVisitorValues(visitor, "PeopleList",null,null);

        visitor=getVisitorAndTranslateSQL("select A,B from PeopleList limit 2,3");
        testVisitorValues(visitor, "PeopleList",3,2);

    }

    @Test
    public void testInsertVisitor() throws Exception {
        String sql="insert into PeopleList(A,B,C) values ('String,String', 'String@String', 15.5)";
        SpreadsheetInsertVisitor visitor=new SpreadsheetInsertVisitor(people);
        visitor.visit((Insert)getCommand(sql));
        assertEquals(3, visitor.getColumnNameValuePair().size());
        assertEquals("String,String",visitor.getColumnNameValuePair().get("A"));
        assertEquals("String@String",visitor.getColumnNameValuePair().get("B"));
        assertEquals(15.5,visitor.getColumnNameValuePair().get("C"));
    }

    @Test
    public void testInsertVisitorNull() throws Exception {
        String sql="insert into PeopleList(A,B,C) values ('String,String', null, 15.5)";
        SpreadsheetInsertVisitor visitor=new SpreadsheetInsertVisitor(people);
        visitor.visit((Insert)getCommand(sql));
        assertEquals(2, visitor.getColumnNameValuePair().size());
        assertEquals("String,String",visitor.getColumnNameValuePair().get("A"));
        assertEquals(15.5,visitor.getColumnNameValuePair().get("C"));
    }

    @Test
    public void testInsertExecution() throws Exception {
        String sql="insert into PeopleList(A,B,C) values ('String,String', 'val', 15.5)";
        Insert insert = (Insert)getCommand(sql);
        GoogleSpreadsheetConnection gsc = Mockito.mock(GoogleSpreadsheetConnection.class);
        Mockito.stub(gsc.getSpreadsheetInfo()).toReturn(people);
        RuntimeMetadata rm = Mockito.mock(RuntimeMetadata.class);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        SpreadsheetUpdateExecution sue = new SpreadsheetUpdateExecution(insert, gsc, ec, rm);
        sue.execute();
        LinkedHashMap<String, Object> vals = new LinkedHashMap<String, Object>();
        vals.put("A", "String,String");
        vals.put("B", "val");
        vals.put("C", 15.5);
        Mockito.verify(gsc).executeRowInsert(people.getWorksheetByName("PeopleList"), vals);
    }

    @Test
    public void testUpdateVisitor() throws Exception {
        String sql="UPDATE PeopleList set A = 'String,String', C = 1.5";
        SpreadsheetUpdateVisitor visitor=new SpreadsheetUpdateVisitor(people);
        visitor.visit((Update)getCommand(sql));
        assertEquals(2,visitor.getChanges().size());
        assertEquals("A", visitor.getChanges().get(0).getColumnID());
        assertEquals("'String,String", visitor.getChanges().get(0).getValue());
        assertEquals("C", visitor.getChanges().get(1).getColumnID());
        assertEquals("1.5", visitor.getChanges().get(1).getValue());
        assertNull(visitor.getCriteriaQuery());
    }

    @Test
    public void testUpdateVisitorNull() throws Exception {
        String sql="UPDATE PeopleList set A = 'String,String', C = null where A='Str,Str'";
        SpreadsheetUpdateVisitor visitor=new SpreadsheetUpdateVisitor(people);
        visitor.visit((Update)getCommand(sql));
        assertEquals(2,visitor.getChanges().size());
        assertEquals("A", visitor.getChanges().get(0).getColumnID());
        assertEquals("'String,String", visitor.getChanges().get(0).getValue());
        assertEquals("C", visitor.getChanges().get(1).getColumnID());
        assertEquals("", visitor.getChanges().get(1).getValue());
        assertEquals("a = \"Str,Str\"", visitor.getCriteriaQuery());
    }

    //should fail as the null string would be treated as empty
    @Test(expected=SpreadsheetOperationException.class)
    public void testUpdateVisitorNullString() throws Exception {
        String sql="UPDATE PeopleList set A = null where A='Str,Str'";
        SpreadsheetUpdateVisitor visitor=new SpreadsheetUpdateVisitor(people);
        visitor.visit((Update)getCommand(sql));
    }

}
