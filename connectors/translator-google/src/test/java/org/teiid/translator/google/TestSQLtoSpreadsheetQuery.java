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

package org.teiid.translator.google;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.cdk.CommandBuilder;
import org.teiid.dqp.internal.datamgr.LanguageBridgeFactory;
import org.teiid.language.Command;
import org.teiid.language.Expression;
import org.teiid.language.Select;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.resource.adapter.google.GoogleSpreadsheetConnection;
import org.teiid.resource.adapter.google.metadata.SpreadsheetInfo;
import org.teiid.resource.adapter.google.metadata.Worksheet;

/**
 * Tests transformation from Teiid Query to worksheet Query.
 * 
 * @author fnguyen
 *
 */
@SuppressWarnings("nls")
public class TestSQLtoSpreadsheetQuery {
	private QueryMetadataInterface dummySpreadsheetMetadata() throws Exception {
	    GoogleSpreadsheetConnection conn = Mockito.mock(GoogleSpreadsheetConnection.class);
	    
		SpreadsheetInfo people=  new SpreadsheetInfo("People");
		Worksheet worksheet = people.createWorksheet("PeopleList");
		worksheet.setColumnCount(3);
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
		
		SpreadsheetSQLVisitor spreadsheetVisitor = new SpreadsheetSQLVisitor();
		spreadsheetVisitor.translateSQL(select);
		assertEquals(expectedSpreadsheetQuery, spreadsheetVisitor.getTranslatedSQL());
	}
	private SpreadsheetSQLVisitor getVisitorAndTranslateSQL(String sql) throws Exception {
        Select select = (Select)getCommand(sql);		
		SpreadsheetSQLVisitor spreadsheetVisitor = new SpreadsheetSQLVisitor();
		spreadsheetVisitor.translateSQL(select);
		return spreadsheetVisitor;
	}
	private void testVisitorValues(SpreadsheetSQLVisitor visitor,String worksheetTitle, Integer limitValue, Integer offsetvalue) {
		assertEquals(worksheetTitle, visitor.getWorksheetTitle());
		assertEquals(limitValue, visitor.getLimitValue());
		assertEquals(offsetvalue, visitor.getOffsetValue());
	}
		
	@Test
	public void testSelectFrom1() throws Exception {
		testConversion("select A,B from PeopleList", "SELECT A, B");
		testConversion("select C from PeopleList", "SELECT C");
		testConversion("select * from PeopleList", "SELECT A, B, C");
		testConversion("select A,B from PeopleList where A like '%car%'", "SELECT A, B WHERE A LIKE '%car%'");
		testConversion("select A,B from PeopleList where A='car'", "SELECT A, B WHERE A = 'car'");
		testConversion("select A,B from PeopleList where A >1  and B='bike'", "SELECT A, B WHERE A > '1' AND B = 'bike'");
		testConversion("select A,B from PeopleList where A<1 or B <> 'bike'", "SELECT A, B WHERE A < '1' OR B <> 'bike'");
		testConversion("select A,B from PeopleList limit 2", "SELECT A, B");
		testConversion("select A,B from PeopleList offset 2 row", "SELECT A, B");
		testConversion("select A,B from PeopleList limit 2,2", "SELECT A, B");
		testConversion("select max(A),B from PeopleList group by B", "SELECT MAX(A), B GROUP BY B");
		testConversion("select A,B from PeopleList where B like 'Filip%' order by B desc", "SELECT A, B WHERE B LIKE 'Filip%' ORDER BY B DESC");
		testConversion("select A,B from PeopleList where B like 'Filip%' order by B asc", "SELECT A, B WHERE B LIKE 'Filip%' ORDER BY B");
		testConversion("select A,B from PeopleList where B like 'Filip%' order by B asc", "SELECT A, B WHERE B LIKE 'Filip%' ORDER BY B");
		testConversion("select ucase(A),lower(B) from PeopleList", "SELECT upper(A), lower(B)");
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
		SpreadsheetSQLVisitor spreadsheetVisitor = new SpreadsheetSQLVisitor();
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
}
