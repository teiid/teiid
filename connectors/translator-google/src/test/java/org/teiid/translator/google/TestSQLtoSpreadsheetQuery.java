package org.teiid.translator.google;

import java.util.Properties;

import junit.framework.Assert;

import org.junit.Test;
import org.teiid.cdk.CommandBuilder;
import org.teiid.language.Command;
import org.teiid.language.Select;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.resource.adapter.google.metadata.SpreadsheetInfo;
import org.teiid.resource.adapter.google.metadata.Worksheet;
import org.teiid.translator.google.execution.SpreadsheetSQLVisitor;
import org.teiid.translator.google.metadata.MetadataProcessor;

/**
 * Tests transformation from Teiid Query to worksheet Query.
 * 
 * @author fnguyen
 *
 */
public class TestSQLtoSpreadsheetQuery {
	private QueryMetadataInterface dummySpreadsheetMetadata() {


		SpreadsheetInfo people=  new SpreadsheetInfo("People");
		Worksheet worksheet = people.createWorksheet("PeopleList");
		worksheet.setColumnCount(3);

		MetadataFactory factory = new MetadataFactory("", 1, "", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), "");
		MetadataProcessor processor = new MetadataProcessor(factory, people);
		processor.processMetadata();
		return new TransformationMetadata(null, new CompositeMetadataStore(factory.asMetadataStore()), null, RealMetadataFactory.SFM.getSystemFunctions(), null);
	}

	public Command getCommand(String sql) {
		CommandBuilder builder = new CommandBuilder(dummySpreadsheetMetadata());
		return builder.getCommand(sql);
	}
	
	private void testConversion(String sql, String expectedSpreadsheetQuery) {
		Select select = (Select)getCommand(sql);
		
		SpreadsheetSQLVisitor spreadsheetVisitor = new SpreadsheetSQLVisitor();
		spreadsheetVisitor.translateSQL(select);
		Assert.assertEquals(expectedSpreadsheetQuery, spreadsheetVisitor.getTranslatedSQL());
	}
	private SpreadsheetSQLVisitor getVisitorAndTranslateSQL(String sql){
        Select select = (Select)getCommand(sql);		
		SpreadsheetSQLVisitor spreadsheetVisitor = new SpreadsheetSQLVisitor();
		spreadsheetVisitor.translateSQL(select);
		return spreadsheetVisitor;
	}
	private void testVisitorValues(SpreadsheetSQLVisitor visitor,String worksheetTitle, Integer limitValue, Integer offsetvalue) {
		Assert.assertEquals(worksheetTitle, visitor.getWorksheetTitle());
		Assert.assertEquals(limitValue, visitor.getLimitValue());
		Assert.assertEquals(offsetvalue, visitor.getOffsetValue());
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
