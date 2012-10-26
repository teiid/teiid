package org.teiid.resource.adapter.google.unit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.junit.Test;
import org.teiid.resource.adapter.google.metadata.WorksheetsElement;

public class MetadataOverridesTest {
	@Test 
	public void emptyXml(){
		WorksheetsElement overrides = WorksheetsElement.parse(loadXml("empty.xml"));
	}
//	
	@Test 
	public void simpleOverrideHeader(){
		WorksheetsElement overrides = WorksheetsElement.parse(loadXml("test.xml"));
	}
	
//	
//	@Test 
//	public void columnDataTypeOverride(){
//		Metadata overrides = Metadata.parse(loadXml("simpleOverride.xml"));
//		Metadata metadata = new Metadata();
//		Spreadsheet sp1 = new Spreadsheet("MyTestSheet");
//		Worksheet w1 = new Worksheet("SH1");
//		sp1.addWorksheet(w1);
//		w1.addColumn(new Column("A","X",SpreadsheetColumnType.DOUBLE));
//		w1.addColumn(new Column("B","B",SpreadsheetColumnType.DATE));
//		w1.addColumn(new Column("C","Ahoj",SpreadsheetColumnType.DOUBLE));
//
//		
//		metadata.addSpreadsheet(sp1);		
//		metadata.override(overrides);
//		
//		Assert.assertEquals("X", metadata.getColumnName("MyTestSheet","SH1","A"));
//		Assert.assertEquals("Ahoj", metadata.getColumnName("MyTestSheet","SH1","C"));
//		Assert.assertEquals(SpreadsheetColumnType.INTEGER, metadata.getColumnDataType("MyTestSheet","SH1","A"));
//		Assert.assertEquals(SpreadsheetColumnType.DATE, metadata.getColumnDataType("MyTestSheet","SH1","B"));
//		Assert.assertEquals(SpreadsheetColumnType.DATE, metadata.getColumnDataType("MyTestSheet","SH1","C"));
//	}
//	
	
	private String loadXml(String fileName) {
		BufferedReader reader  = null;
		 try {
			 System.out.println(ClassLoader.getSystemResource(fileName).toURI());
			 
			 reader = new BufferedReader(
					 new InputStreamReader(
					 
							 ClassLoader.getSystemResourceAsStream(""+fileName)
									 )
						);
			 
			 StringBuilder sb = new StringBuilder();
			 String line=null;
			 while ((line = reader.readLine()) != null)
				 sb.append(line);
			 
			 return sb.toString();
		} catch (Exception ex)	{
			throw new RuntimeException(ex);
		} finally {
			if (reader != null)
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}
}
