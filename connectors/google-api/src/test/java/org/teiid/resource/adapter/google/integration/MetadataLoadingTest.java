package org.teiid.resource.adapter.google.integration;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;
import org.teiid.resource.adapter.google.auth.AuthHeaderFactory;
import org.teiid.resource.adapter.google.auth.ClientLoginHeaderFactory;
import org.teiid.resource.adapter.google.dataprotocol.GoogleDataProtocolAPI;
import org.teiid.resource.adapter.google.gdata.GDataClientLoginAPI;
import org.teiid.resource.adapter.google.gdata.SpreadsheetMetadataExtractor;
import org.teiid.resource.adapter.google.metadata.SpreadsheetInfo;

/**
 * Testing of metadata loading. For metadata loading we use GData API.
 * 
 * @author fnguyen
 *
 *
 */
@Ignore
@SuppressWarnings("nls")
public class MetadataLoadingTest extends IntegrationTest {
	
	private static GDataClientLoginAPI gdata = null;
	private static GoogleDataProtocolAPI visualizationAPI = null;
	
	{
		AuthHeaderFactory auth=  new ClientLoginHeaderFactory(USERNAME, PASSWORD);
		auth.login();
		gdata = new GDataClientLoginAPI();
		gdata.setHeaderFactory(auth);
		visualizationAPI = new GoogleDataProtocolAPI();
		visualizationAPI.setSpreadSheetBrowser(gdata);
		visualizationAPI.setHeaderFactory(auth);
	}
	
	@Test
	public void findSpreadsheet(){
		Assert.assertEquals("Spreadsheet not found", "tWNeVMVpBRQYOSY1Pgav15Q",gdata.getSpreadsheetKeyByTitle("spreadsheet1"));
	}
	@Test 
	public void testMetadata(){

		SpreadsheetMetadataExtractor extractor = new SpreadsheetMetadataExtractor();
		extractor.setGdataAPI(gdata);
		extractor.setVisualizationAPI(visualizationAPI);
		SpreadsheetInfo metadata= extractor.extractMetadata("integration_tests");
		Assert.assertEquals(0,metadata.getWorksheetByName("Sheet1").getColumnCount());
		Assert.assertEquals(2,metadata.getWorksheetByName("Sheet2").getColumnCount());
		metadata= extractor.extractMetadata("people");
		Assert.assertEquals(5,metadata.getWorksheetByName("list").getColumnCount());
		Assert.assertEquals(4,metadata.getWorksheetByName("phones").getColumnCount());
		
	}
	
}

