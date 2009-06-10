package com.metamatrix.connector.xml;

import java.io.FileInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.cdk.api.SysLogger;
import com.metamatrix.connector.xml.base.ExecutionInfo;
import com.metamatrix.connector.xml.base.ProxyObjectFactory;
import com.metamatrix.connector.xml.base.QueryAnalyzer;
import com.metamatrix.connector.xml.file.FileConnectorState;
import com.metamatrix.connector.xml.streaming.DocumentImpl;
import com.metamatrix.connector.xml.streaming.ElementProcessor;
import com.metamatrix.connector.xml.streaming.ReaderFactory;
import com.metamatrix.connector.xml.streaming.StreamingRowCollector;

public class TestElementCollector extends TestCase {

	StreamingRowCollector builder;
	String filename = ProxyObjectFactory.getDocumentsFolder() + "/purchaseOrders.xml";
	String vdbPath = ProxyObjectFactory.getDocumentsFolder() + "/File/purchase_orders.vdb";
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		Map prefixes = new HashMap<String, String>();
		prefixes.put("po", "http://www.example.com/PO1");
		prefixes.put("xsd", "http://www.w3.org/2001/XMLSchema");
		ConnectorEnvironment env = EnvironmentUtility.createEnvironment(
    			ProxyObjectFactory.getDefaultFileProps());
		ConnectorLogger logger = new SysLogger(false);
    	XMLConnectorState state = new FileConnectorState();
    	state.setLogger(logger);
    	state.setState(env);
    	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
    	IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, "SELECT * FROM po_list.ITEM");
    	QueryAnalyzer analyzer = new QueryAnalyzer(query, metadata, new MockQueryPreprocessor(), logger, Mockito.mock(ExecutionContext.class), Mockito.mock(ConnectorEnvironment.class));
    	analyzer.analyze();
    	ExecutionInfo info = analyzer.getExecutionInfo();
    	ElementProcessor processor = new ElementProcessor(info);
		builder = new StreamingRowCollector(prefixes, ReaderFactory.getXMLReader(state), processor);
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		builder = null;
	}

	public void testGetTables1() {
		String path = "/po:purchaseOrders/order/items/item";
		int itemCount = 5968;
		try {
			Document doc = new DocumentImpl(new FileInputStream(filename), "foo");
			List result = builder.getElements(doc, Arrays.asList(path));
			assertEquals(itemCount, result.size());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void testGetRoot() {
		String path = "/";
		int itemCount = 1;
		try {
			Document doc = new DocumentImpl(new FileInputStream(filename), "foo");
			List result = builder.getElements(doc, Arrays.asList(path));
			assertEquals(itemCount, result.size());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
}
