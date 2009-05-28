package com.metamatrix.connector.xml.http;


import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.teiid.connector.api.Connector;
import org.teiid.connector.api.ConnectorException;

import com.metamatrix.cdk.api.ConnectorHost;
import com.metamatrix.connector.xml.base.ProxyObjectFactory;
import com.metamatrix.connector.xml.base.XMLConnector;
import com.metamatrix.connector.xml.base.XMLConnectorStateImpl;

public class TestNoArgConnector extends TestCase {

	Connector connector;
	ConnectorHost host;
	HTTPTestServer server;
	
	@Override
	public void setUp() throws Exception {
		server = new HTTPTestServer();
		
		connector = new XMLConnector();
		Properties props = ProxyObjectFactory.getDefaultHttpProps();
		props.setProperty(HTTPConnectorState.URI, "http://0.0.0.0:8673/purchaseOrdersShort.xml");
		props.setProperty(HTTPConnectorState.PARAMETER_METHOD, HTTPConnectorState.PARAMETER_NONE);
		props.setProperty(XMLConnectorStateImpl.CACHE_ENABLED, "true");
		String vdbPath = ProxyObjectFactory.getDocumentsFolder() + "/purchase_orders.vdb";
		host = new ConnectorHost(connector, props, vdbPath);
		host.setSecurityContext("purchase_orders", "1", "root", null);
	}
	
	public void testSelect() {
		try {
			List result = host.executeCommand("SELECT * FROM po_list_noargs.ITEM");
			assertEquals(2, result.size());
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
}
