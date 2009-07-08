package com.metamatrix.connector.xml.file;


import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.teiid.connector.api.Connector;
import org.teiid.connector.api.ConnectorException;

import com.metamatrix.cdk.api.ConnectorHost;
import com.metamatrix.connector.xml.base.ProxyObjectFactory;
import com.metamatrix.connector.xml.base.XMLConnector;

public class TestFileConnectorLong extends TestCase {

	Connector connector;
	ConnectorHost host;
	
	@Override
	public void setUp() throws Exception {
		connector = new XMLConnector();
		Properties props = ProxyObjectFactory.getDefaultFileProps();
		props.setProperty(FileConnectorState.FILE_NAME, "purchaseOrders.xml");
		String vdbPath = ProxyObjectFactory.getDocumentsFolder() + "/purchase_orders.vdb";
		host = new ConnectorHost(connector, props, vdbPath);
		host.setSecurityContext("purchase_orders", "1", "root", null);
	}
	
	public void testSelect() throws ConnectorException {
		List result = host.executeCommand("SELECT * FROM file_po_list.ITEM");
		assertEquals(5968, result.size());
	}

}
