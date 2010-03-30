package org.teiid.connector.xml;


import java.util.List;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.connector.api.Connector;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.xml.file.FakeFileManagedConnectionfactory;
import org.teiid.connector.xml.file.FileConnector;
import org.teiid.connector.xml.file.FileManagedConnectionFactory;

import com.metamatrix.cdk.api.ConnectorHost;

public class TestCachingFileConnector extends TestCase {

	Connector connector;
	ConnectorHost host;
	
	@Override
	public void setUp() throws Exception {
		connector = new FileConnector();
		FileManagedConnectionFactory env = FakeFileManagedConnectionfactory.getDefaultFileProps();
		env.setFileName("purchaseOrdersShort.xml");
		this.connector.initialize(env);
		
		String vdbPath = FakeFileManagedConnectionfactory.getDocumentsFolder() + "/File/purchase_orders.vdb";
		host = new ConnectorHost(connector, env, vdbPath);

		ExecutionContext context = Mockito.mock(ExecutionContext.class);
		host.setExecutionContext(context);
	}
	
	public void testSelect() {
		try {
			List result = host.executeCommand("SELECT * FROM po_list.ITEM");
			assertEquals(2, result.size());
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
}
