package org.teiid.connector.xml.file;


import java.util.List;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.xmlsource.file.FileConnector;
import org.teiid.connector.xmlsource.file.FileManagedConnectionFactory;

import com.metamatrix.cdk.api.ConnectorHost;

public class TestFileConnector extends TestCase {

	public void testSelect() throws Exception{
		
		FileConnector connector = new FileConnector();
		FileManagedConnectionFactory env = FakeFileManagedConnectionfactory.getDefaultFileProps();
		env.setFileName("purchaseOrdersShort.xml");
		
		String vdbPath = FakeFileManagedConnectionfactory.getDocumentsFolder() + "/purchase_orders.vdb";
		ConnectorHost host = new ConnectorHost(connector, env, vdbPath);
		
		ExecutionContext context = Mockito.mock(ExecutionContext.class);
		host.setExecutionContext(context);
		
		try {
			List result = host.executeCommand("SELECT * FROM file_po_list.ITEM");
			assertEquals(2, result.size());
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
}
