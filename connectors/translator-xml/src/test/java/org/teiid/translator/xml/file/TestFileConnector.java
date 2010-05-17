package org.teiid.translator.xml.file;


import java.util.List;

import javax.resource.ResourceException;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.xml.XMLExecutionFactory;
import org.teiid.translator.xml.file.TestCachingFileConnectorLong.FileImpl;

import com.metamatrix.cdk.api.ConnectorHost;

@SuppressWarnings("nls")
public class TestFileConnector extends TestCase {

	public void testSelect() throws Exception{
		
		XMLExecutionFactory factory  = new XMLExecutionFactory();
		BasicConnectionFactory cf = new BasicConnectionFactory() {
			@Override
			public BasicConnection getConnection() throws ResourceException {				
				return new FileImpl(UnitTestUtil.getTestDataPath()+"/documents/purchaseOrdersShort.xml");
			}
			
		};
		
		String vdbPath = UnitTestUtil.getTestDataPath()+"/documents/purchase_orders.vdb";
		ConnectorHost host = new ConnectorHost(factory, cf, vdbPath);
		ExecutionContext context = Mockito.mock(ExecutionContext.class);
		host.setExecutionContext(context);
		
		try {
			List result = host.executeCommand("SELECT * FROM file_po_list.ITEM");
			assertEquals(2, result.size());
		} catch (TranslatorException e) {
			fail(e.getMessage());
		}
	}
}
