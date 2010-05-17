package org.teiid.translator.xml.file;


import java.io.File;
import java.util.List;

import javax.resource.ResourceException;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.FileConnection;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.xml.XMLExecutionFactory;

import com.metamatrix.cdk.api.ConnectorHost;

@SuppressWarnings("nls")
public class TestCachingFileConnectorLong extends TestCase {

	ConnectorHost host;
	
	@Override
	public void setUp() throws Exception {
		XMLExecutionFactory factory  = new XMLExecutionFactory();
		BasicConnectionFactory cf = new BasicConnectionFactory() {
			@Override
			public BasicConnection getConnection() throws ResourceException {				
				return new FileImpl(UnitTestUtil.getTestDataPath()+"/documents/purchaseOrders.xml");
			}
			
		};
		
		String vdbPath = UnitTestUtil.getTestDataPath()+"/documents/purchase_orders.vdb";
		host = new ConnectorHost(factory, cf, vdbPath);
		ExecutionContext context = Mockito.mock(ExecutionContext.class);
		host.setExecutionContext(context);
	}
	
	/**
	 * This primes the cache with the response docs, then gets them from the cache
	 */
	public void testSelectFromCache() {
		try {
			List result = host.executeCommand("SELECT * FROM file_po_list.ITEM");
			assertEquals(5968, result.size());
			
			result = host.executeCommand("SELECT * FROM file_po_list.ITEM");
			assertEquals(5968, result.size());
		} catch (TranslatorException e) {
			fail(e.getMessage());
		}
	}

    static class FileImpl extends BasicConnection implements FileConnection{
    	File file;
    	public FileImpl(String file) {
    		this.file = new File(file);
    	}
    	
		@Override
		public File[] getFiles(String path) {
			
			File f = null;
			if (path != null) {
				f = new File(file, path);
			}
			else {
				f = file;
			}
			
			if (!f.exists()) {
				return null;
			}
			if (f.isDirectory()) {
				return file.listFiles();
			}
			return new File[] {f};
		}

		@Override
		public void close() throws ResourceException {
		}
    }
    
}
