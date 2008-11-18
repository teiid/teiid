/*
 * © 2007 Varsity Gateway LLC. All Rights Reserved.
 */

package com.metamatrix.connector.xml.base;

import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.cdk.api.TranslationUtility;
import com.metamatrix.connector.xml.XMLConnection;
import com.metamatrix.connector.xml.XMLExecution;
import com.metamatrix.connector.xml.file.FileConnectorState;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IQuery;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;

/**
 * created by JChoate on Jun 16, 2005
 *
 */
public class TestXMLExecution extends TestCase {

	private String m_vdbPath = null;	
    
    public TestXMLExecution() {
    	super();
    	checkVdbPath();
    }
    
	private void checkVdbPath() {
    	if (m_vdbPath == null) {
    		m_vdbPath = ProxyObjectFactory.getStateCollegeVDBLocation();	
    	}        		
	}


	public TestXMLExecution(String test) {
        super(test);
        checkVdbPath();
    }
    
    protected void setUp() throws Exception {
        super.setUp();
    }
    
    public void testInit() {        
        
        XMLExecution execution = ProxyObjectFactory.getDefaultXMLExecution(m_vdbPath);
        assertNotNull("XMLExecutionImpl is null", execution);
    }

    public void testExecute() {
        
        XMLExecutionImpl execution = ProxyObjectFactory.getXMLExecution(m_vdbPath, "oh", "no");
        String queryString = "select Company_id from Company where Company_id is not null order by Company_id";
        IQuery query = ProxyObjectFactory.getDefaultIQuery(m_vdbPath, queryString);
        final int maxBatch = 1000;
        try {
            execution.execute(query, maxBatch);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    
    }

    public void testEmptyNextBatch() {
        XMLExecutionImpl execution = ProxyObjectFactory.getDefaultXMLExecution(m_vdbPath);
        try {
            Batch batch = execution.nextBatch();
            assertNotNull("The batch is null", batch);
            assertEquals(0, batch.getRowCount());
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }        
    }
    
    public void testNextBatch() {
        XMLExecutionImpl execution = ProxyObjectFactory.getDefaultXMLExecution(m_vdbPath);
        assertNull(execution.getInfo());
        String queryString = "select Company_id from Company where Company_id is not null order by Company_id";
        IQuery query = ProxyObjectFactory.getDefaultIQuery(m_vdbPath, queryString);
        final int maxBatch = 1000;
        try {
            execution.execute(query, maxBatch);
            Batch batch = execution.nextBatch();
            assertNotNull("Batch is null", batch);
            assertTrue(batch.getRowCount() > 0);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }
    
    public void testNextBatchExceedSize() {
        XMLExecutionImpl execution = ProxyObjectFactory.getDefaultXMLExecution(m_vdbPath);
        assertNull(execution.getInfo());
        String queryString = "select Name from Employee where Name in ('george')";
        IQuery query = ProxyObjectFactory.getDefaultIQuery(m_vdbPath, queryString);
        final int maxBatch = 1;
        try {
            execution.execute(query, maxBatch);
            Batch batch = execution.nextBatch();
            assertNotNull("Batch is null", batch);
            assertTrue(batch.getRowCount() > 0);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }   

    public void testNextBatchMultiCriteria() {
        XMLExecutionImpl execution = ProxyObjectFactory.getDefaultXMLExecution(m_vdbPath);
        assertNull(execution.getInfo());
        String queryString = "select state, zip From Conference where Company_id in ('Widgets Inc.') and Department_id in ('QA')";
        IQuery query = ProxyObjectFactory.getDefaultIQuery(m_vdbPath, queryString);
        final int maxBatch = 1;
        try {
            execution.execute(query, maxBatch);
            Batch batch = execution.nextBatch();
            assertNotNull("Batch is null", batch);
            assertTrue("The query results are the wrong size", batch.getRowCount() > 0);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    } 
 
    
    public void testNextBatchClob() {
    	String vdb = ProxyObjectFactory.getDocumentsFolder() + "/Gutenberg.vdb";
    	               
    	Properties testFileProps = new Properties();
        testFileProps.put(XMLConnectorStateImpl.CACHE_TIMEOUT, new String("500000"));
        testFileProps.put(XMLConnectorStateImpl.MAX_MEMORY_CACHE_SIZE, new String("5000"));
        testFileProps.put(XMLConnectorStateImpl.MAX_FILE_CACHE_SIZE, new String("5000"));
        testFileProps.put(XMLConnectorStateImpl.CACHE_ENABLED, Boolean.TRUE);
        testFileProps.put(XMLConnectorStateImpl.FILE_CACHE_LOCATION, new String("./cache"));
        testFileProps.put(XMLConnectorStateImpl.MAX_IN_MEMORY_STRING_SIZE, new String("1"));
        testFileProps.put(XMLConnectorStateImpl.CONNECTOR_CAPABILITES, "com.metamatrix.connector.xml.base.XMLCapabilities");
        testFileProps.setProperty(XMLConnectorStateImpl.STATE_CLASS_PROP, "com.metamatrix.connector.xml.file.FileConnectorState");
        testFileProps.setProperty(XMLConnectorStateImpl.QUERY_PREPROCESS_CLASS, "com.metamatrix.connector.xml.base.NoQueryPreprocessing");
        testFileProps.setProperty(XMLConnectorStateImpl.SAX_FILTER_PROVIDER_CLASS, "com.metamatrix.connector.xml.base.NoExtendedFilters"); 
        
        testFileProps.put(FileConnectorState.FILE_NAME, "mdprp10.xml");
        String localPath = ProxyObjectFactory.getDocumentsFolder() + "/books";
        testFileProps.put(FileConnectorState.DIRECTORY_PATH, localPath);
        
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(testFileProps);
    	XMLConnector ctor = new XMLConnector();
        try {
	    	ctor.initialize(env);
	    	SecurityContext exCtx = EnvironmentUtility.createExecutionContext("Request", "testPartId");
	    	XMLConnectionImpl conn = new XMLConnectionImpl(ctor, exCtx, env);
	    	
	    	TranslationUtility transUtil = new TranslationUtility(vdb);
	        RuntimeMetadata meta = transUtil.createRuntimeMetadata();  
	    	XMLExecutionImpl execution = new XMLExecutionImpl(conn, meta, exCtx, env);
	    	
	    	assertNull(execution.getInfo());
	        String queryString = "select Author_Last, Title, Pub_Year, Header_About, Text from Document";
	        IQuery query = ProxyObjectFactory.getDefaultIQuery(vdb, queryString);
	        
	        
	        final int maxBatch = 1;

            execution.execute(query, maxBatch);
            Batch batch = execution.nextBatch();
            assertNotNull("Batch is null", batch);
            assertTrue(batch.getRowCount() > 0);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }
    
    
    public void testClose() {
        XMLExecution execution = ProxyObjectFactory.getDefaultXMLExecution(m_vdbPath);
        try {
            execution.close();          
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }

    public void testCancel() {
        XMLExecution execution = ProxyObjectFactory.getDefaultXMLExecution(m_vdbPath);
         try {
            execution.cancel();            
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }

    public void testGetConnection() {
        XMLExecution execution = ProxyObjectFactory.getDefaultXMLExecution(m_vdbPath);
        XMLConnection conn = execution.getConnection();
        assertNotNull("XMLConnectionImpl is null", conn);
    }

    public void testSetConnection() {
        XMLExecutionImpl execution = ProxyObjectFactory.getDefaultXMLExecution(m_vdbPath);
        XMLConnectionImpl conn = ProxyObjectFactory.getDefaultXMLConnection();
        execution.setConnection(conn);
        assertEquals(conn, execution.getConnection());
    }

    public void testGetInfo() {
        XMLExecutionImpl execution = ProxyObjectFactory.getDefaultXMLExecution(m_vdbPath);
        assertNull(execution.getInfo());
        String queryString = "select Company_id from Company where Company_id is not null order by Company_id";
        IQuery query = ProxyObjectFactory.getDefaultIQuery(m_vdbPath, queryString);
        final int maxBatch = 1000;        
        try {
            execution.execute(query, maxBatch);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
        assertNotNull("ExecutionInfo is null", execution.getInfo());
    }
}
