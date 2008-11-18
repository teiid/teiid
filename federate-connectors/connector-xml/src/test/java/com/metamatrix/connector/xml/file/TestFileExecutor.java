/*
 * © 2007 Varsity Gateway LLC. All Rights Reserved.
 */

package com.metamatrix.connector.xml.file;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.cdk.api.SysLogger;
import com.metamatrix.connector.xml.MockXMLExecution;
import com.metamatrix.connector.xml.base.ExecutionInfo;
import com.metamatrix.connector.xml.base.OutputXPathDesc;
import com.metamatrix.connector.xml.base.ProxyObjectFactory;
import com.metamatrix.connector.xml.base.XMLConnectionImpl;
import com.metamatrix.connector.xml.base.XMLConnector;
import com.metamatrix.connector.xml.base.XMLConnectorStateImpl;
import com.metamatrix.connector.xml.base.XMLDocument;
import com.metamatrix.connector.xml.base.XMLExecutionImpl;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IElement;
import com.metamatrix.data.language.IExpression;
import com.metamatrix.data.language.IQuery;
import com.metamatrix.data.language.ISelectSymbol;
import com.metamatrix.data.metadata.runtime.Element;
import com.metamatrix.data.metadata.runtime.MetadataID;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;

/**
 * created by JChoate on Jun 27, 2005
 *
 */
public class TestFileExecutor extends TestCase {

    /**
     * Constructor for FileExecutorTest.
     * @param arg0
     */
    public TestFileExecutor(String arg0) {
        super(arg0);
    }

    public void testValidate() {
    	try {
    		FileConnectorState state = new FileConnectorState();
        	state.setLogger(new SysLogger());
        	state.setState(getEnv(ProxyObjectFactory.getDefaultFileProps()));
        	XMLConnectionImpl conn = ProxyObjectFactory.getDefaultXMLConnection();
        	RuntimeMetadata meta = ProxyObjectFactory.getDefaultRuntimeMetadata(ProxyObjectFactory.getStateCollegeVDBLocation());
        	MockXMLExecution exec = new MockXMLExecution(conn, meta);
        	FileExecutor executor = new FileExecutor(state, exec);
		} catch (ConnectorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
    }
    
    public void testValidateSourceFromModel() {
    	try {
    		FileConnectorState state = new FileConnectorState();
        	state.setLogger(new SysLogger());
        	state.setState(getEnv(ProxyObjectFactory.getDefaultFileProps()));
        	XMLConnectionImpl conn = ProxyObjectFactory.getDefaultXMLConnection();
        	RuntimeMetadata meta = ProxyObjectFactory.getDefaultRuntimeMetadata(ProxyObjectFactory.getStateCollegeVDBLocation());
        	MockXMLExecution exec = new MockXMLExecution(conn, meta);
    		ExecutionInfo info = exec.getInfo();
        	Properties props = info.getOtherProperties();
        	props.put(FileExecutor.PARM_FILE_NAME_TABLE_PROPERTY_NAME, "state_college.xml");
        	info.setOtherProperties(props);
        	FileExecutor executor = new FileExecutor(state, exec);
		} catch (ConnectorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
    	
    }
    
    public void testValidateSourceDirectory() {
    	try {
    		FileConnectorState state = new FileConnectorState();
        	state.setLogger(new SysLogger());
        	Properties props;
        	state.setState(getEnv(ProxyObjectFactory.getDefaultFileProps()));
        	state.setFileName(null);
        	XMLConnectionImpl conn = ProxyObjectFactory.getDefaultXMLConnection();
        	RuntimeMetadata meta = ProxyObjectFactory.getDefaultRuntimeMetadata(ProxyObjectFactory.getStateCollegeVDBLocation());
        	MockXMLExecution exec = new MockXMLExecution(conn, meta);
    		ExecutionInfo info = exec.getInfo();
        	props = info.getOtherProperties();
        	props.put(FileExecutor.PARM_FILE_NAME_TABLE_PROPERTY_NAME, " ");
        	info.setOtherProperties(props);
        	FileExecutor executor = new FileExecutor(state, exec);
		} catch (ConnectorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
    	
    }
    
    public void testValidateSourceDirectoryEmpty() {
	try {
		FileConnectorState state = new FileConnectorState();
    	state.setLogger(new SysLogger());
    	state.setState(getEnv(ProxyObjectFactory.getDefaultFileProps()));
    	state.setFileName(null);
    	File nullDir = UnitTestUtil.getTestScratchFile("devnull");
    	if(!nullDir.exists()) {
    		assertTrue("could not create directory nulldir", nullDir.mkdir());
    	}
    	assertTrue("nulldir is not a directory", nullDir.isDirectory());
    	assertTrue("nulldir is not empty", nullDir.list().length == 0);
    	state.setDirectoryPath(nullDir.getAbsolutePath());
    	XMLConnectionImpl conn = ProxyObjectFactory.getDefaultXMLConnection();
    	RuntimeMetadata meta = ProxyObjectFactory.getDefaultRuntimeMetadata(ProxyObjectFactory.getStateCollegeVDBLocation());
    	MockXMLExecution exec = new MockXMLExecution(conn, meta);
		ExecutionInfo info = exec.getInfo();
    	Properties props = info.getOtherProperties();
    	props.put(FileExecutor.PARM_FILE_NAME_TABLE_PROPERTY_NAME, " ");
    	info.setOtherProperties(props);
    	FileExecutor executor = new FileExecutor(state, exec);
		fail("empty directory should not validate");
		} catch (ConnectorException e) {
			assertNotNull(e);
		}
    	
    }
    
    public void testValidateFileAsDirectory() {
    	try {
    		FileConnectorState state = new FileConnectorState();
    	state.setLogger(new SysLogger());
    	state.setState(getEnv(ProxyObjectFactory.getDefaultFileProps()));
    	state.setFileName(null);
    	File nullDir = UnitTestUtil.getTestScratchFile("devnull");
    	if(!nullDir.exists()) {
    		assertTrue("could not create directory nulldir", nullDir.mkdir());
    	}
    	assertTrue("nulldir is not a directory", nullDir.isDirectory());
    	assertTrue("nulldir is not empty", nullDir.list().length == 0);
    	state.setDirectoryPath(nullDir.getAbsolutePath());
    	state.setFileName(nullDir.getName());
    	XMLConnectionImpl conn = ProxyObjectFactory.getDefaultXMLConnection();
    	RuntimeMetadata meta = ProxyObjectFactory.getDefaultRuntimeMetadata(ProxyObjectFactory.getStateCollegeVDBLocation());
    	MockXMLExecution exec = new MockXMLExecution(conn, meta);
    	
    		FileExecutor executor = new FileExecutor(state, exec);
			fail("directory specified in filename should fail");
		} catch (ConnectorException e) {
			assertNotNull(e);
		}	
    }
    
    public void testValidateParameter() {
    	try {
    		FileConnectorState state = new FileConnectorState();
    	state.setLogger(new SysLogger());
    	state.setState(getEnv(ProxyObjectFactory.getDefaultFileProps()));
    	String strQuery = "select DefaultedValue from DefaultedRequiredValueTable";
    	
    	XMLConnectionImpl conn = ProxyObjectFactory.getDefaultXMLConnection();
    	RuntimeMetadata meta = ProxyObjectFactory.getDefaultRuntimeMetadata(ProxyObjectFactory.getStateCollegeVDBLocation());
    	MockXMLExecution exec = new MockXMLExecution(conn, meta);
		ExecutionInfo info = exec.getInfo();
		info.setColumnCount(1);
		ArrayList param = new ArrayList();
    			
			Element elem = getElement(strQuery);
			OutputXPathDesc desc = new OutputXPathDesc(elem);
			param.add(desc);
			info.setRequestedColumns(param);
	    	FileExecutor executor = new FileExecutor(state, exec);
			fail("should not validate a query with param columns");
		} catch (ConnectorException e) {
			assertNotNull(e);
		}
    }
    
    public void testValidateNotXML() {
    	try {
    		FileConnectorState state = new FileConnectorState();
    	state.setLogger(new SysLogger());
    	state.setState(getEnv(ProxyObjectFactory.getDefaultFileProps()));
    	state.setFileName("StateCollege.vdb");
    	XMLConnectionImpl conn = ProxyObjectFactory.getDefaultXMLConnection();
    	RuntimeMetadata meta = ProxyObjectFactory.getDefaultRuntimeMetadata(ProxyObjectFactory.getStateCollegeVDBLocation());
    	MockXMLExecution exec = new MockXMLExecution(conn, meta);
    	
    		FileExecutor executor = new FileExecutor(state, exec);
		} catch (ConnectorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
    }

    public void testGetXMLDocument() {
    	try {
    		FileConnectorState state = new FileConnectorState();
    	state.setLogger(new SysLogger());
    	state.setState(getEnv(ProxyObjectFactory.getDefaultFileProps()));
    	XMLExecutionImpl exec = ProxyObjectFactory.getDefaultXMLExecution(ProxyObjectFactory.getStateCollegeVDBLocation());
    	
    		final int maxBatch = 50;
			exec.execute(ProxyObjectFactory.getDefaultIQuery(ProxyObjectFactory.getStateCollegeVDBLocation(),
					"select Company_id from Company"), maxBatch);
		
        	FileExecutor executor = new FileExecutor(state, exec);
	    	XMLDocument[] docs = executor.getXMLResponse(0).getDocuments();
	    	assertEquals(1, docs.length);
	    	assertNotNull(docs[0]);
    	} catch (ConnectorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

    }
    
    public void testGetXMLDocumentCache() {
    	try {
    	FileConnectorState state = new FileConnectorState();
    	state.setLogger(new SysLogger());
    	state.setState(getEnv(ProxyObjectFactory.getDefaultFileProps()));
    	XMLExecutionImpl exec = ProxyObjectFactory.getDefaultXMLExecution(ProxyObjectFactory.getStateCollegeVDBLocation());
    	
    		final int maxBatch = 50;
			exec.execute(ProxyObjectFactory.getDefaultIQuery(ProxyObjectFactory.getStateCollegeVDBLocation(),
					"select Company_id from Company"), maxBatch);
			FileExecutor executor = new FileExecutor(state, exec);
	    	XMLDocument[] docs = executor.getXMLResponse(0).getDocuments();
	    	assertEquals(1, docs.length);
	    	assertNotNull(docs[0]);
	    	docs = executor.getXMLResponse(0).getDocuments();
	    	assertEquals(1, docs.length);
	    	assertNotNull(docs[0]);
    	} catch (ConnectorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

    }
 
    
    public void testGetXMLDocumentError() {
    	try {
            
    		FileConnectorState state = new FileConnectorState();
    	state.setLogger(new SysLogger());
    	state.setState(getEnv(ProxyObjectFactory.getDefaultFileProps()));
    	String fileName = "empty.xml";
    	File tempFile = new File(ProxyObjectFactory.getDocumentsFolder() + "/" + fileName);
    	if(!tempFile.exists()) {
    		try {
    			tempFile.createNewFile();
    		} catch(IOException ioe) {
    			fail("could not create temp file");
    		}
    	}
    	state.setFileName("empty.xml");
    	XMLConnectionImpl conn = ProxyObjectFactory.getDefaultXMLConnection();
    	RuntimeMetadata meta = ProxyObjectFactory.getDefaultRuntimeMetadata(ProxyObjectFactory.getStateCollegeVDBLocation());
    	MockXMLExecution exec = new MockXMLExecution(conn, meta);
    		FileExecutor executor = new FileExecutor(state, exec);
			assertTrue(tempFile.delete());
			tempFile = null;
			XMLDocument[] docs = executor.getXMLResponse(0).getDocuments();
			fail("should not be able to get non-existant document");
		} catch (ConnectorException e) {
			assertNotNull(e);
		}
    }
    
    
    public void testUniqueIds() {
		String query = "select Author_Last, Title, TitleId1, TitleId2 from Document"; //$NON-NLS-1$
        String vdb = ProxyObjectFactory.getDocumentsFolder() + "/Gutenberg.vdb";//$NON-NLS-1$
        Properties props = new Properties();
        
        
        props.setProperty(XMLConnectorStateImpl.CACHE_TIMEOUT, new String("5000")); //$NON-NLS-1$
        props.setProperty(XMLConnectorStateImpl.MAX_MEMORY_CACHE_SIZE, new String("0")); //$NON-NLS-1$
        props.setProperty(XMLConnectorStateImpl.MAX_FILE_CACHE_SIZE, new String("5000")); //$NON-NLS-1$
        //props.setProperty(XMLConnectorStateImpl.PREPROCESS, Boolean.FALSE.toString());
        props.setProperty(XMLConnectorStateImpl.CACHE_ENABLED, Boolean.TRUE.toString());
        props.setProperty(XMLConnectorStateImpl.STATE_CLASS_PROP, "com.metamatrix.connector.xml.file.FileConnectorState"); //$NON-NLS-1$
        props.setProperty(XMLConnectorStateImpl.FILE_CACHE_LOCATION, "cache"); //$NON-NLS-1$
        props.setProperty(XMLConnectorStateImpl.MAX_IN_MEMORY_STRING_SIZE, "1"); //$NON-NSL-1$
        props.setProperty(XMLConnectorStateImpl.CONNECTOR_CAPABILITES, "com.metamatrix.connector.xml.base.XMLCapabilities"); //$NON-NSL-1$
        props.setProperty(XMLConnectorStateImpl.SAX_FILTER_PROVIDER_CLASS, "com.metamatrix.connector.xml.base.NoExtendedFilters");
        props.setProperty(XMLConnectorStateImpl.QUERY_PREPROCESS_CLASS, "com.metamatrix.connector.xml.base.NoQueryPreprocessing");
        
        props.setProperty(FileConnectorState.DIRECTORY_PATH, ProxyObjectFactory.getDocumentsFolder() + "/books");
        props.setProperty(FileConnectorState.FILE_NAME, "mdprp10.xml");
        //props.setProperty(FileConnectorState.PREPROCESS, "true");
        
        try { 
        	
        	FileConnectorState state = new FileConnectorState();
        	state.setLogger(new SysLogger());
        	state.setState(getEnv(props));

        	String requestId = "Request"; //$NON-NLS-1$
        	String partId = "testPartId"; //$NON-NLS-1$

        	ExecutionContext context = EnvironmentUtility.createExecutionContext(requestId, partId);
            ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props);
            RuntimeMetadata meta = ProxyObjectFactory.getDefaultRuntimeMetadata(vdb);
                
            SecurityContext ctx = ProxyObjectFactory.getDefaultSecurityContext();
            
            XMLConnector conn = new XMLConnector();
            conn.initialize(env);
            
            Connection cn = (XMLConnectionImpl) conn.getConnection(ctx);
            XMLExecutionImpl exec = (XMLExecutionImpl)cn.createExecution(ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERY, context, meta);
    		
        	final int batchSize = 50;
    		
        	IQuery iq = ProxyObjectFactory.getDefaultIQuery(vdb, query);
    	    	
    		exec.execute(iq, batchSize);
    		
    		Batch batch = exec.nextBatch();
    		List[] results = batch.getResults();
    		for(int i = 0; i < results.length; i++) {
    			List theList = results[i];
    			Iterator iter = theList.iterator();
    			while(iter.hasNext()) {
    				Object col = iter.next();
    				System.out.print("\t" + col);    				
    			}
    			System.out.println();
    		}    		
    	} catch (ConnectorException e) {
    		e.printStackTrace();
    		fail(e.getMessage());
    	}
	}
    
	public void testUniqueIdsForMultipleDocs() {
		String query = "select Author_Last, Title, TitleId1, TitleId2 from Document"; //$NON-NLS-1$
        String vdb = ProxyObjectFactory.getDocumentsFolder() + "/Gutenberg.vdb";//$NON-NLS-1$
        Properties props = new Properties();
        
        
        props.setProperty(XMLConnectorStateImpl.CACHE_TIMEOUT, new String("5000")); //$NON-NLS-1$
        props.setProperty(XMLConnectorStateImpl.MAX_MEMORY_CACHE_SIZE, new String("0")); //$NON-NLS-1$
        props.setProperty(XMLConnectorStateImpl.MAX_FILE_CACHE_SIZE, new String("5000")); //$NON-NLS-1$
        props.setProperty(XMLConnectorStateImpl.CACHE_ENABLED, Boolean.TRUE.toString());
        props.setProperty(XMLConnectorStateImpl.STATE_CLASS_PROP, "com.metamatrix.connector.xml.file.FileConnectorState"); //$NON-NLS-1$
        props.setProperty(XMLConnectorStateImpl.FILE_CACHE_LOCATION, "cache"); //$NON-NLS-1$
        props.setProperty(XMLConnectorStateImpl.MAX_IN_MEMORY_STRING_SIZE, "1"); //$NON-NSL-1$
        props.setProperty(XMLConnectorStateImpl.CONNECTOR_CAPABILITES, "com.metamatrix.connector.xml.base.XMLCapabilities"); //$NON-NSL-1$
        props.setProperty(XMLConnectorStateImpl.SAX_FILTER_PROVIDER_CLASS, "com.metamatrix.connector.xml.base.NoExtendedFilters");
        props.setProperty(XMLConnectorStateImpl.QUERY_PREPROCESS_CLASS, "com.metamatrix.connector.xml.base.NoQueryPreprocessing");
        
        props.setProperty(FileConnectorState.DIRECTORY_PATH, ProxyObjectFactory.getDocumentsFolder() + "/books");
        props.setProperty(FileConnectorState.FILE_NAME, "");
        
        try { 
        	
        	FileConnectorState state = new FileConnectorState();
        	state.setLogger(new SysLogger());
        	state.setState(getEnv(props));

        	String requestId = "Request2"; //$NON-NLS-1$
        	String partId = "testPartId2"; //$NON-NLS-1$

        	ExecutionContext context = EnvironmentUtility.createExecutionContext(requestId, partId);
            ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props);
            RuntimeMetadata meta = ProxyObjectFactory.getDefaultRuntimeMetadata(vdb);
                
            SecurityContext ctx = ProxyObjectFactory.getDefaultSecurityContext();
            
            XMLConnector conn = new XMLConnector();
            conn.initialize(env);
            
            Connection cn = (XMLConnectionImpl) conn.getConnection(ctx);
            XMLExecutionImpl exec = (XMLExecutionImpl)cn.createExecution(ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERY, context, meta);
    		
        	final int batchSize = 50;
    		
        	IQuery iq = ProxyObjectFactory.getDefaultIQuery(vdb, query);
    	    	
    		exec.execute(iq, batchSize);
    		
    		Batch batch = exec.nextBatch();
    		List[] results = batch.getResults();
    		for(int i = 0; i < results.length; i++) {
    			List theList = results[i];
    			Iterator iter = theList.iterator();
    			while(iter.hasNext()) {
    				Object col = iter.next();
    				System.out.print("\t" + col);    				
    			}
    			System.out.println();
    		}    		
    	} catch (ConnectorException e) {
    		e.printStackTrace();
    		fail(e.getMessage());
    	}
	}
    
    private Element getElement(String query) throws ConnectorException {
    	return getElement(query, 0);
    }
    
    private Element getElement(String query, int colLocation)
			throws ConnectorException {
    	String vdbPath = ProxyObjectFactory.getStateCollegeVDBLocation();
        RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
    	Element elem = (Element) metadata.getObject(elementID);
    	return elem;        		
	}
    
    private ConnectorEnvironment getEnv(Properties props) {
    	return EnvironmentUtility.createEnvironment(props);
    }


}
