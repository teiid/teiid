/*
 * © 2007 Varsity Gateway LLC. All Rights Reserved.
 */

package com.metamatrix.connector.xml.base;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import junit.framework.TestCase;

import com.metamatrix.connector.xml.XMLConnectorState;
import com.metamatrix.connector.xml.XMLExecution;
import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.exception.ConnectorException;
//import junit.framework.TestSuite;

/**
 * created by JChoate on Jun 27, 2005
 *
 */
public class TestExecutor extends TestCase {

	//TODO:  simulate table w/ empty nameInSource
	//TODO:  simulate table w/ blank nameInSource
	//TODO:  simulate column w/ empty NameInSource
	//TODO:  run with preprocess set to true
	
    private static final String VDBPATH = ProxyObjectFactory.getStateCollegeVDBLocation();
    
    /**
     * Constructor for ExecutorTest.
     * @param arg0
     */
    public TestExecutor(String arg0) {
        super(arg0);
    }
    
/*
    public void testExecutor() {

        XMLExecutionImpl execution = ProxyObjectFactory.getDefaultXMLExecution(VDBPATH);
        XMLConnectorStateImpl state = execution.getConnection().getState();//ProxyObjectFactory.getDefaultXMLConnector().getState();
        try{
	        RequestResponseDocumentProducer exec = new ExecutorImpl(state, execution);
	        
	        assertNotNull("state is null", exec.getState());
	        assertEquals(state.isPreprocess(), exec.getState().isPreprocess());
	        assertEquals(state.getCacheLocation(), exec.getState().getCacheLocation());
	        assertEquals(state.getCacheTimeoutMillis(), exec.getState().getCacheTimeoutMillis());
	        assertEquals(state.getMaxFileCacheSizeByte(), exec.getState().getMaxFileCacheSizeByte());
	        assertEquals(state.getMaxMemoryCacheSizeByte(), exec.getState().getMaxMemoryCacheSizeByte());
	                                
	        assertNotNull("execution is null", exec.getExecution());
	        assertEquals(execution, exec.getExecution());
	        
	        assertNull("executionInfo is not null", exec.getExecutionInfo());
	        assertEquals(execution.getInfo(), exec.getExecutionInfo());
        } catch (ConnectorException e) {
        	fail(e.getMessage());
        }
    }
*/
/*
    public void testGetState() {
        RequestResponseDocumentProducer exec = createExecutor();
        XMLConnectorStateImpl state = exec.getState();
        assertNotNull("state is null", state);
        assertEquals(state.isPreprocess(), exec.getState().isPreprocess());
        assertEquals(state.getCacheLocation(), exec.getState().getCacheLocation());
        assertEquals(state.getCacheTimeoutMillis(), exec.getState().getCacheTimeoutMillis());
        assertEquals(state.getMaxFileCacheSizeByte(), exec.getState().getMaxFileCacheSizeByte());
        assertEquals(state.getMaxMemoryCacheSizeByte(), exec.getState().getMaxMemoryCacheSizeByte());
        
    }
*/
/*    public void testGetExecutionInfo() {
    	ExecutorImpl exec = (ExecutorImpl)createExecutor();
        try {
        	final int maxBatch = 50;
        	final String strQuery = "select Company_id from Company where Company_id = 'MetaMatrix' order by Company_id";
        	IQuery query = ProxyObjectFactory.getDefaultIQuery(VDBPATH, strQuery);
        	exec.getExecution().execute(query, maxBatch);
        } catch (ConnectorException ce) {
        	ce.printStackTrace();
        	fail(ce.getMessage());
        }
        ExecutionInfo info = exec.getExecutionInfo();
        assertNotNull("executionInfo is null", info);
    }
*/
/*
    public void testGetResult() {
        RequestResponseDocumentProducer exec = createExecutor();        
        try {
            final String strQuery = "select Company_id from Company";
            IQuery query = ProxyObjectFactory.getDefaultIQuery(VDBPATH, strQuery);
            final int maxBatch = 50;        	
        	exec.getExecution().execute(query, maxBatch);
            List list = exec.getResult();
            assertFalse(list.size() == 0);            
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }                
    }
*/    
    
/*   public void testGetResultDefaultNamespace() {
    	XMLExecutionImpl execution = ProxyObjectFactory.getDefaultXMLExecution(VDBPATH);
        FileConnectorState state = (FileConnectorState) ProxyObjectFactory.getDefaultXMLConnector().getState();
        state.setFileName("state_college_default_namespace.xml");
        RequestResponseDocumentProducer exec = new ExecutorImpl(state, execution);     
        try {
            final String strQuery = "select Company_id from Company";
            IQuery query = ProxyObjectFactory.getDefaultIQuery(VDBPATH, strQuery);
            final int maxBatch = 50;        	
        	exec.getExecution().execute(query, maxBatch);
            List list = exec.getResult();
            assertFalse(list.size() == 0);            
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }  
    }
*/    
    
/*    public void testGetResultClob() {
    	String vdb = ProxyObjectFactory.getDocumentsFolder() + "/Gutenberg.vdb";
        
    	Properties testFileProps = new Properties();
        testFileProps.put(XMLConnectorStateImpl.CACHE_TIMEOUT, new String("500000"));
        testFileProps.put(XMLConnectorStateImpl.MAX_MEMORY_CACHE_SIZE, new String("5000"));
        testFileProps.put(XMLConnectorStateImpl.MAX_FILE_CACHE_SIZE, new String("5000"));
        testFileProps.put(XMLConnectorStateImpl.CACHE_ENABLED, Boolean.TRUE);
        testFileProps.put(XMLConnectorStateImpl.FILE_CACHE_LOCATION, new String("./test/cache"));
        testFileProps.setProperty(XMLConnectorStateImpl.STATE_CLASS_PROP, "com.metamatrix.connector.xml.file.FileConnectorState");
        
        testFileProps.put(FileConnectorState.FILE_NAME, "mdprp10.xml");
        String localPath = "test/documents/books";
        String ccPath = "checkout/XMLConnectorFramework/" + localPath;
        if (new File(localPath).exists()) {
        	testFileProps.put(FileConnectorState.DIRECTORY_PATH, localPath);
        } else {
        	if (new File(ccPath).exists()) {
        		testFileProps.put(FileConnectorState.DIRECTORY_PATH, ccPath);
        	} else {
        		testFileProps.put(FileConnectorState.DIRECTORY_PATH, "");
        	}
        }
        
        
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(testFileProps);
    	XMLConnector ctor = new XMLConnector();
        try {
	    	
        	ctor.initialize(env);
        	SecurityContext exCtx = EnvironmentUtility.createExecutionContext("Request", "testPartId");
        	XMLConnectionImpl conn = new XMLConnectionImpl(ctor, exCtx, env);
	    	
	    	TranslationUtility transUtil = new TranslationUtility(vdb);
	        RuntimeMetadata meta = transUtil.createRuntimeMetadata();  
	    	XMLExecutionImpl execution = new XMLExecutionImpl(conn, meta, exCtx, env);
	    	
        RequestResponseDocumentProducer exec = new ExecutorImpl(ctor.getState(), execution);     
           final String strQuery = "select Title, Text from Document";
            IQuery query = ProxyObjectFactory.getDefaultIQuery(vdb, strQuery);
            final int maxBatch = 50;        	
        	exec.getExecution().execute(query, maxBatch);
            List list = exec.getResult();
            assertFalse(list.size() == 0);            
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }  
    }
*/    
    public void testGetResultNullTableNIS() {
    	
    }
    
    public void testGetResultEmptyTableNIS() {
    	
    }
    
    public void testGetResultNullColumnNIS() {
    	
    }
    
/*    public void testGetResultPreprocess() {
        XMLExecutionImpl execution = ProxyObjectFactory.getDefaultXMLExecution(VDBPATH);
        XMLConnectorStateImpl state = ProxyObjectFactory.getDefaultXMLConnector().getState();
        state.setPreprocess(true);
        RequestResponseDocumentProducer exec = new ExecutorImpl(state, execution);
        try {
            final String strQuery = "select Company_id from Company";
            IQuery query = ProxyObjectFactory.getDefaultIQuery(VDBPATH, strQuery);
            final int maxBatch = 50;        	
        	exec.getExecution().execute(query, maxBatch);
            List list = exec.getResult();
            assertFalse(list.size() == 0);            
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }  
    }
*/
/*    public void testAddNamespacePairs() {
        final String key = "ut";
        final String ns = "http://unit/test";
        String namespaceString = "xmlns:" + key + "=\"" + ns + "\"";
        RequestResponseDocumentProducer exec = createExecutor();
        Map pairs = new HashMap();
        try {
            String defaultNamespace = exec.getNamespaces(pairs, namespaceString);
            assertEquals(defaultNamespace, null);
            exec.addNamespacePairs(new JDOMXPath("/mydata/ut:company"), pairs);
        } catch (Exception ex) {
            ex.printStackTrace();
            fail(ex.getMessage());
        }        
    }
*/
/*
    public void testPreprocessDoc() {
        Executor exec = createExecutor();
        try {
            XMLDocument[] docs = exec.getXMLDocument();
            Document doc = exec.preprocessDoc(docs[0].getDocument());
            assertNotNull("preprocess doc is null", doc);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }
*/    
/*
    public void testGetExecution() {
        RequestResponseDocumentProducer exec = createExecutor();
        XMLExecution execute = exec.getExecution();
        assertNotNull("XMLExecutionImpl is null", execute);
        XMLExecution exec2 = ProxyObjectFactory.getDefaultXMLExecution(VDBPATH);
        XMLExecution exec3 = exec.getExecution();
        assertEquals(exec2.getConnection().getQueryId(), exec3.getConnection().getQueryId());
        assertEquals(exec2.getInfo(), exec3.getInfo());
    }
*/  
/*
    public void testGetLogger() {
    	RequestResponseDocumentProducer exec = createExecutor();
    	ConnectorLogger log = exec.getLogger();
    	assertNotNull(log);
    }
*/
    private RequestResponseDocumentProducer createExecutor() {
        XMLExecutionImpl execution = ProxyObjectFactory.getDefaultXMLExecution(VDBPATH);
        XMLConnectorState state = ProxyObjectFactory.getDefaultXMLConnector().getState();
        RequestResponseDocumentProducer exec = null;
		try {
			exec = new ExecutorImpl(state, execution);
		} catch (ConnectorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return exec;
    }
    
    private class ExecutorImpl extends RequestResponseDocumentProducer {

        
        protected ExecutorImpl(XMLConnectorState state, XMLExecutionImpl execution) throws ConnectorException {
            super(state, execution);
            String baseLocation = "test/documents";
            String ccLocation = "checkout/XMLConnectorFramework/" + baseLocation;
            File baseFile = new File(baseLocation);
            if (baseFile.exists()) {
                dirFile = baseFile;
            } else {
                dirFile = new File(ccLocation);
            }

        }

        File dirFile;
        String[] xmlDocs = {
            "state_college.xml",
			"state_college2.xml"
        };

        public int getDocumentCount() throws ConnectorException
        {
            return xmlDocs.length;
        }
        
        public int getCachedSize(int i) throws ConnectorException
        {
            File file = new File(dirFile, xmlDocs[i]);
            long length = file.length();
            return (int)file.length() * 5;
        }
        
        public String getCacheKey(int i) throws ConnectorException
        {
            File file = new File(dirFile, xmlDocs[i]);
            try {
                return file.getCanonicalPath();
            }
            catch (IOException e) {
            	throw new ConnectorException(e);   
            }
        }

        public InputStream getDocumentStream(int i) throws ConnectorException
        {
            File file = new File(dirFile, xmlDocs[i]);
            try {
            	return new FileInputStream(file);
            }
            catch (IOException e)
            {
            	throw new ConnectorException(e);
            }
        };        
        
        public void releaseDocumentStream(int i) throws ConnectorException
        {
        }
        
        public Serializable getRequestObject(int i) {
        	return null;
        }

		public Response getXMLResponse(int i) throws ConnectorException {
			return null;
		}
        
    }
}
