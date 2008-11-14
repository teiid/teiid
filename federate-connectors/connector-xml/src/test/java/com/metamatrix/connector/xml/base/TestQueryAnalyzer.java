/*
 * © 2007 Varsity Gateway LLC. All Rights Reserved.
 */

package com.metamatrix.connector.xml.base;



import junit.framework.TestCase;

import com.metamatrix.cdk.api.SysLogger;
import com.metamatrix.connector.xml.IQueryPreprocessor;
import com.metamatrix.connector.xml.MockConnectorEnvironment;
import com.metamatrix.connector.xml.MockExecutionContext;
import com.metamatrix.connector.xml.MockQueryPreprocessor;
import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IQuery;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;

/**
 * created by JChoate on Jun 27, 2005
 *
 */
public class TestQueryAnalyzer extends TestCase {
    
	private static String vdbPath;
    private static final String QUERY = "select SimpleOutput from SimpleTable where SimpleOutput = 'MetaMatrix' order by SimpleOutput";
   
    static {
    	vdbPath = ProxyObjectFactory.getStateCollegeVDBLocation();
    }
    
//  removing hansel while testing clover
/*    
    public static Test suite() {
    	return new CoverageDecorator(QueryAnalyzerTest.class, new Class[] {QueryAnalyzer.class}); 
    }
  
*/
    /**
     * Constructor for QueryAnalyzerTest.
     * @param arg0
     */
    public TestQueryAnalyzer(String arg0) {
        super(arg0);
    }

    public void testQueryAnalyzer() {
        IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
        RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        IQueryPreprocessor preprocessor = new MockQueryPreprocessor();
        MockExecutionContext exeCtx = new MockExecutionContext();
        MockConnectorEnvironment connEnv = new MockConnectorEnvironment();
        ConnectorLogger logger = new SysLogger();
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query, metadata, preprocessor, logger, exeCtx, connEnv);
        	assertNotNull("analyzer is null", analyzer);
        } catch (ConnectorException e) {
            fail(e.getMessage());
        }
    }

    public void testAnalyze() {
        IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
        RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        IQueryPreprocessor preprocessor = new MockQueryPreprocessor();
        MockExecutionContext exeCtx = new MockExecutionContext();
        MockConnectorEnvironment connEnv = new MockConnectorEnvironment();
        ConnectorLogger logger = new SysLogger();
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query, metadata, preprocessor, logger, exeCtx, connEnv);
            analyzer.analyze();
        } catch (ConnectorException e) {
            fail(e.getMessage());
        }        
    }
    
    public void testAnalyzeSimpleSelect() {
    	String strQuery = "select SimpleOutput from SimpleTable";
        IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, strQuery);
        RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        IQueryPreprocessor preprocessor = new MockQueryPreprocessor();
        MockExecutionContext exeCtx = new MockExecutionContext();
        MockConnectorEnvironment connEnv = new MockConnectorEnvironment();
        ConnectorLogger logger = new SysLogger();
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query, metadata, preprocessor, logger, exeCtx, connEnv);
            analyzer.analyze();
        } catch (ConnectorException e) {
            fail(e.getMessage());
        }    	
    }
    
    public void testAnalyzeLiteralSelect() {
    	String strQuery = "select SimpleOutput, 'foo' from SimpleTable";
        IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, strQuery);
        RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        IQueryPreprocessor preprocessor = new MockQueryPreprocessor();
        MockExecutionContext exeCtx = new MockExecutionContext();
        MockConnectorEnvironment connEnv = new MockConnectorEnvironment();
        ConnectorLogger logger = new SysLogger();
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query, metadata, preprocessor, logger, exeCtx, connEnv);
            analyzer.analyze();
        } catch (ConnectorException e) {
            fail(e.getMessage());
        }    	
    }
    
    public void testAnalyzeFunctionSelect() {
    	String strQuery = "select concat(SimpleOutput, 'foo') from SimpleTable";
        IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, strQuery);
        RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        IQueryPreprocessor preprocessor = new MockQueryPreprocessor();
        MockExecutionContext exeCtx = new MockExecutionContext();
        MockConnectorEnvironment connEnv = new MockConnectorEnvironment();
        ConnectorLogger logger = new SysLogger();
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query, metadata, preprocessor, logger, exeCtx, connEnv);
        	analyzer.analyze();
        } catch (ConnectorException e) {
            fail(e.getMessage());
        }    	
    }

    public void testAnalyzeParameterSelect() {
    	String strQuery = "select SimpleParam from SimpleInput where SimpleParam in ('foo')";
        IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, strQuery);
        RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        IQueryPreprocessor preprocessor = new MockQueryPreprocessor();
        MockExecutionContext exeCtx = new MockExecutionContext();
        MockConnectorEnvironment connEnv = new MockConnectorEnvironment();
        ConnectorLogger logger = new SysLogger();
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query, metadata, preprocessor, logger, exeCtx, connEnv);
            analyzer.analyze();
        } catch (ConnectorException e) {
            fail(e.getMessage());
        }    	
    }
    
    public void testAnalyzeComplexQuery() {
    	String strQuery = "select SimpleOut from SimpleInput where SimpleParam in ('foo')";
        IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, strQuery);
        RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        IQueryPreprocessor preprocessor = new MockQueryPreprocessor();
        MockExecutionContext exeCtx = new MockExecutionContext();
        MockConnectorEnvironment connEnv = new MockConnectorEnvironment();
        ConnectorLogger logger = new SysLogger();
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query, metadata, preprocessor, logger, exeCtx, connEnv);
            analyzer.analyze();
        } catch (ConnectorException e) {
            fail(e.getMessage());
        }   	
    }
    
    public void testAnalyzeComplexQuery2() {
    	String strQuery = "select SimpleOut from SimpleInput where SimpleParam in ('foo') and OtherOut in ('bar')";
        IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, strQuery);
        RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        IQueryPreprocessor preprocessor = new MockQueryPreprocessor();
        MockExecutionContext exeCtx = new MockExecutionContext();
        MockConnectorEnvironment connEnv = new MockConnectorEnvironment();
        ConnectorLogger logger = new SysLogger();
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query, metadata, preprocessor, logger, exeCtx, connEnv);
            analyzer.analyze();
        } catch (ConnectorException e) {
            fail(e.getMessage());
        }   	
    }
    
    public void testGetExecutionInfo() {
        IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
        RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        IQueryPreprocessor preprocessor = new MockQueryPreprocessor();
        MockExecutionContext exeCtx = new MockExecutionContext();
        MockConnectorEnvironment connEnv = new MockConnectorEnvironment();
        ConnectorLogger logger = new SysLogger();
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query, metadata, preprocessor, logger, exeCtx, connEnv);
	        assertNotNull("analyzer is null", analyzer);
	        ExecutionInfo base = analyzer.getExecutionInfo();
	        assertEquals(1, base.getColumnCount());
            analyzer.analyze();
            ExecutionInfo post = analyzer.getExecutionInfo();
            assertTrue(post.getColumnCount() > 0);
            assertEquals(1, post.getCriteria().size());
            assertEquals(1, post.getRequestedColumns().size());
            assertNotNull(post.getTableXPath());
        } catch (ConnectorException e) {
            fail(e.getMessage());
        }

    }    
}
