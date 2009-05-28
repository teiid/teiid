/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.connector.xml.base;



import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.cdk.api.SysLogger;
import com.metamatrix.connector.xml.IQueryPreprocessor;
import com.metamatrix.connector.xml.MockQueryPreprocessor;

/**
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
        ConnectorLogger logger = new SysLogger(false);
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query, metadata, preprocessor, logger, Mockito.mock(ExecutionContext.class), Mockito.mock(ConnectorEnvironment.class));
        	assertNotNull("analyzer is null", analyzer);
        } catch (ConnectorException e) {
            fail(e.getMessage());
        }
    }

    public void testAnalyze() {
        IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
        RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        IQueryPreprocessor preprocessor = new MockQueryPreprocessor();
        ConnectorLogger logger = new SysLogger(false);
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query, metadata, preprocessor, logger, Mockito.mock(ExecutionContext.class), Mockito.mock(ConnectorEnvironment.class));
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
        ConnectorLogger logger = new SysLogger(false);
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query, metadata, preprocessor, logger, Mockito.mock(ExecutionContext.class), Mockito.mock(ConnectorEnvironment.class));
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
        ConnectorLogger logger = new SysLogger(false);
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query, metadata, preprocessor, logger, Mockito.mock(ExecutionContext.class), Mockito.mock(ConnectorEnvironment.class));
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
        ConnectorLogger logger = new SysLogger(false);
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query, metadata, preprocessor, logger, Mockito.mock(ExecutionContext.class), Mockito.mock(ConnectorEnvironment.class));
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
        ConnectorLogger logger = new SysLogger(false);
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query, metadata, preprocessor, logger, Mockito.mock(ExecutionContext.class), Mockito.mock(ConnectorEnvironment.class));
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
        ConnectorLogger logger = new SysLogger(false);
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query, metadata, preprocessor, logger, Mockito.mock(ExecutionContext.class), Mockito.mock(ConnectorEnvironment.class));
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
        ConnectorLogger logger = new SysLogger(false);
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query, metadata, preprocessor, logger, Mockito.mock(ExecutionContext.class), Mockito.mock(ConnectorEnvironment.class));
            analyzer.analyze();
        } catch (ConnectorException e) {
            fail(e.getMessage());
        }   	
    }
    
    public void testGetExecutionInfo() {
        IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
        RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        IQueryPreprocessor preprocessor = new MockQueryPreprocessor();
        ConnectorLogger logger = new SysLogger(false);
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query, metadata, preprocessor, logger, Mockito.mock(ExecutionContext.class), Mockito.mock(ConnectorEnvironment.class));
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
