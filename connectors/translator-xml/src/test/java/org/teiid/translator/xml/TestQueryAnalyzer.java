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

package org.teiid.translator.xml;



import junit.framework.TestCase;

import org.teiid.language.Select;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.xml.ExecutionInfo;
import org.teiid.translator.xml.QueryAnalyzer;

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
        Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query);
        	assertNotNull("analyzer is null", analyzer);
        } catch (TranslatorException e) {
            fail(e.getMessage());
        }
    }

    public void testAnalyze() {
        Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query);
            analyzer.analyze();
        } catch (TranslatorException e) {
            fail(e.getMessage());
        }        
    }
    
    public void testAnalyzeSimpleSelect() {
    	String strQuery = "select SimpleOutput from SimpleTable";
        Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, strQuery);
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query);
            analyzer.analyze();
        } catch (TranslatorException e) {
            fail(e.getMessage());
        }    	
    }
    
    public void testAnalyzeLiteralSelect() {
    	String strQuery = "select SimpleOutput, 'foo' from SimpleTable";
        Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, strQuery);
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query);
            analyzer.analyze();
        } catch (TranslatorException e) {
            fail(e.getMessage());
        }    	
    }
    
    public void testAnalyzeFunctionSelect() {
    	String strQuery = "select concat(SimpleOutput, 'foo') from SimpleTable";
        Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, strQuery);
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query);
        	analyzer.analyze();
        } catch (TranslatorException e) {
            fail(e.getMessage());
        }    	
    }

    public void testAnalyzeParameterSelect() {
    	String strQuery = "select SimpleParam from SimpleInput where SimpleParam in ('foo')";
        Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, strQuery);
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query);
            analyzer.analyze();
        } catch (TranslatorException e) {
            fail(e.getMessage());
        }    	
    }
    
    public void testAnalyzeComplexQuery() {
    	String strQuery = "select SimpleOut from SimpleInput where SimpleParam in ('foo')";
        Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, strQuery);
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query);
            analyzer.analyze();
        } catch (TranslatorException e) {
            fail(e.getMessage());
        }   	
    }
    
    public void testAnalyzeComplexQuery2() {
    	String strQuery = "select SimpleOut from SimpleInput where SimpleParam in ('foo') and OtherOut in ('bar')";
        Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, strQuery);
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query);
            analyzer.analyze();
        } catch (TranslatorException e) {
            fail(e.getMessage());
        }   	
    }
    
    public void testGetExecutionInfo() {
        Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
        try {
        	QueryAnalyzer analyzer = new QueryAnalyzer(query);
	        assertNotNull("analyzer is null", analyzer);
	        ExecutionInfo base = analyzer.getExecutionInfo();
	        assertEquals(1, base.getColumnCount());
            analyzer.analyze();
            ExecutionInfo post = analyzer.getExecutionInfo();
            assertTrue(post.getColumnCount() > 0);
            assertEquals(1, post.getCriteria().size());
            assertEquals(1, post.getRequestedColumns().size());
            assertNotNull(post.getTableXPath());
        } catch (TranslatorException e) {
            fail(e.getMessage());
        }

    }    
}
