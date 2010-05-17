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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.teiid.language.Select;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.xml.ExecutionInfo;
import org.teiid.translator.xml.QueryAnalyzer;

/**
 *
 */
public class TestExecutionInfo extends TestCase {

    /**
     * Constructor for ExecutionInfoTest.
     * @param arg0
     */
    
    private static final String QUERY = "select Company_id from Company where Company_id = 'MetaMatrix' order by Company_id";
    
    private ExecutionInfo m_info;

    //removing hansel while testing clover
/*    
    public static Test suite() {
    	return new CoverageDecorator(ExecutionInfoTest.class, new Class[] {ExecutionInfo.class});
    	
    }
*/  
    
    public TestExecutionInfo(String arg0) {
        super(arg0);
    }
    
    @Override
	public void setUp() throws TranslatorException {
     String vdbPath = ProxyObjectFactory.getStateCollegeVDBLocation();
     Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
     QueryAnalyzer analyzer = new QueryAnalyzer(query);
     analyzer.analyze();
     m_info = analyzer.getExecutionInfo();
     
    }
    
    @Override
	public void tearDown() {
        m_info = null;        
    }

    public void testExecutionInfo() {
        ExecutionInfo info = new ExecutionInfo();
        assertNotNull("ExecutionInfo is null", info);
        assertEquals(0, info.getColumnCount());
        assertNotNull(info.getCriteria());
        assertNotNull(info.getOtherProperties());
        assertNotNull(info.getParameters());
        assertNotNull(info.getRequestedColumns());
        assertNotNull(info.getTableXPath());
    }

    public void testGetTableXPath() {
        assertEquals("/Mydata/company", m_info.getTableXPath());
    }

    public void testGetRequestedColumns() {
        List columns = m_info.getRequestedColumns();
        assertNotNull("requestedColumns list is null", columns);
        assertEquals(1, columns.size());
    }

    public void testGetColumnCount() {
        assertEquals(1, m_info.getColumnCount());
    }

    public void testGetParameters() {
        List params = m_info.getParameters();
        assertNotNull("Param list is null", params);
        assertEquals(0, params.size());
    }

    public void testGetCriteria() {
    	String vdbPath = ProxyObjectFactory.getStateCollegeVDBLocation();
    	//String query = "Select AttributeColumn from TestTable where AttributeColumn in ('foo')";
        Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
        try {
            QueryAnalyzer analyzer = new QueryAnalyzer(iquery);
        	analyzer.analyze();
        	ExecutionInfo info = analyzer.getExecutionInfo();
        	List crits = info.getCriteria();
            assertNotNull("Criteria list is null", crits);
            assertEquals(1, crits.size());
        } catch (TranslatorException ce) {
        	ce.printStackTrace();
        	fail(ce.getMessage());
        }
    }

    public void testGetOtherProperties() throws TranslatorException {
        String vdbPath = ProxyObjectFactory.getDocumentsFolder() + "/UnitTests.vdb";
        String strQuery = "select * from Response";
        Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, strQuery);
        QueryAnalyzer analyzer = new QueryAnalyzer(query);
		analyzer.analyze();
        ExecutionInfo info = analyzer.getExecutionInfo();
        Map<String, String> props = info.getOtherProperties();
        assertFalse("properties are empty", props.isEmpty());
        info.setOtherProperties(null);
        assertNotNull(info.getOtherProperties());
    }

    public void testSetTableXPath() {
        String xpath = "/new/path";
        m_info.setTableXPath(xpath);
        assertEquals(xpath, m_info.getTableXPath());
        m_info.setTableXPath("");
        assertNull(m_info.getTableXPath());
    }

    public void testSetRequestedColumns() {
        String reqCol = "Company_id";
        ArrayList reqCols = new ArrayList();
        reqCols.add(reqCol);
        m_info.setRequestedColumns(reqCols);
        assertEquals(reqCol, m_info.getRequestedColumns().get(0));
    }

    public void testSetColumnCount() {
        final int count = 3;
        m_info.setColumnCount(count);
        assertEquals(count, m_info.getColumnCount());

    }

    public void testSetParameters() {
        String param = "[Company_id]";
        ArrayList params = new ArrayList();
        params.add(param);
        m_info.setParameters(params);
        assertEquals(param, m_info.getParameters().get(0));
    }

    public void testSetCriteria() {
        String crit = "Company_id";
        ArrayList crits = new ArrayList();
        crits.add(crit);
        m_info.setParameters(crits);
        assertEquals(crit, m_info.getParameters().get(0));
    }

    public void testSetOtherProperties() {
        String prop = "myProp";
        String key = "foo";
        Map<String, String> props = new HashMap<String, String>();
        props.put(key, prop);
        m_info.setOtherProperties(props);
        assertEquals(prop, m_info.getOtherProperties().get(key));
        
        m_info.setOtherProperties(null);
        assertNotNull("OtherProerties was set to null", m_info.getOtherProperties());
        assertEquals(0, m_info.getOtherProperties().size());
    }

}
