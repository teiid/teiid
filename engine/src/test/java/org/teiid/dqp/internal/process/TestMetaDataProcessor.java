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

package org.teiid.dqp.internal.process;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

import org.mockito.Mockito;

import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.dqp.client.MetadataResult;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.dqp.metadata.ResultsMetadataConstants;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.FakeVDBService;
import com.metamatrix.dqp.service.MetadataService;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.unittest.FakeMetadataFactory;

/**
 */
public class TestMetaDataProcessor extends TestCase {

    public TestMetaDataProcessor(String name) {
        super(name);
    }
    
    public Map[] helpGetMetadata(String sql, QueryMetadataInterface metadata) throws Exception {
        // Prepare sql 
        Command command = QueryParser.getQueryParser().parseCommand(sql);
        QueryResolver.resolveCommand(command, Collections.EMPTY_MAP, false, metadata, AnalysisRecord.createNonRecordingRecord());
        
        // Create components
        MetadataService mdSvc = Mockito.mock(MetadataService.class);
        Mockito.stub(mdSvc.lookupMetadata(Mockito.anyString(), Mockito.anyString())).toReturn(metadata);
        PreparedPlanCache prepPlanCache = new PreparedPlanCache();
        DQPCore requestMgr = new DQPCore();

        DQPWorkContext workContext = new DQPWorkContext();
        workContext.setVdbName("MyVDB"); //$NON-NLS-1$
        workContext.setVdbVersion("1"); //$NON-NLS-1$
        workContext.setSessionToken(new SessionToken(new MetaMatrixSessionID(1), "foo")); //$NON-NLS-1$

        // Initialize components
        RequestID requestID = workContext.getRequestID(1);  
        RequestMessage requestMsg = new RequestMessage(sql);
        TestDQPCoreRequestHandling.addRequest(requestMgr, requestMsg, requestID, command, null); 
        
        ApplicationEnvironment env = new ApplicationEnvironment();
        FakeVDBService vdbService = new FakeVDBService();
        env.bindService(DQPServiceNames.VDB_SERVICE, vdbService);
        MetaDataProcessor mdProc = new MetaDataProcessor(mdSvc, requestMgr, prepPlanCache, env, null);
                     
        return mdProc.processMessage(requestID, workContext, null, true).getColumnMetadata();    
    }
    
    public void testSimpleQuery() throws Exception {
        Map[] metadata = helpGetMetadata("SELECT e1 FROM pm1.g1", FakeMetadataFactory.example1Cached()); //$NON-NLS-1$
        assertNotNull(metadata);
        assertEquals(1, metadata.length);
    }

    public void testSimpleUpdate() throws Exception {
        Map[] metadata = helpGetMetadata("INSERT INTO pm1.g1 (e1) VALUES ('x')", FakeMetadataFactory.example1Cached()); //$NON-NLS-1$
        assertNull(metadata);
        
        metadata = helpGetMetadata("DELETE FROM pm1.g1 WHERE e1 = 'x'", FakeMetadataFactory.example1Cached()); //$NON-NLS-1$
        assertNull(metadata);
        
        metadata = helpGetMetadata("UPDATE pm1.g1 SET e1='y' WHERE e1 = 'x'", FakeMetadataFactory.example1Cached()); //$NON-NLS-1$
        assertNull(metadata);
        
        metadata = helpGetMetadata("SELECT e1, e2, e3, e4 INTO pm1.g2 FROM pm1.g1", FakeMetadataFactory.example1Cached()); //$NON-NLS-1$
        assertNull(metadata);
    }
    
    public void testElementLabel() throws Exception {
    	Map[] metadata = helpGetMetadata("SELECT E2 FROM pm1.g1", FakeMetadataFactory.example1Cached()); //$NON-NLS-1$
        assertNotNull(metadata);
        assertEquals(1, metadata.length);
        assertEquals("E2", metadata[0].get(ResultsMetadataConstants.ELEMENT_NAME)); //$NON-NLS-1$
    }
    
    public void testSimpleExec() throws Exception {
        Map[] metadata = helpGetMetadata("EXEC pm1.sq1()", FakeMetadataFactory.example1Cached()); //$NON-NLS-1$
        assertNotNull(metadata);
        assertEquals(2, metadata.length);        
    }
    
    public void testExecNoResultColumns() throws Exception {
        Map[] metadata = helpGetMetadata("EXEC pm1.sp5()", FakeMetadataFactory.example1Cached()); //$NON-NLS-1$
        assertNotNull(metadata);
        assertEquals(0, metadata.length);                
    }
    
    public void testDefect15029() throws Exception {
        String sql = "SELECT * FROM g1"; //$NON-NLS-1$
        QueryMetadataInterface metadata = FakeMetadataFactory.examplePrivatePhysicalModel();
        
        MetadataResult response = helpTestQuery(metadata, sql);

        Map[] md = response.getColumnMetadata();    
        assertNotNull(md);
        assertEquals(1, md.length);
        assertEquals("e1", md[0].get(ResultsMetadataConstants.ELEMENT_NAME)); //$NON-NLS-1$
        assertEquals("vm1.g1", md[0].get(ResultsMetadataConstants.GROUP_NAME)); //$NON-NLS-1$
    }
    
    private MetadataResult helpTestQuery(QueryMetadataInterface metadata, String sql) throws Exception {
        FakeVDBService vdbService = new FakeVDBService();
        vdbService.addModel("MyVDB", "1", "pm1", ModelInfo.PRIVATE, false);  //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
        
        // Create components
        MetadataService mdSvc = Mockito.mock(MetadataService.class);
        Mockito.stub(mdSvc.lookupMetadata(Mockito.anyString(), Mockito.anyString())).toReturn(metadata);
        PreparedPlanCache prepPlanCache = new PreparedPlanCache();
        
        // Initialize components
        ApplicationEnvironment env = new ApplicationEnvironment();
        env.bindService(DQPServiceNames.VDB_SERVICE, vdbService);
        MetaDataProcessor mdProc = new MetaDataProcessor(mdSvc, new DQPCore(), prepPlanCache, env, null);
                     
        DQPWorkContext workContext = new DQPWorkContext();
        workContext.setVdbName("MyVDB"); //$NON-NLS-1$
        workContext.setVdbVersion("1"); //$NON-NLS-1$
        workContext.setSessionToken(new SessionToken(new MetaMatrixSessionID(1), "foo")); //$NON-NLS-1$
        return mdProc.processMessage(workContext.getRequestID(1), workContext, sql, true);    
    }

    private void helpCheckNumericAttributes(MetadataResult message, int column, int expectedSize, int expectedPrecision, int expectedScale) {
        Map[] md = message.getColumnMetadata();    
        assertNotNull(md);
        assertEquals(new Integer(expectedSize), md[column].get(ResultsMetadataConstants.DISPLAY_SIZE)); 
        assertEquals(new Integer(expectedPrecision), md[column].get(ResultsMetadataConstants.PRECISION)); 
        assertEquals(new Integer(expectedScale), md[column].get(ResultsMetadataConstants.SCALE)); 
    }

    public void testDefect16629_moneyType() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.examplePrivatePhysicalModel(); 
        String sql = "SELECT e1 FROM pm1.g2"; //$NON-NLS-1$
        
        MetadataResult response = helpTestQuery(metadata, sql);
        
        helpCheckNumericAttributes(response, 0, 21, 19, 4);
    }

    public void testDefect16629_aggregatesOnMoneyType() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.examplePrivatePhysicalModel(); 
        String sql = "SELECT min(e1), max(e1), sum(e1), avg(e1) FROM pm1.g2"; //$NON-NLS-1$
        
        MetadataResult response = helpTestQuery(metadata, sql);
        helpCheckNumericAttributes(response, 0, 21, 19, 4);
        helpCheckNumericAttributes(response, 1, 21, 19, 4);
        helpCheckNumericAttributes(response, 2, 22, 20, 0);
        helpCheckNumericAttributes(response, 3, 22, 20, 0);
    }
    
    public void testMetadataGenerationForAllTypes() throws Exception {
        Set dataTypes = DataTypeManager.getAllDataTypeNames();
        Iterator iter = dataTypes.iterator();
        
        while(iter.hasNext()) {
            String type = (String) iter.next();
            Class typeClass = DataTypeManager.getDataTypeClass(type);
            MetaDataProcessor processor = new MetaDataProcessor(null, null, null, null, null);
            Map columnMetadata = processor.getDefaultColumn("vdb", "1", "t", "c", typeClass); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            verifyColumn(columnMetadata, type);            
        }               
    }
    
    private void verifyColumn(Map column, String dataType) {
        verifyAttribute(column, ResultsMetadataConstants.AUTO_INCREMENTING, false, Boolean.class, dataType);        
        verifyAttribute(column, ResultsMetadataConstants.CASE_SENSITIVE, false, Boolean.class, dataType);
        verifyAttribute(column, ResultsMetadataConstants.CURRENCY, false, Boolean.class, dataType);
        
        Object dt = verifyAttribute(column, ResultsMetadataConstants.DATA_TYPE, false, String.class, dataType);
        assertEquals(dataType, dt);
        
        verifyAttribute(column, ResultsMetadataConstants.DISPLAY_SIZE, false, Integer.class, dataType);
        verifyAttribute(column, ResultsMetadataConstants.ELEMENT_LABEL, true, String.class, dataType);
        verifyAttribute(column, ResultsMetadataConstants.ELEMENT_NAME, false, String.class, dataType);
        verifyAttribute(column, ResultsMetadataConstants.GROUP_NAME, true, String.class, dataType);

        Object nullable = verifyAttribute(column, ResultsMetadataConstants.NULLABLE, false, Integer.class, dataType);
        verifyNullable((Integer)nullable);

        verifyAttribute(column, ResultsMetadataConstants.PRECISION, false, Integer.class, dataType);
        verifyAttribute(column, ResultsMetadataConstants.RADIX, false, Integer.class, dataType);
        verifyAttribute(column, ResultsMetadataConstants.SCALE, false, Integer.class, dataType);
        
        Object searchable = verifyAttribute(column, ResultsMetadataConstants.SEARCHABLE, false, Integer.class, dataType);
        verifySearchable((Integer)searchable);
        
        verifyAttribute(column, ResultsMetadataConstants.SIGNED, false, Boolean.class, dataType);

        verifyAttribute(column, ResultsMetadataConstants.VIRTUAL_DATABASE_NAME, false, String.class, dataType);
        verifyAttribute(column, ResultsMetadataConstants.VIRTUAL_DATABASE_VERSION, false, String.class, dataType);
        verifyAttribute(column, ResultsMetadataConstants.WRITABLE, false, Boolean.class, dataType);
    }
    
    private Object verifyAttribute(Map column, Integer attributeType, boolean nullsAllowed, Class expectedClass, String columnDataType) {
        //System.out.println("Checking " + columnDataType + ", attribute " + attributeType); //$NON-NLS-1$ //$NON-NLS-2$
        Object value = column.get(attributeType);
        if(! nullsAllowed) {
            assertNotNull("Got null when not allowed for column of type " + columnDataType, value); //$NON-NLS-1$
        }
        
        if(value != null) {
            assertEquals("Got attribute of incorrect class for column of type " + columnDataType, expectedClass, value.getClass()); //$NON-NLS-1$
        }
        
        return value;   
    }
    
    private void verifyNullable(Integer nullable) {
        if(! 
            (nullable.equals(ResultsMetadataConstants.NULL_TYPES.NOT_NULL) ||
             nullable.equals(ResultsMetadataConstants.NULL_TYPES.NULLABLE) || 
             nullable.equals(ResultsMetadataConstants.NULL_TYPES.UNKNOWN) ) ) {
        
            fail("Invalid nullable constant value: " + nullable);          //$NON-NLS-1$
        }
    }

    private void verifySearchable(Integer searchable) {
        if(! 
            (searchable.equals(ResultsMetadataConstants.SEARCH_TYPES.ALLEXCEPTLIKE) ||
            searchable.equals(ResultsMetadataConstants.SEARCH_TYPES.LIKE_ONLY) || 
            searchable.equals(ResultsMetadataConstants.SEARCH_TYPES.UNSEARCHABLE) || 
            searchable.equals(ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE) ) ) {
        
            fail("Invalid searchable constant value: " + searchable);          //$NON-NLS-1$
        }
    }
}
