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

import static org.junit.Assert.*;

import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.client.RequestMessage;
import org.teiid.client.metadata.MetadataResult;
import org.teiid.client.metadata.ResultsMetadataConstants;
import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.internal.datamgr.FakeTransactionService;
import org.teiid.dqp.message.RequestID;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.FakeMetadataFactory;


/**
 */
@SuppressWarnings({"nls", "unchecked"})
public class TestMetaDataProcessor {

	public Map[] helpGetMetadata(String sql, QueryMetadataInterface metadata, VDBMetaData vdb) throws Exception {
        // Prepare sql 
        Command command = QueryParser.getQueryParser().parseCommand(sql);
        QueryResolver.resolveCommand(command, metadata);
        
        // Create components
        SessionAwareCache<PreparedPlan> prepPlanCache = new SessionAwareCache<PreparedPlan>();
        DQPCore requestMgr = new DQPCore();
        requestMgr.setTransactionService(new FakeTransactionService());

        DQPWorkContext workContext = FakeMetadataFactory.buildWorkContext(metadata, vdb);

        // Initialize components
        RequestID requestID = workContext.getRequestID(1);  
        RequestMessage requestMsg = new RequestMessage(sql);
        TestDQPCoreRequestHandling.addRequest(requestMgr, requestMsg, requestID, command, null); 
        
        MetaDataProcessor mdProc = new MetaDataProcessor(requestMgr, prepPlanCache, "MyVDB", 1);
                     
        return mdProc.processMessage(requestID, workContext, null, true).getColumnMetadata();    
    }
    
    @Test public void testSimpleQuery() throws Exception {
        Map[] metadata = helpGetMetadata("SELECT e1 FROM pm1.g1", FakeMetadataFactory.example1Cached(), FakeMetadataFactory.example1VDB()); //$NON-NLS-1$
        assertNotNull(metadata);
        assertEquals(1, metadata.length);
    }

    @Test public void testSimpleUpdate() throws Exception {
        Map[] metadata = helpGetMetadata("INSERT INTO pm1.g1 (e1) VALUES ('x')", FakeMetadataFactory.example1Cached(), FakeMetadataFactory.example1VDB()); //$NON-NLS-1$
        assertNull(metadata);
        
        metadata = helpGetMetadata("DELETE FROM pm1.g1 WHERE e1 = 'x'", FakeMetadataFactory.example1Cached(), FakeMetadataFactory.example1VDB()); //$NON-NLS-1$
        assertNull(metadata);
        
        metadata = helpGetMetadata("UPDATE pm1.g1 SET e1='y' WHERE e1 = 'x'", FakeMetadataFactory.example1Cached(), FakeMetadataFactory.example1VDB()); //$NON-NLS-1$
        assertNull(metadata);
        
        metadata = helpGetMetadata("SELECT e1, e2, e3, e4 INTO pm1.g2 FROM pm1.g1", FakeMetadataFactory.example1Cached(),FakeMetadataFactory.example1VDB()); //$NON-NLS-1$
        assertNull(metadata);
    }
    
    @Test public void testElementLabel() throws Exception {
    	Map[] metadata = helpGetMetadata("SELECT E2 FROM pm1.g1", FakeMetadataFactory.example1Cached(), FakeMetadataFactory.example1VDB()); //$NON-NLS-1$
        assertNotNull(metadata);
        assertEquals(1, metadata.length);
        assertEquals("e2", metadata[0].get(ResultsMetadataConstants.ELEMENT_NAME)); //$NON-NLS-1$
        assertEquals("E2", metadata[0].get(ResultsMetadataConstants.ELEMENT_LABEL)); //$NON-NLS-1$
    }
    
    @Test public void testSimpleExec() throws Exception {
        Map[] metadata = helpGetMetadata("EXEC pm1.sq1()", FakeMetadataFactory.example1Cached(), FakeMetadataFactory.example1VDB()); //$NON-NLS-1$
        assertNotNull(metadata);
        assertEquals(2, metadata.length);        
    }
    
    @Test public void testExecNoResultColumns() throws Exception {
        Map[] metadata = helpGetMetadata("EXEC pm1.sp5()", FakeMetadataFactory.example1Cached(), FakeMetadataFactory.example1VDB()); //$NON-NLS-1$
        assertNotNull(metadata);
        assertEquals(0, metadata.length);                
    }
    
    private MetadataResult helpTestQuery(QueryMetadataInterface metadata, String sql, VDBMetaData vdb) throws Exception {
        // Create components
        SessionAwareCache<PreparedPlan> prepPlanCache = new SessionAwareCache<PreparedPlan>();
        
        // Initialize components
        MetaDataProcessor mdProc = new MetaDataProcessor(new DQPCore(), prepPlanCache, "MyVDB", 1);
                     
        DQPWorkContext workContext = FakeMetadataFactory.buildWorkContext(metadata, vdb);
        return mdProc.processMessage(workContext.getRequestID(1), workContext, sql, true);    
    }

    private void helpCheckNumericAttributes(MetadataResult message, int column, int expectedSize, int expectedPrecision, int expectedScale) {
        Map[] md = message.getColumnMetadata();    
        assertNotNull(md);
        assertEquals(new Integer(expectedSize), md[column].get(ResultsMetadataConstants.DISPLAY_SIZE)); 
        assertEquals(new Integer(expectedPrecision), md[column].get(ResultsMetadataConstants.PRECISION)); 
        assertEquals(new Integer(expectedScale), md[column].get(ResultsMetadataConstants.SCALE)); 
    }

    @Test public void testDefect16629_moneyType() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.examplePrivatePhysicalModel(); 
        String sql = "SELECT e1 FROM pm1.g2"; //$NON-NLS-1$
        
        MetadataResult response = helpTestQuery(metadata, sql, FakeMetadataFactory.examplePrivatePhysicalModelVDB());
        
        helpCheckNumericAttributes(response, 0, 21, 19, 4);
    }

    @Test public void testDefect16629_aggregatesOnMoneyType() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.examplePrivatePhysicalModel(); 
        String sql = "SELECT min(e1), max(e1), sum(e1), avg(e1) FROM pm1.g2"; //$NON-NLS-1$
        
        MetadataResult response = helpTestQuery(metadata, sql, FakeMetadataFactory.examplePrivatePhysicalModelVDB());
        helpCheckNumericAttributes(response, 0, 21, 19, 4);
        helpCheckNumericAttributes(response, 1, 21, 19, 4);
        helpCheckNumericAttributes(response, 2, 22, 20, 0);
        helpCheckNumericAttributes(response, 3, 22, 20, 0);
    }
    
    @Test public void testMetadataGenerationForAllTypes() throws Exception {
        Set<String> dataTypes = DataTypeManager.getAllDataTypeNames();
        for (String type : dataTypes) {
            Class<?> typeClass = DataTypeManager.getDataTypeClass(type);
            MetaDataProcessor processor = new MetaDataProcessor(null, null, "vdb", 1);
            Map<Integer, Object> columnMetadata = processor.getDefaultColumn("t", "c", typeClass); //$NON-NLS-1$ //$NON-NLS-2$
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
        verifyAttribute(column, ResultsMetadataConstants.VIRTUAL_DATABASE_VERSION, false, Integer.class, dataType);
        verifyAttribute(column, ResultsMetadataConstants.WRITABLE, false, Boolean.class, dataType);
    }
    
    private Object verifyAttribute(Map column, Integer attributeType, boolean nullsAllowed, Class<?> expectedClass, String columnDataType) {
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
