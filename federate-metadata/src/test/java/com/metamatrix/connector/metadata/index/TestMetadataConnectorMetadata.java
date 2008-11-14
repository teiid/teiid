/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.connector.metadata.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.metamatrix.connector.metadata.RuntimeVdbRecord;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.modeler.core.index.IndexConstants;
import com.metamatrix.modeler.core.index.IndexSelector;
import com.metamatrix.modeler.core.metadata.runtime.FileRecord;
import com.metamatrix.modeler.core.metadata.runtime.MetadataRecord;
import com.metamatrix.modeler.core.metadata.runtime.ModelRecord;
import com.metamatrix.modeler.internal.core.index.CompositeIndexSelector;
import com.metamatrix.modeler.internal.core.index.RuntimeIndexSelector;


/** 
 * @since 4.3
 */
public class TestMetadataConnectorMetadata extends TestCase {

    public static final String TEST_FILE_NAME = UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb"; //$NON-NLS-1$
    private static final String TEST_VDB_NAME = "Parts"; //$NON-NLS-1$
    private static final String TEST_VDB_VERSION = "1"; //$NON-NLS-1$

    /**
     * Constructor for TestServerRuntimeMetadata.
     * @param name
     */
    public TestMetadataConnectorMetadata(String name) {
        super(name);
    }

    // =========================================================================
    //                        T E S T   C O N T R O L
    // =========================================================================

    /** 
     * Construct the test suite, which uses a one-time setup call
     * and a one-time tear-down call.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("TestMetadataConnectorMetadata"); //$NON-NLS-1$
        suite.addTestSuite(TestMetadataConnectorMetadata.class);
        //suite.addTest(new TestMetadataConnectorMetadata("testGetCharacterVDBResource")); //$NON-NLS-1$
        //suite.addTest(new TestMetadataConnectorMetadata("testGetElementIDsInKey13760")); //$NON-NLS-1$

        return new TestSetup(suite);
    }

    // =========================================================================
    //                      H E L P E R   M E T H O D S
    // =========================================================================

    public MetadataConnectorMetadata helpGetMetadata(String vdbFilePath, String vdbName, String vdbVersion, VDBService service) throws Exception {
        List selectors = new ArrayList();
        selectors.add(new RuntimeIndexSelector(vdbFilePath));        
        IndexSelector composite = new CompositeIndexSelector(selectors);
        VdbMetadataContext context = new VdbMetadataContext(composite);
        context.setVdbName(vdbName);
        context.setVdbVersion(vdbVersion);
        context.setVdbService(service);
        return new MetadataConnectorMetadata(context);
    }
    
    public VDBService helpGetVdbService() {
        return new FakeVDBService();
    }
    
    public void testGetFileRecords() throws Exception {
        String entityPath = "/parts/partsmd/PartsSupplier.xmi"; //$NON-NLS-1$
        FakeVDBService service = (FakeVDBService) helpGetVdbService();
        service.publicFiles.add(entityPath);
        MetadataLiteralCriteria literalcriteria = new MetadataLiteralCriteria(FileRecord.MetadataMethodNames.PATH_IN_VDB_FIELD, entityPath); 
        Map criteria = new HashMap();
        criteria.put(FileRecord.MetadataMethodNames.PATH_IN_VDB_FIELD.toUpperCase(), literalcriteria);
        MetadataConnectorMetadata metadata = helpGetMetadata(TEST_FILE_NAME, TEST_VDB_NAME, TEST_VDB_VERSION, service);
        Collection records = metadata.getObjects(IndexConstants.INDEX_NAME.FILES_INDEX, criteria);
        assertNotNull(records);
        assertEquals(1, records.size());
    }
    
    public void testGetPublicModelRecords() throws Exception {
        String modelName = "PartsSupplier"; //$NON-NLS-1$
        FakeVDBService service = (FakeVDBService) helpGetVdbService();
        service.publicModels.add(modelName);
        MetadataLiteralCriteria literalcriteria = new MetadataLiteralCriteria(MetadataRecord.MetadataFieldNames.FULL_NAME_FIELD, modelName); 
        Map criteria = new HashMap();
        criteria.put(MetadataRecord.MetadataFieldNames.FULL_NAME_FIELD.toUpperCase(), literalcriteria);
        MetadataConnectorMetadata metadata = helpGetMetadata(TEST_FILE_NAME, TEST_VDB_NAME, TEST_VDB_VERSION, service);
        Collection records = metadata.getObjects(IndexConstants.INDEX_NAME.MODELS_INDEX, criteria);
        assertNotNull(records);
        assertEquals(1, records.size());
        ModelRecord modelRecord = (ModelRecord) records.iterator().next();
        assertTrue(modelRecord.isVisible());
    }
    
    public void testGetPrivateModelRecords() throws Exception {
        String modelName = "PartsSupplier"; //$NON-NLS-1$
        FakeVDBService service = (FakeVDBService) helpGetVdbService();
        //service.publicModels.add(modelName);
        MetadataLiteralCriteria literalcriteria = new MetadataLiteralCriteria(MetadataRecord.MetadataFieldNames.FULL_NAME_FIELD, modelName); 
        Map criteria = new HashMap();
        criteria.put(MetadataRecord.MetadataFieldNames.FULL_NAME_FIELD.toUpperCase(), literalcriteria);
        MetadataConnectorMetadata metadata = helpGetMetadata(TEST_FILE_NAME, TEST_VDB_NAME, TEST_VDB_VERSION, service);
        Collection records = metadata.getObjects(IndexConstants.INDEX_NAME.MODELS_INDEX, criteria);
        assertNotNull(records);
        assertEquals(1, records.size());
        ModelRecord modelRecord = (ModelRecord) records.iterator().next();
        assertTrue(!modelRecord.isVisible());
    }

    public void testGetWrappedVdbRecords() throws Exception {
        FakeVDBService service = (FakeVDBService) helpGetVdbService();
        Map criteria = new HashMap();
        MetadataConnectorMetadata metadata = helpGetMetadata(TEST_FILE_NAME, TEST_VDB_NAME, TEST_VDB_VERSION, service);
        Collection records = metadata.getObjects(IndexConstants.INDEX_NAME.VDBS_INDEX, criteria);
        assertNotNull(records);
        assertEquals(1, records.size());
        RuntimeVdbRecord vdbRecord = (RuntimeVdbRecord) records.iterator().next();
        assertEquals(TEST_VDB_NAME, vdbRecord.getVdbRuntimeName());        
    }
    
    public void testGetRecordsWithFalseCriteria() throws Exception {
        String modelName = "PartsSupplier"; //$NON-NLS-1$
        FakeVDBService service = (FakeVDBService) helpGetVdbService();
        service.publicModels.add(modelName);
        MetadataLiteralCriteria literalcriteria = new MetadataLiteralCriteria(MetadataRecord.MetadataFieldNames.FULL_NAME_FIELD, modelName);
        literalcriteria.setFieldFunction("UPPER"); //$NON-NLS-1$
        Map criteria = new HashMap();
        criteria.put(MetadataRecord.MetadataFieldNames.FULL_NAME_FIELD.toUpperCase(), literalcriteria);
        MetadataConnectorMetadata metadata = helpGetMetadata(TEST_FILE_NAME, TEST_VDB_NAME, TEST_VDB_VERSION, service);
        Collection records = metadata.getObjects(IndexConstants.INDEX_NAME.MODELS_INDEX, criteria);
        assertNotNull(records);
        assertEquals(0, records.size());
    }    
}