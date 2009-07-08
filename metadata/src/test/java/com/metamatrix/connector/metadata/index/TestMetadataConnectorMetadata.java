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

package com.metamatrix.connector.metadata.index;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.teiid.connector.metadata.FileRecordImpl;
import org.teiid.connector.metadata.MetadataConnectorMetadata;
import org.teiid.connector.metadata.MetadataLiteralCriteria;
import org.teiid.connector.metadata.RuntimeVdbRecord;
import org.teiid.connector.metadata.VdbMetadataContext;
import org.teiid.connector.metadata.runtime.AbstractMetadataRecord;
import org.teiid.connector.metadata.runtime.ModelRecordImpl;
import org.teiid.metadata.CompositeMetadataStore;
import org.teiid.metadata.index.IndexConstants;
import org.teiid.metadata.index.IndexMetadataStore;

import junit.framework.TestCase;

import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.metadata.runtime.api.MetadataSource;


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
    //                      H E L P E R   M E T H O D S
    // =========================================================================

    public MetadataConnectorMetadata helpGetMetadata(String vdbFilePath, String vdbName, String vdbVersion, VDBService service) throws Exception {
    	MetadataSource source = new VDBArchive(new File(vdbFilePath));
        IndexMetadataStore composite = new IndexMetadataStore(source);
        VdbMetadataContext context = new VdbMetadataContext();
        context.setVdbName(vdbName);
        context.setVdbVersion(vdbVersion);
        context.setVdbService(service);
        return new MetadataConnectorMetadata(context, new CompositeMetadataStore(Arrays.asList(composite), source));
    }
    
    public VDBService helpGetVdbService() {
        return new FakeVDBService();
    }
    
    public void testGetFileRecords() throws Exception {
        String entityPath = "/parts/partsmd/PartsSupplier.xmi"; //$NON-NLS-1$
        FakeVDBService service = (FakeVDBService) helpGetVdbService();
        service.publicFiles.add(entityPath);
        MetadataLiteralCriteria literalcriteria = new MetadataLiteralCriteria(FileRecordImpl.MetadataMethodNames.PATH_IN_VDB_FIELD, entityPath); 
        Map criteria = new HashMap();
        criteria.put(FileRecordImpl.MetadataMethodNames.PATH_IN_VDB_FIELD.toUpperCase(), literalcriteria);
        MetadataConnectorMetadata metadata = helpGetMetadata(TEST_FILE_NAME, TEST_VDB_NAME, TEST_VDB_VERSION, service);
        Collection records = metadata.getObjects(IndexConstants.INDEX_NAME.FILES_INDEX, criteria);
        assertNotNull(records);
        assertEquals(1, records.size());
    }
    
    public void testGetPublicModelRecords() throws Exception {
        String modelName = "PartsSupplier"; //$NON-NLS-1$
        FakeVDBService service = (FakeVDBService) helpGetVdbService();
        service.publicModels.add(modelName);
        MetadataLiteralCriteria literalcriteria = new MetadataLiteralCriteria(AbstractMetadataRecord.MetadataFieldNames.FULL_NAME_FIELD, modelName); 
        Map criteria = new HashMap();
        criteria.put(AbstractMetadataRecord.MetadataFieldNames.FULL_NAME_FIELD.toUpperCase(), literalcriteria);
        MetadataConnectorMetadata metadata = helpGetMetadata(TEST_FILE_NAME, TEST_VDB_NAME, TEST_VDB_VERSION, service);
        Collection records = metadata.getObjects(IndexConstants.INDEX_NAME.MODELS_INDEX, criteria);
        assertNotNull(records);
        assertEquals(1, records.size());
        ModelRecordImpl modelRecord = (ModelRecordImpl) records.iterator().next();
        assertTrue(modelRecord.isVisible());
    }
    
    public void testGetPrivateModelRecords() throws Exception {
        String modelName = "PartsSupplier"; //$NON-NLS-1$
        FakeVDBService service = (FakeVDBService) helpGetVdbService();
        //service.publicModels.add(modelName);
        MetadataLiteralCriteria literalcriteria = new MetadataLiteralCriteria(AbstractMetadataRecord.MetadataFieldNames.FULL_NAME_FIELD, modelName); 
        Map criteria = new HashMap();
        criteria.put(AbstractMetadataRecord.MetadataFieldNames.FULL_NAME_FIELD.toUpperCase(), literalcriteria);
        MetadataConnectorMetadata metadata = helpGetMetadata(TEST_FILE_NAME, TEST_VDB_NAME, TEST_VDB_VERSION, service);
        Collection records = metadata.getObjects(IndexConstants.INDEX_NAME.MODELS_INDEX, criteria);
        assertNotNull(records);
        assertEquals(1, records.size());
        ModelRecordImpl modelRecord = (ModelRecordImpl) records.iterator().next();
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
        MetadataLiteralCriteria literalcriteria = new MetadataLiteralCriteria(AbstractMetadataRecord.MetadataFieldNames.FULL_NAME_FIELD, modelName);
        literalcriteria.setFieldFunction("UPPER"); //$NON-NLS-1$
        Map criteria = new HashMap();
        criteria.put(AbstractMetadataRecord.MetadataFieldNames.FULL_NAME_FIELD.toUpperCase(), literalcriteria);
        MetadataConnectorMetadata metadata = helpGetMetadata(TEST_FILE_NAME, TEST_VDB_NAME, TEST_VDB_VERSION, service);
        Collection records = metadata.getObjects(IndexConstants.INDEX_NAME.MODELS_INDEX, criteria);
        assertNotNull(records);
        assertEquals(0, records.size());
    }    
}