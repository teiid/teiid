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

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.metadata.runtime.impl.ColumnRecordImpl;
import com.metamatrix.metadata.runtime.impl.ModelRecordImpl;
import com.metamatrix.modeler.core.metadata.runtime.MetadataRecord;


/** 
 * TestMetadataResultsPostProcessor
 * @since 4.3
 */
public class TestMetadataResultsPostProcessor extends TestCase {

    /**
     * Constructor for TestMetadataResultsPostProcessor.
     * @param name
     */
    public TestMetadataResultsPostProcessor(String name) {
        super(name);
    }

    public MetadataResultsPostProcessor helpGetProcessor() {
        VdbMetadataContext context = new VdbMetadataContext();
        context.setVdbName("testvdb"); //$NON-NLS-1$
        context.setVdbVersion("1"); //$NON-NLS-1$
        return new MetadataResultsPostProcessor(context);
    }
    
    public VDBService helpGetVdbService() {
        return new FakeVDBService();
    }
    
    public void testFilterCaseMisMatch() {
        String modelName = "PartsSupplier"; //$NON-NLS-1$
        
        MetadataLiteralCriteria literalcriteria = new MetadataLiteralCriteria(MetadataRecord.MetadataFieldNames.FULL_NAME_FIELD, modelName); 
        Map criteria = new HashMap();
        criteria.put(MetadataRecord.MetadataFieldNames.FULL_NAME_FIELD.toUpperCase(), literalcriteria);
        
        ModelRecordImpl modelRecord = new ModelRecordImpl();
        modelRecord.setFullName(modelName.toUpperCase());

        MetadataResultsPostProcessor processor = helpGetProcessor();
        Object filteredRecord = processor.filterBySearchCriteria(modelRecord, criteria);

        assertNull(filteredRecord);
    }
    
    public void testFilterWildCardMatch() {
        String modelName = "PartsSupplier"; //$NON-NLS-1$
        
        MetadataLiteralCriteria literalcriteria = new MetadataLiteralCriteria(MetadataRecord.MetadataFieldNames.FULL_NAME_FIELD, "*Supplie?"); //$NON-NLS-1$
        Map criteria = new HashMap();
        criteria.put(MetadataRecord.MetadataFieldNames.FULL_NAME_FIELD.toUpperCase(), literalcriteria);
        
        ModelRecordImpl modelRecord = new ModelRecordImpl();
        modelRecord.setFullName(modelName);
        
        MetadataResultsPostProcessor processor = helpGetProcessor();
        Object filteredRecord = processor.filterBySearchCriteria(modelRecord, criteria);

        assertNotNull(filteredRecord);
    }    
    
    public void testFilterNullMatch() {
        String uuid = null;
        
        MetadataLiteralCriteria literalcriteria = new MetadataLiteralCriteria(MetadataRecord.MetadataFieldNames.UUID_FIELD, uuid); 
        Map criteria = new HashMap();
        criteria.put(MetadataRecord.MetadataFieldNames.UUID_FIELD.toUpperCase(), literalcriteria);
        
        ColumnRecordImpl columnRecord = new ColumnRecordImpl();
        columnRecord.setFullName("testname.name"); //$NON-NLS-1$
        columnRecord.setUUID(null);
        
        MetadataResultsPostProcessor processor = helpGetProcessor();
        Object filteredRecord = processor.filterBySearchCriteria(columnRecord, criteria);

        assertNotNull(filteredRecord);
    }
    
    public void testFilterNullMisMatch() {
        String uuid = null;
        
        MetadataLiteralCriteria literalcriteria = new MetadataLiteralCriteria(MetadataRecord.MetadataFieldNames.UUID_FIELD, uuid); 
        Map criteria = new HashMap();
        criteria.put(MetadataRecord.MetadataFieldNames.UUID_FIELD.toUpperCase(), literalcriteria);
        
        ColumnRecordImpl columnRecord = new ColumnRecordImpl();
        columnRecord.setFullName("testname.name"); //$NON-NLS-1$
        columnRecord.setUUID("uuid"); //$NON-NLS-1$
        
        MetadataResultsPostProcessor processor = helpGetProcessor();
        Object filteredRecord = processor.filterBySearchCriteria(columnRecord, criteria);

        assertNull(filteredRecord);
    }    
}
