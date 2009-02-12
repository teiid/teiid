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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import com.metamatrix.modeler.core.index.IndexConstants;
import com.metamatrix.modeler.core.metadata.runtime.DatatypeRecord;
import com.metamatrix.modeler.core.metadata.runtime.MetadataRecord;
import com.metamatrix.modeler.core.metadata.runtime.PropertyRecord;


/** 
 * @since 4.3
 */
public class TestIndexCriteriaBuilder extends TestCase {

    // =========================================================================
    //                        F R A M E W O R K
    // =========================================================================

    /**
     * Constructor for TestIndexCriteriaBuilder.
     * @param name
     */
    public TestIndexCriteriaBuilder(String name) {
        super(name);
    }

    public void helpAddToCriteria(Map criteria, String fieldName, Object fieldValue) {
        MetadataLiteralCriteria literalCriteria = new MetadataLiteralCriteria(fieldName, fieldValue);        
        criteria.put(fieldName.toUpperCase(), literalCriteria);
    }
    
    public void helpAddToInCriteria(Map criteria, String fieldName, Collection fieldValues) {
        MetadataInCriteria literalCriteria = new MetadataInCriteria(fieldName, fieldValues);        
        criteria.put(fieldName.toUpperCase(), literalCriteria);        
    }
    
    public void testDataTypeCriteriaPrefix1() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, DatatypeRecord.MetadataFieldNames.DATA_TYPE_UUID, "dataTypeUUID"); //$NON-NLS-1$
        String matchPrefix = IndexCriteriaBuilder.getMatchPrefix(IndexConstants.INDEX_NAME.DATATYPES_INDEX, criteria);
        String expectedPrefix = ""+IndexConstants.RECORD_TYPE.DATATYPE+//$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"datatypeuuid"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER;

        assertNotNull(matchPrefix);
        assertEquals(matchPrefix, expectedPrefix);        
    }

    public void testDataTypeCriteriaPrefix2() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, DatatypeRecord.MetadataFieldNames.BASE_TYPE_UUID, "baseTypeUUID"); //$NON-NLS-1$
        String matchPrefix = IndexCriteriaBuilder.getMatchPrefix(IndexConstants.INDEX_NAME.DATATYPES_INDEX, criteria);
        assertNull(matchPrefix);
    }
    
    public void testDataTypeCriteriaPrefix3() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, DatatypeRecord.MetadataFieldNames.DATA_TYPE_UUID, "dataType?UUID"); //$NON-NLS-1$
        String matchPrefix = IndexCriteriaBuilder.getMatchPrefix(IndexConstants.INDEX_NAME.DATATYPES_INDEX, criteria);
        assertNull(matchPrefix);
    }

    public void testDataTypeCriteriaPrefix4() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, DatatypeRecord.MetadataFieldNames.DATA_TYPE_UUID, "dataType*UUID"); //$NON-NLS-1$
        String matchPrefix = IndexCriteriaBuilder.getMatchPrefix(IndexConstants.INDEX_NAME.DATATYPES_INDEX, criteria);
        assertNull(matchPrefix);
    }

    public void testDataTypeCriteria1() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, DatatypeRecord.MetadataFieldNames.DATA_TYPE_UUID, "dataTypeUUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, DatatypeRecord.MetadataFieldNames.BASE_TYPE_UUID, "baseTypeUUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, DatatypeRecord.MetadataFieldNames.RUN_TYPE_NAME, "runtTypeName"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_IN_SOURCE_FIELD, "nameInSource"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, "UUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_FIELD, "Name"); //$NON-NLS-1$
        String matchPattern = IndexCriteriaBuilder.getMatchPattern(IndexConstants.INDEX_NAME.DATATYPES_INDEX, criteria);
        String expectedPattern = ""+IndexConstants.RECORD_TYPE.DATATYPE+//$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"dataTypeUUID"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"baseTypeUUID"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"Name"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"UUID"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"nameInSource"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+IndexConstants.RECORD_STRING.MATCH_CHAR+
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+IndexConstants.RECORD_STRING.MATCH_CHAR+
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"runtTypeName"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+IndexConstants.RECORD_STRING.MATCH_CHAR; 

        assertEquals(matchPattern, expectedPattern);
    }
    
    public void testDataTypeCriteria2() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, DatatypeRecord.MetadataFieldNames.BASE_TYPE_UUID, "baseTypeUUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, DatatypeRecord.MetadataFieldNames.RUN_TYPE_NAME, "runtTypeName"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_IN_SOURCE_FIELD, "nameInSource"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, "UUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_FIELD, "Name"); //$NON-NLS-1$
        String matchPattern = IndexCriteriaBuilder.getMatchPattern(IndexConstants.INDEX_NAME.DATATYPES_INDEX, criteria);
        String expectedPattern = ""+IndexConstants.RECORD_TYPE.DATATYPE+//$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"*"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"baseTypeUUID"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"Name"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"UUID"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"nameInSource"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+IndexConstants.RECORD_STRING.MATCH_CHAR+
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+IndexConstants.RECORD_STRING.MATCH_CHAR+
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"runtTypeName"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+IndexConstants.RECORD_STRING.MATCH_CHAR; 

        assertEquals(matchPattern, expectedPattern);
    }
    
    public void testPropertyCriteriaPrefix1() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, PropertyRecord.MetadataFieldNames.PROPERTY_NAME_FIELD, "propName"); //$NON-NLS-1$
        helpAddToCriteria(criteria, PropertyRecord.MetadataFieldNames.PROPERTY_VALUE_FIELD, "propValue"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, "UUID"); //$NON-NLS-1$
        String matchPrefix = IndexCriteriaBuilder.getMatchPrefix(IndexConstants.INDEX_NAME.PROPERTIES_INDEX, criteria);
        String expectedPrefix = ""+IndexConstants.RECORD_TYPE.PROPERTY+//$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"uuid"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER;

        assertNotNull(matchPrefix);
        assertEquals(matchPrefix, expectedPrefix);        
    }
    
    public void testPropertyCriteriaPrefix2() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, PropertyRecord.MetadataFieldNames.PROPERTY_NAME_FIELD, "propName"); //$NON-NLS-1$
        helpAddToCriteria(criteria, PropertyRecord.MetadataFieldNames.PROPERTY_VALUE_FIELD, "propValue"); //$NON-NLS-1$
        String matchPrefix = IndexCriteriaBuilder.getMatchPrefix(IndexConstants.INDEX_NAME.PROPERTIES_INDEX, criteria);
        assertNull(matchPrefix);
    }
    
    public void testPropertyCriteriaPrefix3() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, PropertyRecord.MetadataFieldNames.PROPERTY_NAME_FIELD, "propName"); //$NON-NLS-1$
        helpAddToCriteria(criteria, PropertyRecord.MetadataFieldNames.PROPERTY_VALUE_FIELD, "propValue"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, "UU?ID"); //$NON-NLS-1$
        String matchPrefix = IndexCriteriaBuilder.getMatchPrefix(IndexConstants.INDEX_NAME.PROPERTIES_INDEX, criteria);
        assertNull(matchPrefix);       
    }
    
    public void testPropertyCriteriaPrefix4() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, PropertyRecord.MetadataFieldNames.PROPERTY_NAME_FIELD, "propName"); //$NON-NLS-1$
        helpAddToCriteria(criteria, PropertyRecord.MetadataFieldNames.PROPERTY_VALUE_FIELD, "propValue"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, "UU*ID"); //$NON-NLS-1$
        String matchPrefix = IndexCriteriaBuilder.getMatchPrefix(IndexConstants.INDEX_NAME.PROPERTIES_INDEX, criteria);
        assertNull(matchPrefix);      
    }    

    public void testPropertyCriteria1() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, PropertyRecord.MetadataFieldNames.PROPERTY_NAME_FIELD, "propName"); //$NON-NLS-1$
        helpAddToCriteria(criteria, PropertyRecord.MetadataFieldNames.PROPERTY_VALUE_FIELD, "propValue"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, "UUID"); //$NON-NLS-1$
        String matchPattern = IndexCriteriaBuilder.getMatchPattern(IndexConstants.INDEX_NAME.PROPERTIES_INDEX, criteria);
        String expectedPattern = ""+IndexConstants.RECORD_TYPE.PROPERTY+//$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"UUID"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"propName"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"propValue"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+IndexConstants.RECORD_STRING.MATCH_CHAR; 

        assertEquals(matchPattern, expectedPattern);
    }
    
    public void testPropertyCriteria2() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, PropertyRecord.MetadataFieldNames.PROPERTY_NAME_FIELD, "propName"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, "UUID"); //$NON-NLS-1$
        String matchPattern = IndexCriteriaBuilder.getMatchPattern(IndexConstants.INDEX_NAME.PROPERTIES_INDEX, criteria);
        String expectedPattern = ""+IndexConstants.RECORD_TYPE.PROPERTY+//$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"UUID"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"propName"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"*"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+IndexConstants.RECORD_STRING.MATCH_CHAR; 

        assertEquals(matchPattern, expectedPattern);
    }
    
    public void testModelCriteriaPrefix1() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_IN_SOURCE_FIELD, "nameInSource"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, "UUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_FIELD, "Name"); //$NON-NLS-1$
        String matchPrefix = IndexCriteriaBuilder.getMatchPrefix(IndexConstants.INDEX_NAME.MODELS_INDEX, criteria);
        String expectedPrefix = ""+IndexConstants.RECORD_TYPE.MODEL+//$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"NAME"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER;

        assertNotNull(matchPrefix);
        assertEquals(matchPrefix, expectedPrefix);        
    }
    
    public void testModelCriteriaPrefix2() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_IN_SOURCE_FIELD, "nameInSource"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, "UUID"); //$NON-NLS-1$
        String matchPrefix = IndexCriteriaBuilder.getMatchPrefix(IndexConstants.INDEX_NAME.MODELS_INDEX, criteria);
        assertNull(matchPrefix);       
    }
    
    public void testModelCriteriaPrefix3() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_IN_SOURCE_FIELD, "nameInSource"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, "UUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_FIELD, "Na?me"); //$NON-NLS-1$
        String matchPrefix = IndexCriteriaBuilder.getMatchPrefix(IndexConstants.INDEX_NAME.MODELS_INDEX, criteria);
        assertNull(matchPrefix);       
    }
    
    public void testModelCriteriaPrefix4() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_IN_SOURCE_FIELD, "nameInSource"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, "UUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_FIELD, "Nam*e"); //$NON-NLS-1$
        String matchPrefix = IndexCriteriaBuilder.getMatchPrefix(IndexConstants.INDEX_NAME.MODELS_INDEX, criteria);
        assertNull(matchPrefix);      
    }    
    
    public void testModelCriteria1() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.FULL_NAME_FIELD, "MyModel"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.PARENT_UUID_FIELD, "parentUUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_IN_SOURCE_FIELD, "nameInSource"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, "UUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_FIELD, "Name"); //$NON-NLS-1$
        String matchPattern = IndexCriteriaBuilder.getMatchPattern(IndexConstants.INDEX_NAME.MODELS_INDEX, criteria);
        String expectedPattern = ""+IndexConstants.RECORD_TYPE.MODEL+//$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"*"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"UUID"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"MyModel"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"nameInSource"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"parentUUID"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+IndexConstants.RECORD_STRING.MATCH_CHAR;

        assertEquals(matchPattern, expectedPattern);
    }
    
    public void testModelCriteria2() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.PARENT_UUID_FIELD, "parentUUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_IN_SOURCE_FIELD, "nameInSource"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, "UUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_FIELD, "MyModel"); //$NON-NLS-1$
        String matchPattern = IndexCriteriaBuilder.getMatchPattern(IndexConstants.INDEX_NAME.MODELS_INDEX, criteria);
        String expectedPattern = ""+IndexConstants.RECORD_TYPE.MODEL+//$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"*"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"UUID"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"MyModel"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"nameInSource"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"parentUUID"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+IndexConstants.RECORD_STRING.MATCH_CHAR;

        assertEquals(matchPattern, expectedPattern);
    }
    
    public void testModelCriteria3() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.PARENT_UUID_FIELD, "parentUUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_IN_SOURCE_FIELD, "nameInSource"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, "UUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_FIELD, "MyModel"); //$NON-NLS-1$
        String matchPattern = IndexCriteriaBuilder.getMatchPattern(IndexConstants.INDEX_NAME.MODELS_INDEX, criteria);
        String expectedPattern = ""+IndexConstants.RECORD_TYPE.MODEL+//$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"*"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"UUID"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"MyModel"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"nameInSource"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"parentUUID"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+IndexConstants.RECORD_STRING.MATCH_CHAR;

        assertEquals(matchPattern, expectedPattern);
    }    
    
    public void testHeaderCriteriaPrefix1() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.FULL_NAME_FIELD, "MyModel.Name"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.MODEL_NAME_FIELD, "MyModel"); //$NON-NLS-1$        
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.PARENT_UUID_FIELD, "parentUUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_IN_SOURCE_FIELD, "nameInSource"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, "UUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_FIELD, "Name"); //$NON-NLS-1$
        String matchPrefix = IndexCriteriaBuilder.getMatchPrefix(IndexConstants.INDEX_NAME.COLUMNS_INDEX, criteria);
        String expectedPrefix = ""+IndexConstants.RECORD_TYPE.COLUMN+//$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"MYMODEL.NAME"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER;

        assertNotNull(matchPrefix);
        assertEquals(matchPrefix, expectedPrefix);        
    }
    
    public void testHeaderCriteriaPrefix2() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.MODEL_NAME_FIELD, "MyModel"); //$NON-NLS-1$        
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.PARENT_UUID_FIELD, "parentUUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_IN_SOURCE_FIELD, "nameInSource"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, "UUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_FIELD, "Name"); //$NON-NLS-1$
        String matchPrefix = IndexCriteriaBuilder.getMatchPrefix(IndexConstants.INDEX_NAME.COLUMNS_INDEX, criteria);
        assertNull(matchPrefix);       
    }
    
    public void testHeaderCriteriaPrefix3() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.FULL_NAME_FIELD, "*"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.MODEL_NAME_FIELD, "MyModel"); //$NON-NLS-1$        
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.PARENT_UUID_FIELD, "parentUUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_IN_SOURCE_FIELD, "nameInSource"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, "UUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_FIELD, "Name"); //$NON-NLS-1$
        String matchPrefix = IndexCriteriaBuilder.getMatchPrefix(IndexConstants.INDEX_NAME.COLUMNS_INDEX, criteria);
        assertNull(matchPrefix);        
    }
    
    public void testHeaderCriteriaPrefix4() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.FULL_NAME_FIELD, "?"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.MODEL_NAME_FIELD, "MyModel"); //$NON-NLS-1$        
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.PARENT_UUID_FIELD, "parentUUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_IN_SOURCE_FIELD, "nameInSource"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, "UUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_FIELD, "Name"); //$NON-NLS-1$
        String matchPrefix = IndexCriteriaBuilder.getMatchPrefix(IndexConstants.INDEX_NAME.COLUMNS_INDEX, criteria);
        assertNull(matchPrefix);       
    }    

    public void testHeaderCriteria1() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.FULL_NAME_FIELD, "MyModel.Name"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.MODEL_NAME_FIELD, "MyModel"); //$NON-NLS-1$        
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.PARENT_UUID_FIELD, "parentUUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_IN_SOURCE_FIELD, "nameInSource"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, "UUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_FIELD, "Name"); //$NON-NLS-1$
        String matchPattern = IndexCriteriaBuilder.getMatchPattern(IndexConstants.INDEX_NAME.COLUMNS_INDEX, criteria);
        String expectedPattern = ""+IndexConstants.RECORD_TYPE.COLUMN+//$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"*"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"UUID"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"MyModel.Name"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"nameInSource"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"parentUUID"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+IndexConstants.RECORD_STRING.MATCH_CHAR; 

        assertEquals(matchPattern, expectedPattern);
    }
    
    public void testHeaderCriteria2() {
        Map criteria = new HashMap();
        //helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.FULL_NAME_FIELD, "MyModel.Name"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.MODEL_NAME_FIELD, "MyModel"); //$NON-NLS-1$        
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.PARENT_UUID_FIELD, "parentUUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_IN_SOURCE_FIELD, "nameInSource"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, "UUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_FIELD, "Name"); //$NON-NLS-1$
        String matchPattern = IndexCriteriaBuilder.getMatchPattern(IndexConstants.INDEX_NAME.COLUMNS_INDEX, criteria);
        String expectedPattern = ""+IndexConstants.RECORD_TYPE.COLUMN+//$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"*"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"UUID"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"MyModel.*.Name"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"nameInSource"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"parentUUID"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+IndexConstants.RECORD_STRING.MATCH_CHAR; 

        assertEquals(matchPattern, expectedPattern);
    }
    
    public void testHeaderCriteria3() {
        Map criteria = new HashMap();
        //helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.FULL_NAME_FIELD, "MyModel.Name"); //$NON-NLS-1$
        //helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.MODEL_NAME_FIELD, "MyModel"); //$NON-NLS-1$        
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.PARENT_UUID_FIELD, "parentUUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_IN_SOURCE_FIELD, "nameInSource"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, "UUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_FIELD, "Name"); //$NON-NLS-1$
        String matchPattern = IndexCriteriaBuilder.getMatchPattern(IndexConstants.INDEX_NAME.COLUMNS_INDEX, criteria);
        String expectedPattern = ""+IndexConstants.RECORD_TYPE.COLUMN+//$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"*"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"UUID"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"*.Name"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"nameInSource"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"parentUUID"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+IndexConstants.RECORD_STRING.MATCH_CHAR; 

        assertEquals(matchPattern, expectedPattern);
    }
    
    public void testHeaderCriteria4() {
        Map criteria = new HashMap();
        //helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.FULL_NAME_FIELD, "MyModel.Name"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.MODEL_NAME_FIELD, "MyModel"); //$NON-NLS-1$        
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.PARENT_UUID_FIELD, "parentUUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_IN_SOURCE_FIELD, "nameInSource"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, "UUID"); //$NON-NLS-1$
        //helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_FIELD, "Name"); //$NON-NLS-1$
        String matchPattern = IndexCriteriaBuilder.getMatchPattern(IndexConstants.INDEX_NAME.COLUMNS_INDEX, criteria);
        String expectedPattern = ""+IndexConstants.RECORD_TYPE.COLUMN+//$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"*"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"UUID"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"MyModel.*"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"nameInSource"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"parentUUID"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+IndexConstants.RECORD_STRING.MATCH_CHAR; 

        assertEquals(matchPattern, expectedPattern);
    }
    
    public void testHeaderCriteria5() {
        Map criteria = new HashMap();
        //helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.FULL_NAME_FIELD, "MyModel.Name"); //$NON-NLS-1$
        //helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.MODEL_NAME_FIELD, "MyModel"); //$NON-NLS-1$        
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.PARENT_UUID_FIELD, "parentUUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_IN_SOURCE_FIELD, "nameInSource"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, "UUID"); //$NON-NLS-1$
        //helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_FIELD, "Name"); //$NON-NLS-1$
        String matchPattern = IndexCriteriaBuilder.getMatchPattern(IndexConstants.INDEX_NAME.COLUMNS_INDEX, criteria);
        String expectedPattern = ""+IndexConstants.RECORD_TYPE.COLUMN+//$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"*"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"UUID"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"*"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"nameInSource"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"parentUUID"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+IndexConstants.RECORD_STRING.MATCH_CHAR; 

        assertEquals(matchPattern, expectedPattern);
    }
    
    public void testNoCriteriaPrefix() {
        Map criteria = new HashMap();
        String matchPattern = IndexCriteriaBuilder.getMatchPrefix(IndexConstants.INDEX_NAME.COLUMNS_INDEX, criteria);
        String expectedPattern = ""+IndexConstants.RECORD_TYPE.COLUMN+//$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER;
        assertEquals(matchPattern, expectedPattern);        
    }
    
    public void testNoCriteriaPattern() {
        Map criteria = new HashMap();
        String matchPattern = IndexCriteriaBuilder.getMatchPattern(IndexConstants.INDEX_NAME.COLUMNS_INDEX, criteria);
        String expectedPattern = ""+IndexConstants.RECORD_TYPE.COLUMN+//$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"*"; //$NON-NLS-1$
        assertEquals(matchPattern, expectedPattern);        
    }
    
    public void testNullCriteriaPrefix() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.FULL_NAME_FIELD, null);
        String matchPattern = IndexCriteriaBuilder.getMatchPrefix(IndexConstants.INDEX_NAME.COLUMNS_INDEX, criteria);
        String expectedPattern = ""+IndexConstants.RECORD_TYPE.COLUMN+//$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+IndexConstants.RECORD_STRING.SPACE+
        IndexConstants.RECORD_STRING.RECORD_DELIMITER;
        assertEquals(matchPattern, expectedPattern);        
    }

    public void testNullCriteriaPattern() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.FULL_NAME_FIELD, null);
        String matchPattern = IndexCriteriaBuilder.getMatchPattern(IndexConstants.INDEX_NAME.COLUMNS_INDEX, criteria);
        String expectedPattern = ""+IndexConstants.RECORD_TYPE.COLUMN+//$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"*"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"*"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+IndexConstants.RECORD_STRING.SPACE+
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"*"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"*"+ //$NON-NLS-1$
        IndexConstants.RECORD_STRING.RECORD_DELIMITER+"*"; //$NON-NLS-1$
        assertEquals(matchPattern, expectedPattern);        
    }
    
    public void testGetMetadataLiteralCriteria1() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.MODEL_NAME_FIELD, "MyModel"); //$NON-NLS-1$        
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.PARENT_UUID_FIELD, "parentUUID"); //$NON-NLS-1$
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_IN_SOURCE_FIELD, "nameInSource"); //$NON-NLS-1$
        Collection criteriaValues = new ArrayList();
        criteriaValues.add("uuid1"); //$NON-NLS-1$
        criteriaValues.add("uuid2"); //$NON-NLS-1$
        criteriaValues.add("uuid3"); //$NON-NLS-1$
        helpAddToInCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, criteriaValues);
        Collection criteriaMaps = IndexCriteriaBuilder.getLiteralCriteria(criteria);
        assertEquals(3, criteriaMaps.size());
    }
    
    public void testGetMetadataLiteralCriteria2() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.MODEL_NAME_FIELD, "MyModel"); //$NON-NLS-1$        
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_IN_SOURCE_FIELD, "nameInSource"); //$NON-NLS-1$
        Collection criteriaValues = new ArrayList();
        criteriaValues.add("uuid1"); //$NON-NLS-1$
        criteriaValues.add("uuid2"); //$NON-NLS-1$
        criteriaValues.add("uuid3"); //$NON-NLS-1$
        helpAddToInCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, criteriaValues);
        Collection criteriaValues2 = new ArrayList();
        criteriaValues2.add("parentUUID1"); //$NON-NLS-1$
        criteriaValues2.add("parentUUID2"); //$NON-NLS-1$
        criteriaValues2.add("parentUUID3"); //$NON-NLS-1$
        helpAddToInCriteria(criteria, MetadataRecord.MetadataFieldNames.PARENT_UUID_FIELD, criteriaValues);
        
        
        Collection criteriaMaps = IndexCriteriaBuilder.getLiteralCriteria(criteria);
        assertEquals(9, criteriaMaps.size());
    }
    
    public void testGetMetadataLiteralCriteria3() {
        Map criteria = new HashMap();
        helpAddToCriteria(criteria, MetadataRecord.MetadataFieldNames.MODEL_NAME_FIELD, "MyModel"); //$NON-NLS-1$        
        Collection criteriaValues = new ArrayList();
        criteriaValues.add("uuid1"); //$NON-NLS-1$
        criteriaValues.add("uuid2"); //$NON-NLS-1$
        criteriaValues.add("uuid3"); //$NON-NLS-1$
        helpAddToInCriteria(criteria, MetadataRecord.MetadataFieldNames.UUID_FIELD, criteriaValues);
        Collection criteriaValues2 = new ArrayList();
        criteriaValues2.add("parentUUID1"); //$NON-NLS-1$
        criteriaValues2.add("parentUUID2"); //$NON-NLS-1$
        criteriaValues2.add("parentUUID3"); //$NON-NLS-1$
        helpAddToInCriteria(criteria, MetadataRecord.MetadataFieldNames.PARENT_UUID_FIELD, criteriaValues);
        Collection criteriaValues3 = new ArrayList();
        criteriaValues3.add("nameInSource1"); //$NON-NLS-1$
        criteriaValues3.add("nameInSource2"); //$NON-NLS-1$
        helpAddToInCriteria(criteria, MetadataRecord.MetadataFieldNames.NAME_IN_SOURCE_FIELD, criteriaValues3);        
        
        Collection criteriaMaps = IndexCriteriaBuilder.getLiteralCriteria(criteria);
        assertEquals(18, criteriaMaps.size());
    }     
}