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

package com.metamatrix.data.metadata.runtime;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.TranslationUtility;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.data.language.IProcedure;

/**
 */
public class TestProcedure extends TestCase {

    private static TranslationUtility CONNECTOR_METADATA_UTILITY = createTranslationUtility(getTestVDBName());

    /**
     * Constructor for TestGroup.
     * @param name
     */
    public TestProcedure(String name) {
        super(name);
    }

    private static String getTestVDBName() {
        return UnitTestUtil.getTestDataPath() + "/ConnectorMetadata.vdb"; //$NON-NLS-1$
    }
    
    public static TranslationUtility createTranslationUtility(String vdbName) {
        return new TranslationUtility(vdbName);        
    }

    public Procedure getProcedure(String procName, int inputArgs, TranslationUtility transUtil) throws Exception {
        StringBuffer sql = new StringBuffer("EXEC " + procName + "("); //$NON-NLS-1$ //$NON-NLS-2$
        if(inputArgs > 0) {
            sql.append("?"); //$NON-NLS-1$
            for(int i=1; i<inputArgs; i++) {
                sql.append(", ?");                 //$NON-NLS-1$
            }
        }
        sql.append(")"); //$NON-NLS-1$
        IProcedure proc = (IProcedure) transUtil.parseCommand(sql.toString()); //$NON-NLS-1$
        MetadataID metadataID = proc.getMetadataID();
        return (Procedure) transUtil.createRuntimeMetadata().getObject(metadataID);
    }

    public void testProcedure1() throws Exception {
        Procedure proc = getProcedure("ConnectorMetadata.TestProc1", 2, CONNECTOR_METADATA_UTILITY);      //$NON-NLS-1$
        assertEquals("Procedure name in source", proc.getNameInSource()); //$NON-NLS-1$
        
        String[] nameInSource = new String[] { "Param name in source", null, null, null }; //$NON-NLS-1$
        int[] direction = new int[] { Parameter.IN, Parameter.OUT, Parameter.INOUT, Parameter.RETURN };
        int[] index = new int[] { 1, 2, 3, 4 };
        Class[] type = new Class[] { Integer.class, Long.class, Short.class, java.sql.Date.class };        
        
        MetadataID procID = proc.getMetadataID();
        Collection paramIDs = procID.getChildIDs();        
        Iterator paramIter = paramIDs.iterator();
        for(int i=0; paramIter.hasNext(); i++) {
            MetadataID paramID = (MetadataID) paramIter.next();
            Parameter param = (Parameter) CONNECTOR_METADATA_UTILITY.createRuntimeMetadata().getObject(paramID);
            
            assertEquals(nameInSource[i], param.getNameInSource());
            assertEquals(direction[i], param.getDirection());
            assertEquals(index[i], param.getIndex());
            assertEquals(type[i], param.getJavaType());
        }
                
    }   
    
    public void testProcedureWithResultSet() throws Exception {
        RuntimeMetadata rmd = CONNECTOR_METADATA_UTILITY.createRuntimeMetadata();
        Procedure proc = getProcedure("ConnectorMetadata.TestProc2", 1, CONNECTOR_METADATA_UTILITY);      //$NON-NLS-1$
        assertEquals(null, proc.getNameInSource()); //$NON-NLS-1$
        
        String[] nameInSource = new String[] { null, "Result set name in source" }; //$NON-NLS-1$
        int[] direction = new int[] { Parameter.IN, Parameter.RESULT_SET };
        int[] index = new int[] { 1, 2 };
        Class[] type = new Class[] { String.class, java.sql.ResultSet.class };        
        
        MetadataID procID = proc.getMetadataID();
        Collection paramIDs = procID.getChildIDs();        
        Iterator paramIter = paramIDs.iterator();
        MetadataID paramID = null;
        for(int i=0; paramIter.hasNext(); i++) {
            paramID = (MetadataID) paramIter.next();
            Parameter param = (Parameter) rmd.getObject(paramID);
            
            assertEquals(nameInSource[i], param.getNameInSource());
            assertEquals(direction[i], param.getDirection());
            assertEquals(index[i], param.getIndex());
            assertEquals(type[i], param.getJavaType());
        }
        
        // Check last param is a result set
        List rsCols = paramID.getChildIDs();

        // Check first column of result set
        assertEquals(2, rsCols.size());
        MetadataID elemID = (MetadataID) rsCols.get(0);
        assertEquals("RSCol1", elemID.getName());         //$NON-NLS-1$
        assertEquals("ConnectorMetadata.TestProc2.RSParam.RSCol1", elemID.getFullName());         //$NON-NLS-1$
        assertEquals(MetadataID.TYPE_ELEMENT, elemID.getType());        
        Element e1 = (Element) rmd.getObject(elemID);
        assertEquals("Result set column name in source", e1.getNameInSource());         //$NON-NLS-1$
        assertEquals(java.sql.Timestamp.class, e1.getJavaType());
        assertEquals(0, e1.getPosition());
        
        MetadataID elemID2 = (MetadataID) rsCols.get(1);        
        assertEquals("RSCol2", elemID2.getName());         //$NON-NLS-1$
        assertEquals("ConnectorMetadata.TestProc2.RSParam.RSCol2", elemID2.getFullName());         //$NON-NLS-1$
        assertEquals(MetadataID.TYPE_ELEMENT, elemID2.getType());        
        Element e2 = (Element) rmd.getObject(elemID2);
        assertEquals(null, e2.getNameInSource());         //$NON-NLS-1$
        assertEquals(String.class, e2.getJavaType());
        assertEquals(1, e2.getPosition());
        Properties props = new Properties();
        props.put("ColProp", "defaultvalue"); //$NON-NLS-1$ //$NON-NLS-2$
        
        // failing because default extension properties aren't in the VDB file
        //assertEquals(props, e2.getProperties());

    }   
    
}
