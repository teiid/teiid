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

import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.TranslationUtility;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.data.language.IGroup;
import com.metamatrix.data.language.IQuery;

/**
 */
public class TestGroup extends TestCase {

    private static TranslationUtility CONNECTOR_METADATA_UTILITY = createTranslationUtility(getTestVDBName());

    /**
     * Constructor for TestGroup.
     * @param name
     */
    public TestGroup(String name) {
        super(name);
    }

    private static String getTestVDBName() {
    	return UnitTestUtil.getTestDataPath() + "/ConnectorMetadata.vdb"; //$NON-NLS-1$
    }
    
    public static TranslationUtility createTranslationUtility(String vdbName) {
        return new TranslationUtility(vdbName);        
    }

    // ################ TEST GROUP METADATAID ######################
    
    public Group getGroup(String groupName, TranslationUtility transUtil) throws Exception {
        IQuery query = (IQuery) transUtil.parseCommand("SELECT 1 FROM " + groupName); //$NON-NLS-1$
        IGroup group = (IGroup) query.getFrom().getItems().get(0);
        MetadataID metadataID = group.getMetadataID();
        return (Group) transUtil.createRuntimeMetadata().getObject(metadataID);
    }

    public void helpTestGroup(String fullGroupName, String nameInSource, Properties expectedProps, TranslationUtility transUtil) throws Exception {
        Group group = getGroup(fullGroupName, transUtil);     
        assertEquals("table name in source", group.getNameInSource()); //$NON-NLS-1$
        
        Properties extProps = group.getProperties();
        assertEquals(expectedProps, extProps);
    }
    
    public void testGroup() throws Exception {
        Properties props = new Properties();
        props.put("TestExtraProperty", "extension prop value"); //$NON-NLS-1$ //$NON-NLS-2$
        helpTestGroup("ConnectorMetadata.TestTable", "TestTable", props, CONNECTOR_METADATA_UTILITY);//$NON-NLS-1$ //$NON-NLS-2$ 
    }   
    

}
