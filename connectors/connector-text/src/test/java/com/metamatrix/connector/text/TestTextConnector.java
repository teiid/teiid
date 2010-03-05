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

package com.metamatrix.connector.text;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.MetadataProvider;
import org.teiid.connector.metadata.runtime.Datatype;
import org.teiid.connector.metadata.runtime.MetadataFactory;
import org.teiid.connector.metadata.runtime.Table;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.util.UnitTestUtil;

/**
 */
public class TestTextConnector {
    private static final String DESC_FILE = UnitTestUtil.getTestDataPath() + "/testDescriptorDelimited.txt"; //$NON-NLS-1$

    public TextConnector helpSetUp(String descFile) throws Exception {
        Properties props = new Properties();
        props.put(TextPropertyNames.DESCRIPTOR_FILE, descFile);

        TextManagedConnectionFactory config = Mockito.mock(TextManagedConnectionFactory.class);
        Mockito.stub(config.getDescriptorFile()).toReturn(descFile);
        Mockito.stub(config.getLogger()).toReturn(Mockito.mock(ConnectorLogger.class));
        Mockito.stub(config.isPartialStartupAllowed()).toReturn(true);
        
        TextConnector connector = new TextConnector();
        connector.initialize(config);
        return connector;
    }
    
    // descriptor and data file both are files
    @Test public void testGetConnection() throws Exception{
        TextConnector connector = helpSetUp(DESC_FILE);
        TextConnection conn = (TextConnection) connector.getConnection();
        assertNotNull(conn);
    }
    
    @Test public void testGetMetadata() throws Exception{
        TextConnector connector = helpSetUp(UnitTestUtil.getTestDataPath() + "/SummitData_Descriptor.txt"); //$NON-NLS-1$
        Map<String, Datatype> datatypes = new HashMap<String, Datatype>();
        datatypes.put(DataTypeManager.DefaultDataTypes.STRING, new Datatype());
        datatypes.put(DataTypeManager.DefaultDataTypes.BIG_INTEGER, new Datatype());
        datatypes.put(DataTypeManager.DefaultDataTypes.INTEGER, new Datatype());
        datatypes.put(DataTypeManager.DefaultDataTypes.TIMESTAMP, new Datatype());
        
        MetadataFactory metadata = new MetadataFactory("SummitData", datatypes, new Properties()); //$NON-NLS-1$
        
        ((MetadataProvider)connector.getConnection()).getConnectorMetadata(metadata); 
        
        assertEquals(0, metadata.getMetadataStore().getSchemas().values().iterator().next().getProcedures().size());
        Table group = metadata.getMetadataStore().getSchemas().values().iterator().next().getTables().get("summitdata"); //$NON-NLS-1$
        assertEquals("SUMMITDATA", group.getName()); //$NON-NLS-1$
        assertEquals("SummitData.SUMMITDATA", group.getFullName()); //$NON-NLS-1$
        assertEquals(14, group.getColumns().size());
        assertNotNull(group.getUUID());
        assertEquals("string", group.getColumns().get(0).getNativeType()); //$NON-NLS-1$
    }


}
