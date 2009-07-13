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

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.metadata.runtime.DatatypeRecordImpl;
import org.teiid.connector.metadata.runtime.MetadataFactory;
import org.teiid.connector.metadata.runtime.TableRecordImpl;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.util.UnitTestUtil;

/**
 */
public class TestTextConnector {
    private static final String DESC_FILE = UnitTestUtil.getTestDataPath() + "/testDescriptorDelimited.txt"; //$NON-NLS-1$

    public TextConnector helpSetUp(String descFile) throws Exception {
        Properties props = new Properties();
        props.put(TextPropertyNames.DESCRIPTOR_FILE, descFile);

        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, false);
        TextConnector connector = new TextConnector();
        // Initialize license checker with class, non-GUI notifier and don't exitOnFailure
        connector.start(env);
        return connector;
    }
    
    // descriptor and data file both are files
    @Test public void testGetConnection() throws Exception{
        TextConnector connector = helpSetUp(DESC_FILE);
        TextConnection conn = (TextConnection) connector.getConnection(null);
        assertNotNull(conn);
    }
    
    @Test public void testGetMetadata() throws Exception{
        TextConnector connector = helpSetUp(UnitTestUtil.getTestDataPath() + "/SummitData_Descriptor.txt"); //$NON-NLS-1$
        Map<String, DatatypeRecordImpl> datatypes = new HashMap<String, DatatypeRecordImpl>();
        datatypes.put(DataTypeManager.DefaultDataTypes.STRING, new DatatypeRecordImpl());
        datatypes.put(DataTypeManager.DefaultDataTypes.BIG_INTEGER, new DatatypeRecordImpl());
        datatypes.put(DataTypeManager.DefaultDataTypes.INTEGER, new DatatypeRecordImpl());
        datatypes.put(DataTypeManager.DefaultDataTypes.TIMESTAMP, new DatatypeRecordImpl());
        MetadataFactory metadata = new MetadataFactory("SummitData", datatypes, new Properties()); //$NON-NLS-1$
        connector.getConnectorMetadata(metadata); 
        assertFalse(metadata.getProcedures().iterator().hasNext());
        Iterator<TableRecordImpl> tableIter = metadata.getTables().iterator();
        TableRecordImpl group = tableIter.next();
        assertEquals("SUMMITDATA", group.getName()); //$NON-NLS-1$
        assertEquals("SummitData.SUMMITDATA", group.getFullName()); //$NON-NLS-1$
        assertEquals(14, group.getColumns().size());
        assertNotNull(group.getUUID());
        assertEquals("string", group.getColumns().get(0).getNativeType()); //$NON-NLS-1$
    }


}
