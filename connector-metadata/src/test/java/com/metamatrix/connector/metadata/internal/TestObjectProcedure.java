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

package com.metamatrix.connector.metadata.internal;

import java.util.Map;

import org.teiid.connector.metadata.MetadataLiteralCriteria;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import junit.framework.TestCase;
import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.query.unittest.QueryMetadataInterfaceBuilder;


/**
 * @since 4.2
 */
public class TestObjectProcedure extends TestCase {

    private RuntimeMetadata metadata;
    private CommandBuilder commandBuilder;

    /**
     * @param name
     * @since 4.2
     */
    public TestObjectProcedure(String name) {
        super(name);
    }

    /*
     * @see TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
        QueryMetadataInterfaceBuilder builder = new QueryMetadataInterfaceBuilder();
        builder.addPhysicalModel("system"); //$NON-NLS-1$
        builder.addInputParameter("getparam1", String.class); //$NON-NLS-1$
        builder.addInputParameter("getparam2", String.class); //$NON-NLS-1$
        builder.addInputParameter("getparam3", Integer.class);//$NON-NLS-1$
        String [] columns = {"col1", "col2", "col3"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Class[] types =  {String.class, Integer.class, String.class};
        builder.addResultSet("resultSet", columns, types); //$NON-NLS-1$
        builder.addProcedure("proc"); //$NON-NLS-1$
        metadata = builder.getRuntimeMetadata();
        commandBuilder = new CommandBuilder(builder.getQueryMetadata());
    }

    private ObjectProcedure getProcedure(String queryText) throws Exception {
        return new ObjectProcedure(metadata,commandBuilder.getCommand(queryText));
    }

    public void testProcedureName() throws Exception {
        ObjectProcedure proc = getProcedure("exec proc (\"x\", \"y\", 1)"); //$NON-NLS-1$
        assertEquals("proc", proc.getProcedureNameInSource()); //$NON-NLS-1$
    }

    public void testGetColumnNames() throws Exception {
        ObjectProcedure proc = getProcedure("exec proc (\"x\", \"y\", 1)"); //$NON-NLS-1$
        assertEquals("col1", proc.getColumnNames()[0]); //$NON-NLS-1$
    }

    public void testGetCriteria() throws Exception {
        ObjectProcedure proc = getProcedure("exec proc (\"x\", \"y\", 1)"); //$NON-NLS-1$
        Map criteria = proc.getCriteria();
        assertNotNull(criteria);
        assertEquals(3, criteria.size());
        Object litCriteria = criteria.get("getparam1".toUpperCase()); //$NON-NLS-1$
        assertNotNull(litCriteria);
        assertTrue(litCriteria instanceof MetadataLiteralCriteria);
    }

}
