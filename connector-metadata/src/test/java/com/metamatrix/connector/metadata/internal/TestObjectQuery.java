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

import org.teiid.connector.metadata.MetadataLiteralCriteria;
import org.teiid.connector.metadata.MetadataSearchCriteria;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import junit.framework.TestCase;
import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.query.unittest.QueryMetadataInterfaceBuilder;

public class TestObjectQuery extends TestCase {
    private RuntimeMetadata metadata;
    private CommandBuilder commandBuilder;

    /**
     * Constructor for ObjectQueryTest.
     * @param name
     */
    public TestObjectQuery(String name) {
        super(name);
    }

    /*
     * @see TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
        QueryMetadataInterfaceBuilder builder = new QueryMetadataInterfaceBuilder();
        builder.addPhysicalModel("system"); //$NON-NLS-1$
        builder.addGroup("t"); //$NON-NLS-1$
        builder.addElement("x", String.class); //$NON-NLS-1$
        builder.addElement("y", String.class); //$NON-NLS-1$
        builder.addElement("z", Integer.class);           //$NON-NLS-1$
        metadata = builder.getRuntimeMetadata();
        commandBuilder = new CommandBuilder(builder.getQueryMetadata());
    }

    public void testGetTableName() throws Exception {
        ObjectQuery query = getQuery("select x from t"); //$NON-NLS-1$
        assertEquals("t", query.getTableNameInSource()); //$NON-NLS-1$
    }

    public void testTypeCheckingMatch() throws Exception {
        ObjectQuery query = getQuery("select x from t"); //$NON-NLS-1$
        query.checkType(0, ""); //$NON-NLS-1$
    }

    public void testTypeCheckingDoesNotMatch() throws Exception {
        ObjectQuery query = getQuery("select x from t"); //$NON-NLS-1$
        try {
            query.checkType(0, new Integer(0));
            fail();
        } catch (MetaMatrixRuntimeException e) {
            assertEquals("Types do not match for: t.x expected: java.lang.String but was: java.lang.Integer.", e.getMessage()); //$NON-NLS-1$
        }
    }

    private ObjectQuery getQuery(String queryText) throws Exception {
        return new ObjectQuery(metadata,commandBuilder.getCommand(queryText));
    }

    public void testGetColumnNames() throws Exception {
        ObjectQuery query = getQuery("select x from t"); //$NON-NLS-1$
        assertEquals("x", query.getColumnNames()[0]); //$NON-NLS-1$
    }

    public void testGetCriteria() throws Exception {
        ObjectQuery query = getQuery("select x from t"); //$NON-NLS-1$
        assertNotNull(query.getCriteria());
        assertTrue(query.getCriteria().isEmpty());
    }

    public void testOneCriteria() throws Exception {
        ObjectQuery query = getQuery("select x from t where x='a'"); //$NON-NLS-1$
        MetadataSearchCriteria criteria = (MetadataSearchCriteria) query.getCriteria().get("X"); //$NON-NLS-1$
        assertNotNull(criteria);
        assertTrue(criteria instanceof MetadataLiteralCriteria);
        MetadataLiteralCriteria literalCriteria = (MetadataLiteralCriteria) criteria;
        assertEquals("x", literalCriteria.getFieldName()); //$NON-NLS-1$
        assertEquals("a", literalCriteria.getFieldValue()); //$NON-NLS-1$
    }

    public void testReversedCriteria() throws Exception {
        ObjectQuery query = getQuery("select x from t where 'a'=x"); //$NON-NLS-1$
        MetadataSearchCriteria criteria = (MetadataSearchCriteria) query.getCriteria().get("X"); //$NON-NLS-1$
        assertNotNull(criteria);
        assertTrue(criteria instanceof MetadataLiteralCriteria);
        MetadataLiteralCriteria literalCriteria = (MetadataLiteralCriteria) criteria;
        assertEquals("x", literalCriteria.getFieldName()); //$NON-NLS-1$
        assertEquals("a", literalCriteria.getFieldValue()); //$NON-NLS-1$
    }

    public void testMultipleCriteria() throws Exception {
        ObjectQuery query = getQuery("select x from t where x='a' and y='b'"); //$NON-NLS-1$
        MetadataSearchCriteria criteria1 = (MetadataSearchCriteria) query.getCriteria().get("X"); //$NON-NLS-1$
        assertNotNull(criteria1);
        assertTrue(criteria1 instanceof MetadataLiteralCriteria);
        MetadataLiteralCriteria literalCriteria1 = (MetadataLiteralCriteria) criteria1;
        assertEquals("x", literalCriteria1.getFieldName()); //$NON-NLS-1$
        assertEquals("a", literalCriteria1.getFieldValue()); //$NON-NLS-1$

        MetadataSearchCriteria criteria2 = (MetadataSearchCriteria) query.getCriteria().get("X"); //$NON-NLS-1$
        assertNotNull(criteria2);
        assertTrue(criteria2 instanceof MetadataLiteralCriteria);
        MetadataLiteralCriteria literalCriteria = (MetadataLiteralCriteria) criteria2;
        assertEquals("x", literalCriteria.getFieldName()); //$NON-NLS-1$
        assertEquals("a", literalCriteria.getFieldValue()); //$NON-NLS-1$
    }

    public void testMultipleCriteriaWithOr() throws Exception {
        try {
            ObjectQuery query = getQuery("select x from t where x='a' or y='b'"); //$NON-NLS-1$
            query.getCriteria();
            fail("'or' not supported." ); //$NON-NLS-1$
        } catch (RuntimeException e) {
            assertEquals("Only supports 'AND' operator in compound criteria.", e.getMessage()); //$NON-NLS-1$
        }
    }
}
