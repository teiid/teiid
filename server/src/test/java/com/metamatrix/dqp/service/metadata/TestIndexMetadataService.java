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

/*
 */
package com.metamatrix.dqp.service.metadata;

import java.util.Collection;
import java.util.List;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.metadata.QueryMetadataCache;

import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.query.metadata.QueryMetadataInterface;

public class TestIndexMetadataService extends TestCase {
    private QueryMetadataInterface metadata;

    public TestIndexMetadataService(String name) {
        super(name);
    }

    /*
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
        String filePath = "PartsSupplier.vdb"; //$NON-NLS-1$
        QueryMetadataCache sharedCache = new QueryMetadataCache(Thread.currentThread().getContextClassLoader().getResource("System.vdb")); //$NON-NLS-1$
        IndexMetadataService metadataService = new IndexMetadataService(sharedCache);
        ApplicationEnvironment env = new ApplicationEnvironment();
        VDBService vdbService = Mockito.mock(VDBService.class);
        Mockito.stub(vdbService.getVDB("PartsSupplier", "1")).toReturn(new VDBArchive(Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath))); //$NON-NLS-1$ //$NON-NLS-2$ 
        env.bindService(DQPServiceNames.VDB_SERVICE, vdbService);
        metadataService.start(env);
        metadata = metadataService.lookupMetadata("PartsSupplier", "1"); //$NON-NLS-1$ //$NON-NLS-2$ 
    }

    //tests
    public void testGetElementID() throws Exception {
        assertNotNull(getElementID());
    }

    public void testGetGroupIDForElementID() throws Exception {
        assertNotNull(metadata.getGroupIDForElementID(getElementID()));
    }

    public void testGetFullName() throws Exception {
        Object groupID = metadata.getGroupIDForElementID(getElementID());
        String groupName = metadata.getFullName(groupID);
        assertEquals("PartsSupplier.PARTSSUPPLIER.PARTS", groupName); //$NON-NLS-1$
    }

    public void testGetNameInSource() throws Exception {
        String nameInSource = metadata.getNameInSource(getElementID());
        assertEquals("PART_ID", nameInSource); //$NON-NLS-1$
    }

    public void testGetFullNameForModel() throws Exception {
        Object modelID = metadata.getModelID(getGroupID());
        assertNotNull(modelID);
        String modelName = metadata.getFullName(modelID);
        assertEquals("PartsSupplier", modelName); //$NON-NLS-1$
    }

    public void testGetFullNameForModelLookupByGroupID() throws Exception {
        Object groupID = metadata.getGroupIDForElementID(getElementID());

        Object modelID = metadata.getModelID(groupID);
        assertNotNull(modelID);
        String modelName = metadata.getFullName(modelID);
        assertEquals("PartsSupplier", modelName); //$NON-NLS-1$
    }

    public void testGetGroupIDCaseInsensitive() throws Exception {
        Object groupID = metadata.getGroupID("PartsSupplier.PARTSSUPPLIEr.PARTs"); //$NON-NLS-1$
        String groupName = metadata.getFullName(groupID);
        assertEquals("PartsSupplier.PARTSSUPPLIER.PARTS", groupName); //$NON-NLS-1$
    }

    public void testGetFullElementName() throws Exception {
        String name = metadata.getFullElementName("PartsSupplier.PartsSupplier.PARTS", "PART_ID"); //$NON-NLS-1$             //$NON-NLS-2$
        assertEquals("PartsSupplier.PartsSupplier.PARTS.PART_ID", name); //$NON-NLS-1$
    }

    public void testGetElementIDsInGroupID() throws Exception {
        Object groupID = getGroupID();

        List eIDs = metadata.getElementIDsInGroupID(groupID);
        assertEquals(4, eIDs.size());
    }

    public void testGroupSize() throws Exception {
        Collection groups = metadata.getGroupsForPartialName("PARTS"); //$NON-NLS-1$
        assertEquals(1, groups.size());
    }

    public void testIsVirtualGroup() throws Exception {
        Object groupID = getGroupID();
        assertFalse(metadata.isVirtualGroup(groupID));
    }

    private Object getElementID() throws Exception {
        return metadata.getElementID("PartsSupplier.PARTSSUPPLIER.PARTS.PART_ID"); //$NON-NLS-1$
    }

    public Object getGroupID() throws Exception {
        return metadata.getGroupID("PartsSupplier.PARTSSUPPLIER.PARTS"); //$NON-NLS-1$
    }
}
