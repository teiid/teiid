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

package org.teiid.deployers;

import static org.junit.Assert.*;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.vdb.runtime.VDBKey;

@SuppressWarnings("nls")
public class TestVDBRepository {
    
    @Test public void testVDBRespositoryGetActive() throws Exception {
        VDBRepository repo = new VDBRepository();
        CompositeVDB mock1 = Mockito.mock(CompositeVDB.class);
        VDBMetaData vdb = new VDBMetaData();
        vdb.setName("name");
        vdb.setVersion(1);
        vdb.setConnectionType(ConnectionType.NONE);
        Mockito.stub(mock1.getVDB()).toReturn(vdb);
        repo.getVdbRepo().put(new VDBKey("name", 1), mock1);
        CompositeVDB mock2 = Mockito.mock(CompositeVDB.class);
        VDBMetaData vdb2 = new VDBMetaData();
        vdb2.setName("name");
        vdb2.setVersion(2);
        vdb2.setConnectionType(ConnectionType.NONE);
        Mockito.stub(mock2.getVDB()).toReturn(vdb2);
        repo.getVdbRepo().put(new VDBKey("name", 2), mock2);
        
        VDBMetaData live = repo.getLiveVDB("name");
        assertEquals("2", live.getVersion());
    } 


}
