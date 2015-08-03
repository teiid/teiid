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

import java.util.LinkedHashMap;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.tempdata.GlobalTableStore;
import org.teiid.query.tempdata.GlobalTableStoreImpl;
import org.teiid.vdb.runtime.VDBKey;

@SuppressWarnings("nls")
public class TestCompositeGlobalTableStore {

	@Test public void testCompositeGlobalTableStore() throws VirtualDatabaseException {
		CompositeVDB vdb = TestCompositeVDB.createCompositeVDB(new MetadataStore(), "foo");
		GlobalTableStore gts = CompositeGlobalTableStore.createInstance(vdb, BufferManagerFactory.getStandaloneBufferManager(), null);
		assertTrue(gts instanceof GlobalTableStoreImpl);
		
		vdb.children = new LinkedHashMap<VDBKey, CompositeVDB>();
		MetadataStore ms = new MetadataStore();
		Schema s = new Schema();
		s.setName("x");
		ms.addSchema(s);
		CompositeVDB imported = TestCompositeVDB.createCompositeVDB(ms, "foo");
		GlobalTableStore gts1 = Mockito.mock(GlobalTableStore.class);
		imported.getVDB().addAttchment(GlobalTableStore.class, gts1);
		vdb.getChildren().put(new VDBKey("foo1", 1), imported);
		
		CompositeGlobalTableStore cgts = (CompositeGlobalTableStore)CompositeGlobalTableStore.createInstance(vdb, BufferManagerFactory.getStandaloneBufferManager(), null);
		assertEquals(gts1, cgts.getStoreForTable(RelationalPlanner.MAT_PREFIX + "X.Y"));
		assertEquals(cgts.getPrimary(), cgts.getStore("Z"));
	}
	
}
