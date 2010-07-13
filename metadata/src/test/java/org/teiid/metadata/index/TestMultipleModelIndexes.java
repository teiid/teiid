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

package org.teiid.metadata.index;

import static org.junit.Assert.*;

import java.util.Collection;

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.Table;
import org.teiid.metadata.TransformationMetadata;

@SuppressWarnings("nls")
public class TestMultipleModelIndexes {

	@Test public void testMultiple() throws Exception {
		TransformationMetadata tm = VDBMetadataFactory.getVDBMetadata(UnitTestUtil.getTestDataPath() + "/ZZZ.vdb");
		Collection<String> names = tm.getGroupsForPartialName("PRODUCTDATA");
		assertEquals(1, names.size());
		names = tm.getGroupsForPartialName("PARTS");
		assertEquals(1, names.size());
		
		//ensure that datatypes are set
		Table t = (Table)tm.getGroupID(names.iterator().next());
		assertNotNull(t.getColumns().get(0).getDatatype());
	}
	
}
