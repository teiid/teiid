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

package org.teiid.query.processor;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.processor.relational.FakeRelationalNode;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("nls")
public class TestBatchCollector {

	@Test public void testCollect() throws Exception {
		FakeRelationalNode sourceNode = new FakeRelationalNode(1, new List[] {
				Arrays.asList(1),
				Arrays.asList(1),
				Arrays.asList(1)
			}, 1);
		sourceNode.setElements(Arrays.asList(new ElementSymbol("x", null, DataTypeManager.DefaultDataClasses.INTEGER)));
		BatchCollector bc = new BatchCollector(sourceNode, BufferManagerFactory.getStandaloneBufferManager(), new CommandContext(), false);
		bc.collectTuples(true);
		assertEquals(1, bc.getTupleBuffer().getManagedRowCount());
		assertEquals(3, bc.collectTuples().getRowCount());
	}
	
}
