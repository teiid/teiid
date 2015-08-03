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

package org.teiid.dqp.internal.process;

import static org.junit.Assert.*;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.TupleSource;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.RegisterRequestParameter;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("nls")
public class TestTupleSourceCache {

	@Test public void testNodeId() throws Exception {
		TupleSourceCache tsc = new TupleSourceCache();
		HardcodedDataManager pdm = new HardcodedDataManager() {
			@Override
			public TupleSource registerRequest(CommandContext context,
					Command command, String modelName,
					RegisterRequestParameter parameterObject)
					throws TeiidComponentException {
				assertEquals(1, parameterObject.nodeID);
				return Mockito.mock(TupleSource.class);
			}
		};
		CommandContext context = TestProcessor.createCommandContext();
		BufferManagerImpl bufferMgr = BufferManagerFactory.createBufferManager();
		Command command = new Insert();
		RegisterRequestParameter parameterObject = new RegisterRequestParameter("z", 1, 1);
		parameterObject.info = new RegisterRequestParameter.SharedAccessInfo();
		
		tsc.getSharedTupleSource(context, command, "x", parameterObject, bufferMgr, pdm);
	}
	
}
