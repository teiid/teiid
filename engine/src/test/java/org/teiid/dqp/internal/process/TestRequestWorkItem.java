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

import java.util.Arrays;
import java.util.List;

import org.teiid.dqp.internal.process.RequestWorkItem;

import com.metamatrix.dqp.message.ResultsMessage;
import com.metamatrix.dqp.message.TestRequestMessage;
import com.metamatrix.query.sql.symbol.ElementSymbol;

import junit.framework.TestCase;

public class TestRequestWorkItem extends TestCase {
	
	public void testResultsMetadata() {
		ElementSymbol e1 = new ElementSymbol("g1.E1"); //name in metadata
		e1.setOutputName("G1.e1"); //name in query
		ResultsMessage message = RequestWorkItem.createResultsMessage(TestRequestMessage.example(), new List[] {}, Arrays.asList(e1), null);
		assertEquals(Arrays.asList("e1"), Arrays.asList(message.getColumnNames()));
	}

}
