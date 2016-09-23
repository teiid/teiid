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

package org.teiid.translator;

import static org.junit.Assert.*;

import java.lang.reflect.Method;

import org.junit.Test;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;

public class TestBaseDelegatingExecutionFactory {

	@Test public void testMethodOverrides() {
		Method[] methods = ExecutionFactory.class.getDeclaredMethods();
		Method[] proxyMethods = BaseDelegatingExecutionFactory.class.getDeclaredMethods();
		//excluding the setter methods the counts should be equal
		assertEquals(methods.length+78, proxyMethods.length);
	}
	
	@Test public void testExecution() throws TranslatorException {
		BaseDelegatingExecutionFactory<Void, Void> ef = new BaseDelegatingExecutionFactory<Void, Void>() {
			@Override
			public ResultSetExecution createResultSetExecution(
					QueryExpression command, ExecutionContext executionContext,
					RuntimeMetadata metadata, Void connection)
					throws TranslatorException {
				return null;
			}
		};
		ef.setDelegate(new ExecutionFactory<Void, Void>() {
			@Override
			public Execution createExecution(Command command,
					ExecutionContext executionContext,
					RuntimeMetadata metadata, Void connection)
					throws TranslatorException {
				throw new AssertionError();
			}
		});
		ef.createExecution(new Select(null, false, null, null, null, null, null), null, null, null);
	}
	
}
