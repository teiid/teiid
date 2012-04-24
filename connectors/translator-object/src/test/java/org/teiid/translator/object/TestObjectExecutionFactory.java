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
package org.teiid.translator.object;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.language.Select;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectExecution;
import org.teiid.translator.object.ObjectExecutionFactory;
import org.teiid.translator.object.ObjectSourceProxy;
import org.teiid.translator.object.testdata.VDBUtility;

@SuppressWarnings("nls")
public class TestObjectExecutionFactory {

	@Test public void testFactory() throws Exception {

		Select command = Mockito.mock(Select.class);

		ExecutionContext context = Mockito.mock(ExecutionContext.class);

		ObjectSourceProxy proxy = Mockito.mock(ObjectSourceProxy.class);
		
		ObjectExecutionFactory factory = new ObjectExecutionFactory() {

			@Override
			protected ObjectSourceProxy createProxy(Object connection)
					throws TranslatorException {

				return (ObjectSourceProxy) connection;
			}
			
		};
		
		factory.setColumnNameFirstLetterUpperCase(false);
		
		factory.start();
			
		ObjectExecution exec = (ObjectExecution) factory.createExecution(command, context, VDBUtility.RUNTIME_METADATA, proxy);
		
		assertNotNull(exec);
		assertNotNull(factory.getObjectMethodManager());
		assertEquals(factory.isColumnNameFirstLetterUpperCase(), false);
		
	}
	

  
}
