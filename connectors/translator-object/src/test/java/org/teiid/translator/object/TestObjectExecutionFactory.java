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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.language.Select;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.index.VDBMetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.util.VDBUtility;

@SuppressWarnings("nls")
public class TestObjectExecutionFactory {

	@Test public void testFactory() throws Exception {

		Select command = Mockito.mock(Select.class);

		ExecutionContext context = Mockito.mock(ExecutionContext.class);

		final ObjectSourceProxy proxy = Mockito.mock(ObjectSourceProxy.class);
		
		ObjectExecutionFactory factory = new ObjectExecutionFactory() {

			@Override
			protected ObjectSourceProxy createProxy( Object connection)
					throws TranslatorException {

				return proxy;
			}
			
		};
		
		factory.setColumnNameFirstLetterUpperCase(false);
		
		factory.start();
			
		ObjectExecution exec = (ObjectExecution) factory.createExecution(command, context, VDBUtility.RUNTIME_METADATA, null);
		
		assertNotNull(exec);
		assertNotNull(factory.getObjectMethodManager());
		assertEquals(factory.isColumnNameFirstLetterUpperCase(), false);
		
	}
	
	@Test public void testFactoryLoadingJarClassNames() throws Exception {
	

		Select command = Mockito.mock(Select.class);

		ExecutionContext context = Mockito.mock(ExecutionContext.class);

		final ObjectSourceProxy proxy = Mockito.mock(ObjectSourceProxy.class);
		
		ObjectExecutionFactory factory = new ObjectExecutionFactory() {

			@Override
			protected ObjectSourceProxy createProxy(Object connection)
					throws TranslatorException {

				return proxy;
			}
			
		};
		
		factory.setColumnNameFirstLetterUpperCase(false);
		factory.setPackageNamesOfCachedObjects("org.teiid.translator.object.testdata");
		
		factory.start();
			
		ObjectExecution exec = (ObjectExecution) factory.createExecution(command, context, VDBUtility.RUNTIME_METADATA, null);
		
		assertNotNull(exec);
		assertNotNull(factory.getObjectMethodManager());
		assertEquals(factory.isColumnNameFirstLetterUpperCase(), false);
		
	}	
	
	@Test public void testGetMetadata() throws Exception {
		
		Collection dts = VDBMetadataFactory.getSystem().getDatatypes();
		Map<String, Datatype> mapTypes = new HashMap<String, Datatype>();
		for (Iterator it=  dts.iterator(); it.hasNext();) {
			Datatype dt = (Datatype) it.next();
			mapTypes.put(dt.getName()   , dt);
		}
		
		MetadataFactory mfactory = new MetadataFactory("testModel", mapTypes, new Properties());
		
		final ObjectSourceProxy proxy = Mockito.mock(ObjectSourceProxy.class);
		
		ObjectExecutionFactory factory = new ObjectExecutionFactory() {

			@Override
			protected ObjectSourceProxy createProxy(Object connection)
					throws TranslatorException {

				return proxy;
			}
			
		};
		
		factory.setColumnNameFirstLetterUpperCase(false);
		
		factory.setPackageNamesOfCachedObjects("org.teiid.translator.object.testdata");
		
		factory.start();
		
		factory.getMetadata(mfactory, null);

	}

}
