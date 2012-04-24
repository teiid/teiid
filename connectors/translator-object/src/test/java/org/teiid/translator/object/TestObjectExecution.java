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

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.language.Select;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectExecution;
import org.teiid.translator.object.ObjectExecutionFactory;
import org.teiid.translator.object.ObjectSourceProxy;
import org.teiid.translator.object.testdata.TradesCacheSource;
import org.teiid.translator.object.testdata.VDBUtility;

@SuppressWarnings("nls")
public class TestObjectExecution {
	
	private static TradesCacheSource source;
	
	@BeforeClass
    public static void beforeEach() throws Exception {        
		source = TradesCacheSource.loadCache();
    }
	

	@Test public void testQueryRootObject() throws Exception {
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade"); //$NON-NLS-1$
		this.runCommand(command, 3);
	
	}
	
	@Test public void testQueryIncludeLegs() throws Exception {		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName From Trade_Object.Trade as T, Trade_Object.Leg as L Where T.TradeId = L.TradeId"); //$NON-NLS-1$
		runCommand(command, 30);

	}	
	
	private void runCommand(Select command, int expected) throws Exception {

		ExecutionContext context = Mockito.mock(ExecutionContext.class);

		ObjectSourceProxy proxy = Mockito.mock(ObjectSourceProxy.class);
		
		Mockito.stub(proxy.get(command)).toReturn(source.getAll());

		
		ObjectExecutionFactory factory = new ObjectExecutionFactory() {

			@Override
			protected ObjectSourceProxy createProxy(Object connection)
					throws TranslatorException {

				return (ObjectSourceProxy) connection;
			}
			
		};
				
		factory.start();
			
		ObjectExecution exec = (ObjectExecution) factory.createExecution(command, context, VDBUtility.RUNTIME_METADATA, proxy);
		
		exec.execute();
		
		int cnt = 0;
		List<?> row = exec.next();
		
		while (row != null) {
			++cnt;
			row = exec.next();
		}
		assertEquals("Did not get expected number of rows", expected, cnt); //$NON-NLS-1$
		     
		exec.close();
		
	}
	
}
