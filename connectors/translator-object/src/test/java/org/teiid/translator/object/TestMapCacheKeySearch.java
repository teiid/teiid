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

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.naming.Context;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.teiid.language.Select;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.util.TradesCacheSource;
import org.teiid.translator.object.util.VDBUtility;


@SuppressWarnings("nls")
public class TestMapCacheKeySearch extends BasicSearchTest {	      

	protected static final String JNDI_NAME = "java/MyCacheManager";
	   
	private static TradesCacheSource source  = TradesCacheSource.loadCache();
	private static ExecutionContext context;
	
	private ObjectExecutionFactory factory = null;
	
	@Mock
	private static Context jndi;

	protected static boolean print = false;
	
	@BeforeClass
    public static void beforeEachClass() throws Exception {  
	    
		context = mock(ExecutionContext.class);
		
        // Set up the mock JNDI ...
		jndi = mock(Context.class);
        when(jndi.lookup(anyString())).thenReturn(null);
        when(jndi.lookup(JNDI_NAME)).thenReturn(source);

	}
	
	@Before public void beforeEach() throws Exception{	
		 
		factory = new ObjectExecutionFactory();

		factory.start();

    }
	
	@Override
	protected ObjectExecution createExecution(Select command) throws TranslatorException {
		return (ObjectExecution) factory.createExecution(command, context, VDBUtility.RUNTIME_METADATA, source);
	}

	@Ignore
	@Test public void testGetMetadata() throws Exception {
		
		Map<String, Datatype> dts = SystemMetadata.getInstance().getSystemStore().getDatatypes();

		MetadataFactory mfactory = new MetadataFactory("TestVDB", 1, "Trade",  dts, new Properties(), null);
		
		factory.getMetadata(mfactory, null);
		
		Map<String, Table> tables = mfactory.getSchema().getTables();
		for (Iterator<Table> it=tables.values().iterator(); it.hasNext();) {
			Table t = it.next();
		}

	}

}
