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

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.naming.Context;

import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.teiid.core.util.StringUtil;
import org.teiid.language.Select;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.metadata.BaseMetadataProcessor;
import org.teiid.translator.object.testdata.Trade;
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
	
	@Test public void testGetMetadata() throws Exception {
		
		Map<String, Datatype> dts = SystemMetadata.getInstance().getSystemStore().getDatatypes();

		MetadataFactory mfactory = new MetadataFactory("TestVDB", 1, "Trade",  dts, new Properties(), null);
		
		factory.getMetadata(mfactory, source);
		
		assertEquals(mfactory.getSchema().getName(), BaseMetadataProcessor.SCHEMA_NAME);
		
		String clzName = Trade.class.getName();
		clzName = clzName.substring(clzName.lastIndexOf(".") + 1);

		Table physicalTable = mfactory.getSchema().getTable(clzName);
		assertNotNull(physicalTable);
		assertTrue(physicalTable.isPhysical());
		assertTrue(!physicalTable.isVirtual());

		
		String virClzName = clzName + BaseMetadataProcessor.VIEWTABLE_SUFFIX;

		Table virtualTable = mfactory.getSchema().getTable(virClzName);
		assertNotNull(virtualTable);
		assertTrue(virtualTable.isVirtual());
		assertTrue(!virtualTable.isPhysical());
		
//		transform = "SELECT o.Name, o.TradeId, o.TradeDate, o.Settled FROM Trade as T," +
//		" OBJECTTABLE('x' PASSING T.TradeObject AS x COLUMNS Name string 'teiid_row.Name'," + 
//		" TradeId long 'teiid_row.TradeId', TradeDate timestamp 'teiid_row.TradeDate'," + 
//		" Settled boolean 'teiid_row.Settled') as o;";

		
		//  used the following to validate the transform because the class methods are not 
		//		guaranteed to be processed in the same order, 
		//		thereby, the elements in transform statement can be arranged differently each time
		String transform = virtualTable.getSelectTransformation();	
		
		int idx = transform.indexOf("OBJECTTABLE");
		
		String select = transform.substring(0, idx);
		String objecttable = transform.substring(idx);
		assertTrue(select.indexOf("o.Name") >-1);
		assertTrue(select.indexOf("o.TradeId") >-1);
		assertTrue(select.indexOf("o.TradeDate") >-1);
		assertTrue(select.indexOf("o.Settled") >-1);
		assertTrue(select.indexOf("FROM Trade as T") >-1);

		assertTrue(objecttable.indexOf("'x' PASSING T.TradeObject AS x COLUMNS") >-1);
		assertTrue(objecttable.indexOf("Name string 'teiid_row.Name'") >-1);
		assertTrue(objecttable.indexOf("TradeId long 'teiid_row.TradeId'") >-1);
		assertTrue(objecttable.indexOf("TradeDate timestamp 'teiid_row.TradeDate'") >-1);
		assertTrue(objecttable.indexOf("Settled boolean 'teiid_row.Settled'") >-1);


	}

}
