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

package org.teiid.translator.xml.file;

import java.util.List;

import javax.resource.ResourceException;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.xml.XMLExecutionFactory;
import org.teiid.translator.xml.file.TestCachingFileConnectorLong.FileImpl;

import com.metamatrix.cdk.api.ConnectorHost;

@SuppressWarnings("nls")
public class TestFileConnector extends TestCase {

	public void testSelect() throws Exception{
		
		XMLExecutionFactory factory  = new XMLExecutionFactory();
		BasicConnectionFactory cf = new BasicConnectionFactory() {
			@Override
			public BasicConnection getConnection() throws ResourceException {				
				return new FileImpl(UnitTestUtil.getTestDataPath()+"/documents/purchaseOrdersShort.xml");
			}
			
		};
		
		String vdbPath = UnitTestUtil.getTestDataPath()+"/documents/purchase_orders.vdb";
		ConnectorHost host = new ConnectorHost(factory, cf, vdbPath);
		ExecutionContext context = Mockito.mock(ExecutionContext.class);
		host.setExecutionContext(context);
		
		List result = host.executeCommand("SELECT * FROM file_po_list.ITEM");
		assertEquals(2, result.size());
	}
}
