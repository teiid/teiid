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

import java.io.File;
import java.util.List;

import javax.resource.ResourceException;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.FileConnection;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.xml.XMLExecutionFactory;

import com.metamatrix.cdk.api.ConnectorHost;

@SuppressWarnings("nls")
public class TestCachingFileConnectorLong extends TestCase {

	ConnectorHost host;
	
	@Override
	public void setUp() throws Exception {
		XMLExecutionFactory factory  = new XMLExecutionFactory();
		BasicConnectionFactory cf = new BasicConnectionFactory() {
			@Override
			public BasicConnection getConnection() throws ResourceException {				
				return new FileImpl(UnitTestUtil.getTestDataPath()+"/documents/purchaseOrders.xml");
			}
			
		};
		
		String vdbPath = UnitTestUtil.getTestDataPath()+"/documents/purchase_orders.vdb";
		host = new ConnectorHost(factory, cf, vdbPath);
		ExecutionContext context = Mockito.mock(ExecutionContext.class);
		host.setExecutionContext(context);
	}
	
	/**
	 * This primes the cache with the response docs, then gets them from the cache
	 * @throws TranslatorException 
	 */
	public void testSelectFromCache() throws TranslatorException {
		List result = host.executeCommand("SELECT * FROM file_po_list.ITEM");
		assertEquals(5968, result.size());
		
		result = host.executeCommand("SELECT * FROM file_po_list.ITEM");
		assertEquals(5968, result.size());
	}

    static class FileImpl extends BasicConnection implements FileConnection{
    	File file;
    	public FileImpl(String file) {
    		this.file = new File(file);
    	}
    	
    	@Override
    	public File getFile(String path) {
    		if (path == null) {
        		return this.file;
            }
        	return new File(file, path);
    	}

		@Override
		public void close() throws ResourceException {
		}
    }
    
}
