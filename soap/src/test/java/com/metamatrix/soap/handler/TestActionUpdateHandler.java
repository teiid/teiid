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

package com.metamatrix.soap.handler;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.axis2.context.MessageContext;

import com.metamatrix.soap.util.WSDLServletUtil;

public class TestActionUpdateHandler extends TestCase{
	
	public void testgetServerPropertyMap1() {

		Map expectedMap = new HashMap();
		expectedMap.put(WSDLServletUtil.VDB_NAME_KEY, "BooksWsdl"); //$NON-NLS-1$	
		expectedMap.put(WSDLServletUtil.VDB_VERSION_KEY, "1"); //$NON-NLS-1$	
		expectedMap.put(WSDLServletUtil.ADD_PROPS, null);
		expectedMap.put(ActionUpdateHandler.VIRTUAL_PROCEDURE, "BooksWebService.Books.getBookCollection"); //$NON-NLS-1$	
		
		MessageContext messageContext = new MessageContext();
		messageContext.getOptions().setAction("VDBName=BooksWsdl&VDBVersion=1&AdditionalProperties=&procedure=BooksWebService.Books.getBookCollection"); //$NON-NLS-1$	
		Map actualMap = ActionUpdateHandler.getServerPropertyMap(null, messageContext);
		
		assertEquals(expectedMap.get(WSDLServletUtil.VDB_NAME_KEY), actualMap.get(WSDLServletUtil.VDB_NAME_KEY));
		assertEquals(expectedMap.get(WSDLServletUtil.VDB_VERSION_KEY), actualMap.get(WSDLServletUtil.VDB_VERSION_KEY));
		assertEquals(expectedMap.get(WSDLServletUtil.ADD_PROPS), actualMap.get(WSDLServletUtil.ADD_PROPS));
		assertEquals(expectedMap.get(ActionUpdateHandler.VIRTUAL_PROCEDURE), actualMap.get(ActionUpdateHandler.VIRTUAL_PROCEDURE));
	}	
	
	
}
