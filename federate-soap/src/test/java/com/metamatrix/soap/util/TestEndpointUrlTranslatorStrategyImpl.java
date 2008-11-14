/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.soap.util;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import com.metamatrix.core.util.StringUtil;
import com.metamatrix.soap.exceptions.SOAPProcessingException;

import junit.framework.TestCase;

/**
 * @author jpoulsen
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class TestEndpointUrlTranslatorStrategyImpl extends TestCase {
	private static final String urlEncoding = "UTF-8"; //$NON-NLS-1$
	
	public void testCreateJdbcUrlWVDBVersion(){
		String url1=StringUtil.Constants.EMPTY_STRING;
		try {
			url1 = EndpointUriTranslatorStrategyImpl.createJdbcUrl("mm://slntmm05:7000", "DesignTimeCatalog", "1", "txnAutoWrap=PESSIMISTIC");//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		} catch (SOAPProcessingException e) {
			e.printStackTrace();
		}
		assertEquals("jdbc:metamatrix:DesignTimeCatalog@mm://slntmm05:7000;version=1;txnAutoWrap=PESSIMISTIC", url1); //$NON-NLS-1$		
	}
	
	public void testCreateJdbcUrlWVDBVersionWithUrlEncoding(){
		String url1=StringUtil.Constants.EMPTY_STRING;
		try {
			url1 = EndpointUriTranslatorStrategyImpl.createJdbcUrl(URLEncoder.encode("mm://slntmm05:7000",urlEncoding), URLEncoder.encode("Design&TimeCatalog",urlEncoding), "1", URLEncoder.encode("txnAutoWrap=PESSIMISTIC",urlEncoding));//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		} catch (SOAPProcessingException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e){
			e.printStackTrace();
		}
		assertEquals("jdbc:metamatrix:Design&TimeCatalog@mm://slntmm05:7000;version=1;txnAutoWrap=PESSIMISTIC", url1); //$NON-NLS-1$		
	}

}
