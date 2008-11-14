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

import junit.framework.TestCase;

import com.metamatrix.soap.exceptions.SOAPProcessingException;

public class TestWebServiceUtil extends TestCase {

	private static String ERROR = "The SOAP Action and WSAW Action are null or empty. Please format the action as follows: VDBName=MyVDB&ServerURL=mm://mmHost:mmPort&VDBVersion=<optional>&AdditionalProperties=<optional>&procedure=fully.qualified.procedureName"; //$NON-NLS-1$

	public void testValidateCorrectAction() {

		try {
			WebServiceUtil.validateActionIsSet("VDBName=MyVDB&ServerURL=mm://mmHost:mmPort&VDBVersion=<optional>&AdditionalProperties=<optional>&procedure=fully.qualified.procedureName");//$NON-NLS-1$ 
		} catch (SOAPProcessingException e) {
			e.printStackTrace();
		}				
	}

	public void testValidateNullAction() {
		try {
			WebServiceUtil.validateActionIsSet(null);
		} catch (SOAPProcessingException e) {
			assertEquals(ERROR, e.getMessage());
		}
	}

	public void testValidateEmptyAction() {
		try {
			WebServiceUtil.validateActionIsSet("");//$NON-NLS-1$
		} catch (SOAPProcessingException e) {
			assertEquals(ERROR, e.getMessage());
		}
	}
	
	public void testValidateEmptySpacesAction() {
		try {
			WebServiceUtil.validateActionIsSet(" ");//$NON-NLS-1$
		} catch (SOAPProcessingException e) {
			assertEquals(ERROR, e.getMessage());
		}
	}
}
