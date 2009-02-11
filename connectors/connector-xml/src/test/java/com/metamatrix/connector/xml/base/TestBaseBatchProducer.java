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

package com.metamatrix.connector.xml.base;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.mockito.Mockito;

import com.metamatrix.cdk.api.EnvironmentUtility;

public class TestBaseBatchProducer extends TestCase {
	
	public void testIteration() throws Exception {
		List<List<LargeOrSmallString>> results = new ArrayList<List<LargeOrSmallString>>();
		results.add(new ArrayList(Arrays.asList(LargeOrSmallString.createSmallString("1"), LargeOrSmallString.createSmallString("2")))); //$NON-NLS-1$ //$NON-NLS-2$
		results.add(new ArrayList(Arrays.asList(LargeOrSmallString.createSmallString("3"), LargeOrSmallString.createSmallString("4")))); //$NON-NLS-1$ //$NON-NLS-2$

		ExecutionInfo info = new ExecutionInfo();
		info.setColumnCount(2);
		OutputXPathDesc desc = Mockito.mock(OutputXPathDesc.class);
		Mockito.stub(desc.getDataType()).toReturn(String.class);
		info.setRequestedColumns(Arrays.asList(desc, desc));
		BaseBatchProducer baseBatchProducer = new BaseBatchProducer(results, info, null, EnvironmentUtility.createEnvironment(new Properties()));
		
		List row = baseBatchProducer.createRow();
		assertEquals(Arrays.asList("1", "3"), row); //$NON-NLS-1$ //$NON-NLS-2$
		assertNotNull(baseBatchProducer.createRow());
		assertNull(baseBatchProducer.createRow());
	}

}
