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

package com.metamatrix.common.vdb.api;

import junit.framework.TestCase;

import com.metamatrix.core.vdb.ModelType;
import com.metamatrix.vdb.runtime.BasicVDBInfo;

public class TestManifest extends TestCase {

	public void testLoad() throws Exception {
		Manifest m = new Manifest();
		m.load(TestManifest.class.getResourceAsStream("/VDBManifest-Sample.xmi")); //$NON-NLS-1$x`
		
		BasicVDBInfo vdb = m.getVDB();
		
		// test VDB
		assertEquals("QT_Ora9DSwDEF", vdb.getName());

		
		// test a model
		assertEquals(10, vdb.getModels().size());
		ModelInfo model = vdb.getModel("BQT1");
		assertEquals(ModelType.PHYSICAL, model.getModelType());
		assertEquals("/BQT/BQT1.xmi", model.getPath());
		assertEquals("\"pseudoID\" is excluded from the Document, but is still mapped.", m.getValidityErrors()[0]);
		
	}
}
