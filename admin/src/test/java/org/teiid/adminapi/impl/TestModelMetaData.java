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

package org.teiid.adminapi.impl;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.adminapi.Model;

@SuppressWarnings("nls")
public class TestModelMetaData {

	@Test
	public void testModelType() {
		
		ModelMetaData model = new ModelMetaData();
		model.modelType = "physical";
		
		assertTrue(model.getModelType() == Model.Type.PHYSICAL);
		assertTrue(model.isSource());
		
		model.modelType = "VIRTUAL";
		assertTrue(model.getModelType() == Model.Type.VIRTUAL);
		
		model.modelType = "TYPE";
		assertTrue(model.getModelType() == Model.Type.OTHER);
		assertTrue(!model.isSource());
	}
	
	@Test
	public void testSupportMultiSource() {
		ModelMetaData model = new ModelMetaData();
		assertFalse(model.isSupportsMultiSourceBindings());
		model.setSupportsMultiSourceBindings(true);
		
		assertTrue(model.isSupportsMultiSourceBindings());
		
		assertTrue(!model.getProperties().isEmpty());
	}
	
	public void testErrors() {
		ModelMetaData m = new ModelMetaData();
		m.addError("ERROR", "I am Error");
		m.addError("WARNING", "I am warning");
		
		assertFalse(m.getErrors().isEmpty());
		assertEquals(1, m.getErrors().size());
	}
}
