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

package org.teiid.metadata;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.HashMap;

import org.junit.Test;
import org.teiid.CommandContext;
import org.teiid.UserDefinedAggregate;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.metadata.AbstractMetadataRecord.DataModifiable;

@SuppressWarnings("nls")
public class TestMetadataFactory {

	@Test public void testSchemaProperties() {
		ModelMetaData mmd = new ModelMetaData();
		mmd.setName("foo");
		mmd.addProperty("teiid_rel:data-ttl", "1");
		MetadataFactory mf = new MetadataFactory("x", 1, Collections.EMPTY_MAP, mmd);
		Schema s = mf.getSchema();
		assertEquals("foo", s.getName());
		String val = s.getProperty(DataModifiable.DATA_TTL, false);
		assertEquals("1", val);
	}
	
	@Test public void testCreateFunction() throws NoSuchMethodException, SecurityException {
		FunctionMethod fm = MetadataFactory.createFunctionFromMethod("x", TestMetadataFactory.class.getMethod("someFunction"));
		assertEquals(Boolean.class, fm.getOutputParameter().getJavaType());
		
		fm = MetadataFactory.createFunctionFromMethod("x", TestMetadataFactory.class.getMethod("someArrayFunction"));
		assertEquals(String[].class, fm.getOutputParameter().getJavaType());
	}
	
	@Test public void testCreateAggregateFunction() throws NoSuchMethodException, SecurityException {
		FunctionMethod fm = MetadataFactory.createFunctionFromMethod("x", MyUDAF.class.getMethod("addInput", String.class));
		assertEquals(Boolean.class, fm.getOutputParameter().getJavaType());
		assertNotNull(fm.getAggregateAttributes());
	}
	
	@Test public void testCorrectName() {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("foo");
        HashMap<String, Datatype> types = new HashMap<String, Datatype>();
        Datatype value = new Datatype();
        value.setName("string");
        types.put("string", value);
        MetadataFactory factory = new MetadataFactory("x", 1, types, mmd);
        Table x = factory.addTable("x");
        Column c = factory.addColumn("a.b", "string", x);
        assertEquals("a_b", c.getName());
    }
	
	public static boolean someFunction() {
		return true;
	}
	
	public static String[] someArrayFunction() {
		return null;
	}
	
	public static class MyUDAF implements UserDefinedAggregate<Boolean> {
		@Override
		public Boolean getResult(CommandContext commandContext) {
			return null;
		}
		
		@Override
		public void reset() {
			
		}
		
		public void addInput(String val) {
			
		}
	}
	
}
