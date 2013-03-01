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
package org.teiid.deployers;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorProperty;

@SuppressWarnings("nls")
public class TestTranslatorUtil {

	@Test
	public void testBuildExecutionFactory() throws Exception {
		VDBTranslatorMetaData tm = new VDBTranslatorMetaData();
		
		tm.addProperty("MyProperty", "correctly-assigned");
		tm.setExecutionFactoryClass(MyTranslator.class);
		MyTranslator my = (MyTranslator)TranslatorUtil.buildExecutionFactory(tm, this.getClass().getClassLoader());
		
		assertEquals("correctly-assigned", my.getMyProperty());
	}
	
	@Test public void testBuildExecutionFactoryCaseInsensitive() throws Exception {
		VDBTranslatorMetaData tm = new VDBTranslatorMetaData();
		
		tm.addProperty("myproperty", "correctly-assigned");
		tm.setExecutionFactoryClass(MyTranslator.class);
		MyTranslator my = (MyTranslator)TranslatorUtil.buildExecutionFactory(tm, this.getClass().getClassLoader());
		
		assertEquals("correctly-assigned", my.getMyProperty());
	}
	
	@Test public void testBuildExecutionFactory1() throws Exception {
		VDBTranslatorMetaData tm = new VDBTranslatorMetaData();
		
		tm.addProperty("someproperty", "correctly-assigned");
		tm.setExecutionFactoryClass(MyTranslator1.class);
		MyTranslator1 my = (MyTranslator1)TranslatorUtil.buildExecutionFactory(tm, this.getClass().getClassLoader());
		
		assertNull(my.getMyProperty());
		assertEquals("correctly-assigned", my.getSomeProperty());
	}
	
	@Translator(name="my-translator")
	public static class MyTranslator extends ExecutionFactory<Object, Object> {
		String mine;
		
		@TranslatorProperty(display="my-property", required=true)
		public String getMyProperty() {
			return mine;
		}
		
		public void setMyProperty(String value) {
			this.mine = value;
		}
	}
	
	public interface SomeProperty {
		
		@TranslatorProperty(display="my-property", required=false)
		String getSomeProperty();
		
		void setSomeProperty(String value);
		
	}
	
	@Translator(name="my-translator1")
	public static class MyTranslator1 extends MyTranslator implements SomeProperty {
		
		private String someProperty;
		
		@TranslatorProperty(display="my-property", required=false)
		public String getMyProperty() {
			return super.getMyProperty();
		}
		
		@Override
		public String getSomeProperty() {
			return someProperty;
		}
		
		@Override
		public void setSomeProperty(String value) {
			this.someProperty = value;
		}
		
	}
}
