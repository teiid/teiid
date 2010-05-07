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

import org.junit.Test;
import org.teiid.adminapi.impl.TranslatorMetaData;
import org.teiid.resource.adapter.BasicExecutionFactory;
import org.teiid.resource.cci.TranslatorProperty;
import static org.junit.Assert.*;

@SuppressWarnings("nls")
public class TestTranslatorDeployer {

	@Test
	public void testBuildExecutionFactory() throws Exception {
		TranslatorMetaData tm = new TranslatorMetaData();
		
		tm.setCapabilitiesClass("capabilities");
		tm.setXaCapable(true);
		tm.addProperty("my-property", "correctly-assigned");
		
		TranslatorDeployer td = new TranslatorDeployer();
		MyTranslator my = (MyTranslator)td.buildTranslator(MyTranslator.class.getName(), tm);
		
		assertEquals("capabilities", my.getCapabilitiesClass());
		assertEquals("correctly-assigned", my.getMyProperty());
	}
	
	public static class MyTranslator extends BasicExecutionFactory {
		String mine;
		
		@TranslatorProperty(name="my-property")
		public String getMyProperty() {
			return mine;
		}
		
		public void setMyProperty(String value) {
			this.mine = value;
		}
	}
}
