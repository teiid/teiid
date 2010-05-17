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

import java.util.Map;

import org.jboss.managed.api.ManagedProperty;
import org.jboss.metatype.api.values.MetaValueFactory;
import org.junit.Test;
import org.teiid.deployers.TestManagedPropertyUtil.FakeExecutionFactory.PropEnum;
import org.teiid.templates.TranslatorTemplateInfoFactory;
import org.teiid.translator.TranslatorProperty;

@SuppressWarnings("nls")
public class TestManagedPropertyUtil {

	public static class FakeExecutionFactory {
		
		public enum PropEnum {
			A, B, C
		}
		
		private PropEnum val = PropEnum.A;
		
		@TranslatorProperty(display="Read Only")
		public int readonly() {
			return 1;
		}
		
		@TranslatorProperty(display="Setter Property")
		public void x(@SuppressWarnings("unused") String y) {
			
		}

		@TranslatorProperty(display="Enum Property")
		public PropEnum getEnum() {
			return val;
		}
		
		public void setEnum(PropEnum value) {
			this.val = value;
		}
		
	}

	@Test public void testAnnotationProcessing() throws Exception {
		FakeExecutionFactory ef = new FakeExecutionFactory();
		
		Map<String, ManagedProperty> properties = TranslatorTemplateInfoFactory.getProperties(ef.getClass());
		ManagedProperty mp = properties.get("readonly");
		assertTrue(mp.isReadOnly());
		assertEquals(Integer.valueOf(1), MetaValueFactory.getInstance().unwrap(mp.getDefaultValue()));
		
		mp = properties.get("x");
		assertFalse(mp.isReadOnly());
		assertNull(mp.getDefaultValue());
		
		mp = properties.get("Enum");
		assertFalse(mp.isReadOnly());
		assertEquals(PropEnum.A.name(), MetaValueFactory.getInstance().unwrap(mp.getDefaultValue()));
	}
	
}
