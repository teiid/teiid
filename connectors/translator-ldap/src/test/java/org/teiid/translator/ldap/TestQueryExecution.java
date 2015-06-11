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

package org.teiid.translator.ldap;

import static org.junit.Assert.*;

import java.util.Collections;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;

import org.junit.Test;
import org.teiid.language.Literal;
import org.teiid.metadata.Column;

@SuppressWarnings("nls")
public class TestQueryExecution {
	
	@Test public void testMultiAttribute() throws NamingException {
		Column c = new Column();
		c.setDefaultValue(LDAPQueryExecution.MULTIVALUED_CONCAT);
		Attribute a = LDAPUpdateExecution.createBasicAttribute("x", new Literal("a?b?c", String.class), c);
		assertEquals(3, a.size());
		assertEquals("b", Collections.list(a.getAll()).get(1));
	}
	
}
