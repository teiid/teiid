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

package org.teiid.translator.simpledb.visitors;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Command;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.simpledb.SimpleDBSQLVisitor;

@SuppressWarnings("nls")
public class VisitorTest {

	@Test public void testSelect() throws Exception {
		TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table item (\"itemName()\" string, attribute string);", "x", "y");
		TranslationUtility tu = new TranslationUtility(tm);
		
		Command c = tu.parseCommand("select \"itemname()\" from item");
		String result = SimpleDBSQLVisitor.getSQLString(c);
		assertEquals("SELECT itemname() FROM item ", result);
		
		c = tu.parseCommand("select \"itemname()\", attribute from item");
		result = SimpleDBSQLVisitor.getSQLString(c);
		assertEquals("SELECT attribute FROM item ", result);
	}
	
}
