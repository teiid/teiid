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

package org.teiid.query.sql.lang;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.sql.proc.TriggerAction;

@SuppressWarnings("nls")
public class TestIsDistinctCriteria {

	@Test public void testParseClone() throws QueryParserException {
		TriggerAction ta = (TriggerAction)QueryParser.getQueryParser().parseProcedure("for each row begin atomic if (\"new\" is not distinct from \"old\") raise sqlexception ''; end", true);
		assertEquals("FOR EACH ROW\nBEGIN ATOMIC\nIF(\"new\" IS NOT DISTINCT FROM \"old\")\nBEGIN\nRAISE SQLEXCEPTION '';\nEND\nEND", ta.toString());
		QueryParser.getQueryParser().parseProcedure(ta.toString(), true);
		TriggerAction clone = ta.clone();
		assertEquals(ta.toString(), clone.toString());
		UnitTestUtil.helpTestEquivalence(0, ta, ta.clone());
	}

}
