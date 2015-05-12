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

package org.teiid.odata;

import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.Test;
import org.odata4j.edm.EdmSimpleType;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.TransformationException;
import org.teiid.translator.odata.ODataTypeManager;

@SuppressWarnings("nls")
public class TestLocalClient {

	@Test public void testLobBinaryTypes() throws TransformationException, SQLException, IOException {
		assertEquals(EdmSimpleType.BINARY, ODataTypeManager.odataType("blob"));
		assertEquals(EdmSimpleType.STRING, ODataTypeManager.odataType("xml"));
		assertEquals(EdmSimpleType.BINARY, ODataTypeManager.odataType("varbinary"));
		LocalClient.buildPropery("x", EdmSimpleType.BINARY, new byte[] {-1}, null);
		LocalClient.buildPropery("x", EdmSimpleType.STRING, new ClobImpl("foo"), null).getValue().equals("foo");
		LocalClient.buildPropery("x", EdmSimpleType.BINARY, new BlobType(new byte[] {-1}), null);
		LocalClient.buildPropery("x", EdmSimpleType.STRING, new SQLXMLImpl("</a>"), null).getValue().equals("</a>");
	}
	
}
