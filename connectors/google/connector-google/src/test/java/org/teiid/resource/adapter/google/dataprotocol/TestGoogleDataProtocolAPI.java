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

package org.teiid.resource.adapter.google.dataprotocol;

import static org.junit.Assert.*;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;

import org.junit.Test;
import org.teiid.translator.google.api.metadata.SpreadsheetColumnType;

@SuppressWarnings("nls")
public class TestGoogleDataProtocolAPI {

	@Test public void testValueConversion() {
		Date date = (Date)GoogleDataProtocolAPI.convertValue(null, "Date(2001,0,1)", SpreadsheetColumnType.DATE);
		assertEquals("2001-01-01", date.toString());
		Timestamp ts = (Timestamp)GoogleDataProtocolAPI.convertValue(null, "Date(2001,0,1,1,2,3)", SpreadsheetColumnType.DATETIME);
		assertEquals("2001-01-01 01:02:03.0", ts.toString());
		Time t = (Time)GoogleDataProtocolAPI.convertValue(null, Arrays.asList(1.0, 2.0, 3.0, 4.0), SpreadsheetColumnType.TIMEOFDAY);
		assertEquals("01:02:03", t.toString());
	}
	
	
}
