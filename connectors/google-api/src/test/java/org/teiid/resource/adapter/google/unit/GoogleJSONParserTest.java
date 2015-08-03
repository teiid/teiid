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

package org.teiid.resource.adapter.google.unit;

import static org.junit.Assert.*;

import java.io.StringReader;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.resource.adapter.google.dataprotocol.GoogleJSONParser;

@SuppressWarnings("nls")
public class GoogleJSONParserTest {

	GoogleJSONParser parser = new GoogleJSONParser();

	@Test public void testDateParsing() throws Exception {
		Date date = (Date)parser.parseObject(new StringReader("new Date(2001,1,2)"), false);
		assertEquals(TimestampUtil.createDate(101, 1, 2), date);
	}
	
	@Test public void testTimestampParsing() throws Exception {
		Timestamp ts = (Timestamp)parser.parseObject(new StringReader("new Date(2001,11,2,5,6,12,100)"), false);
		assertEquals(TimestampUtil.createTimestamp(101, 11, 2, 5, 6, 12, 100000000), ts);
	}
	
	@Test public void testArray() throws Exception {
		List<?> val = (List<?>)parser.parseObject(new StringReader("['a','b','c']"), false);
		assertEquals(Arrays.asList("a", "b", "c"), val);
	}
	
	/**
	 * the last trailing comma is not a null
	 */
	@Test public void testArrayNullValues() throws Exception {
		List<?> val = (List<?>)parser.parseObject(new StringReader("[,\"a\",]"), false);
		assertEquals(Arrays.asList(null, "a"), val);
	}
	
	@Test public void testWrapped() throws Exception {
		Map<?, ?> val = (Map<?, ?>)parser.parseObject(new StringReader("x({\"y\":100, \"z\" : null})"), true);
		Map<Object, Object> expected = new LinkedHashMap<Object, Object>();
		expected.put("y", Double.valueOf(100));
		expected.put("z", null);
		assertEquals(expected, val);
	}
	
	@Test public void testStringEncoding() throws Exception {
		String val = (String)parser.parseObject(new StringReader("'\\u1234\\n\\t'"), false);
		assertEquals("\u1234\n\t", val);
	}
	
}
