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
package org.teiid.olingo;

import static org.junit.Assert.assertEquals;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.junit.Test;

@SuppressWarnings("nls")
public class TestLiteralParser {

    @Test
    public void test() throws Exception {
        assertEquals(4, LiteralParser.parseLiteral("4"));
        assertEquals(4.5, LiteralParser.parseLiteral("4.5"));
        assertEquals(true, LiteralParser.parseLiteral("true"));
        assertEquals(false, LiteralParser.parseLiteral("FALSE"));
        assertEquals(null, LiteralParser.parseLiteral("null"));
        assertEquals("hello", LiteralParser.parseLiteral("'hello'"));
        assertEquals(Date.valueOf("2008-01-10"), LiteralParser.parseLiteral("2008-01-10"));
        assertEquals(Time.valueOf("13:20:00"), LiteralParser.parseLiteral("13:20:00"));
        assertEquals(new Timestamp(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse("2008-10-13T01:02:03").getTime()), LiteralParser.parseLiteral("2008-10-13T01:02:03"));
        assertEquals(new Timestamp(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse("2008-10-13T00:00:00").getTime()), LiteralParser.parseLiteral("2008-10-13T00:00:00"));
        assertEquals(new Timestamp(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse("2008-10-13T01:02:03.222").getTime()), LiteralParser.parseLiteral("2008-10-13T01:02:03.2222"));
        //assertEquals(new Timestamp(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ").parse("2008-10-13T01:02:03.222Z").getTime()), LiteralParser.parseLiteral("2008-10-13T01:02:03.222+02:00"));
    }

}
