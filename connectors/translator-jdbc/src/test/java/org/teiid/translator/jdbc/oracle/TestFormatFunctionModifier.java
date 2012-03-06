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

package org.teiid.translator.jdbc.oracle;

import static org.junit.Assert.*;

import org.junit.Test;

@SuppressWarnings("nls")
public class TestFormatFunctionModifier {

	OracleFormatFunctionModifier offm = new OracleFormatFunctionModifier("TO_CHAR(");
    
    public void helpTest(String expected, String format) {
    	assertTrue(OracleFormatFunctionModifier.supportsLiteral(format));
        assertEquals(expected, offm.translateFormat(format));
    }
    
    @Test public void testQuoting() {
    	helpTest("\"a'\"\"123\"", "'a'''123");
    }
    
    @Test public void testDay() {
    	helpTest("DD DDD", "d D");
    }
    
    @Test public void testYear() {
    	helpTest("YY YY YY YYYY", "y yy yyy yyyy");
    }
    
    @Test public void testMonth() {
    	helpTest("MM MM Mon Month", "M MM MMM MMMM");
    }
    
    @Test public void testISO() {
    	helpTest("YYYY-MM-DD\"T\"HH24:MI:SS.FF3", "yyyy-MM-dd'T'HH:mm:ss.SSS");
    }
    
    @Test public void testSupports() {
    	assertFalse(OracleFormatFunctionModifier.supportsLiteral("\""));
    	assertFalse(OracleFormatFunctionModifier.supportsLiteral("'"));
    	assertFalse(OracleFormatFunctionModifier.supportsLiteral("x"));
    	assertFalse(OracleFormatFunctionModifier.supportsLiteral("-ax"));
    }

}
