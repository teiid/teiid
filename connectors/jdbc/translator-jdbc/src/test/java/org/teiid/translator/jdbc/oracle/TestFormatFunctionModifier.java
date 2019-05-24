/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.translator.jdbc.oracle;

import static org.junit.Assert.*;

import org.junit.Test;

@SuppressWarnings("nls")
public class TestFormatFunctionModifier {

    OracleFormatFunctionModifier offm = new OracleFormatFunctionModifier("TO_CHAR(", false);

    public void helpTest(String expected, String format) {
        assertTrue(offm.supportsLiteral(format));
        assertEquals(expected, offm.translateFormat(format));
    }

    @Test public void testQuoting() {
        helpTest("'\"a\"\"123\"'", "'a'123");
        helpTest("'\"a''\"\"123\"'", "'a'''123");
    }

    @Test public void testParseHour() {
        OracleFormatFunctionModifier offmFormat = new OracleFormatFunctionModifier("TO_TIMESTAMP(", true);
        String format = "hh:mm:ss";
        String expected = "'HH24:MI:SS'";
        assertTrue(offmFormat.supportsLiteral(format));
        assertEquals(expected, offmFormat.translateFormat(format));
    }

    @Test public void testDay() {
        helpTest("'DD DDD'", "dd DD");
    }

    @Test public void testYear() {
        helpTest("'YYYY YY YYYY YYYY'", "y yy yyy yyyy");
    }

    @Test public void testMonth() {
        helpTest("'MM Mon Month'", "MM MMM MMMM");
    }

    @Test public void testEra() {
        helpTest("'AD'", "GG");
    }

    @Test public void testAmPm() {
        helpTest("'AM'", "aa");
    }

    @Test public void testISO() {
        helpTest("'YYYY-MM-DD\"T\"HH24:MI:SS.FF3'", "yyyy-MM-dd'T'HH:mm:ss.SSS");
    }

    @Test public void testSupports() {
        assertTrue(offm.supportsLiteral("MMM"));
        assertFalse(offm.supportsLiteral("yyyyy"));
        assertFalse(offm.supportsLiteral("h"));
        assertFalse(offm.supportsLiteral("H"));
        assertFalse(offm.supportsLiteral("M"));
        assertFalse(offm.supportsLiteral("\""));
        assertFalse(offm.supportsLiteral("'"));
        assertFalse(offm.supportsLiteral("x"));
        assertFalse(offm.supportsLiteral("-ax"));
        assertFalse(offm.supportsLiteral("k"));
        assertFalse(offm.supportsLiteral("F MMM-dd-yyyy G"));
        assertFalse(offm.supportsLiteral("MMM-dd-yyyy G u "));
    }

}
