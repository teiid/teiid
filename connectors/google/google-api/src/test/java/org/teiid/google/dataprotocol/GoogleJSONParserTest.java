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

package org.teiid.google.dataprotocol;

import static org.junit.Assert.*;

import java.io.StringReader;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.teiid.google.dataprotocol.GoogleJSONParser;
import org.teiid.query.unittest.TimestampUtil;

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
