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

package org.teiid.odbc;

import static org.junit.Assert.*;

import org.junit.Test;

@SuppressWarnings("nls")
public class TestScriptReader {

    @Test public void testRewrite() throws Exception {
        ScriptReader sr = new ScriptReader("select 'a'::b from foo");
        sr.setRewrite(true);
        String result = sr.readStatement();
        assertEquals("select cast('a' AS b) from foo", result);
    }

    @Test public void testRewriteComplexLiteral() throws Exception {
        ScriptReader sr = new ScriptReader("select 'a''c'::b");
        sr.setRewrite(true);
        String result = sr.readStatement();
        assertEquals("select cast('a''c' AS b)", result);
    }

    @Test public void testRewrite1() throws Exception {
        ScriptReader sr = new ScriptReader("select a~b, a!~~c from foo");
        sr.setRewrite(true);
        String result = sr.readStatement();
        assertEquals("select a LIKE_REGEX b, a NOT LIKE c from foo", result);
    }

    @Test public void testRewrite2() throws Exception {
        ScriptReader sr = new ScriptReader("select a~");
        sr.setRewrite(true);
        String result = sr.readStatement();
        assertEquals("select a LIKE_REGEX ", result);
    }

    @Test public void testRewrite3() throws Exception {
        ScriptReader sr = new ScriptReader("select a::b");
        sr.setRewrite(true);
        String result = sr.readStatement();
        assertEquals("select a", result);
    }

    @Test public void testDelimited() throws Exception {
        ScriptReader sr = new ScriptReader("set foo 'bar'; set foo1 'bar1'");
        String result = sr.readStatement();
        assertEquals("set foo 'bar'", result);
        result = sr.readStatement();
        assertEquals(" set foo1 'bar1'", result);
    }

    @Test public void testRegClassCast() throws Exception {
        ScriptReader sr = new ScriptReader("where oid='\"a\"'::regclass");
        sr.setRewrite(true);
        String result = sr.readStatement();
        assertEquals("where oid=regclass('\"a\"')", result);
    }

    @Test public void testExtraDelim() throws Exception {
        ScriptReader sr = new ScriptReader("BEGIN;declare \"SQL_CUR0x1e4ba50\" cursor with hold for select * from pg_proc;;fetch 1 in \"SQL_CUR0x1e4ba50\"");
        String result = sr.readStatement();
        assertEquals("BEGIN", result);
        result = sr.readStatement();
        assertEquals("declare \"SQL_CUR0x1e4ba50\" cursor with hold for select * from pg_proc", result);
        result = sr.readStatement();
        assertEquals("fetch 1 in \"SQL_CUR0x1e4ba50\"", result);
        assertNull(sr.readStatement());
    }

    @Test public void testFunctionRewrite() throws Exception {
        ScriptReader sr = new ScriptReader("select textcat('a', 'b'), textcat+('a', 'b')");
        sr.setRewrite(true);
        String result = sr.readStatement();
        assertEquals("select concat('a', 'b'), textcat+('a', 'b')", result);
    }

    @Test public void testFunctionRewrite2() throws Exception {
        ScriptReader sr = new ScriptReader("select \"ltrunc('a')\", ltrunc('a')");
        sr.setRewrite(true);
        String result = sr.readStatement();
        assertEquals("select \"ltrunc('a')\", left('a')", result);
    }

    @Test public void testRegProc() throws Exception {
        ScriptReader sr = new ScriptReader("SELECT 1 FROM pg_catalog.pg_type WHERE typname = $1 AND typinput='array_in'::regproc");
        sr.setRewrite(true);
        String result = sr.readStatement();
        assertEquals("SELECT 1 FROM pg_catalog.pg_type WHERE typname = $1 AND typinput=(SELECT oid FROM pg_catalog.pg_proc WHERE upper(proname) = 'ARRAY_IN')", result);
    }

}
