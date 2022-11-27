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

package org.teiid.client.util;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.core.TeiidException;
import org.teiid.jdbc.JDBCPlugin;

@SuppressWarnings("nls")
public class TestExceptionUtil {

    @Test public void testSanitize() {
        TeiidException te = new TeiidException(JDBCPlugin.Event.TEIID20000, "you don't want to see this");
        te.initCause(new Exception("or this"));

        Throwable t = ExceptionUtil.sanitize(te, true);
        assertTrue(t.getStackTrace().length != 0);
        assertNotNull(t.getCause());
        assertEquals("TEIID20000", t.getMessage());
        assertEquals("java.lang.Exception", t.getCause().getMessage());

        t = ExceptionUtil.sanitize(te, false);
        assertEquals(0, t.getStackTrace().length);
        assertEquals("TEIID20000", t.getMessage());
    }

}
