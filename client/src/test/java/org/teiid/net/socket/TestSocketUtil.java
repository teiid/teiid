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

package org.teiid.net.socket;

import java.io.IOException;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.net.socket.SocketUtil;

import junit.framework.TestCase;


public class TestSocketUtil extends TestCase {

    public void testNoPassword() throws Exception {
        SocketUtil.loadKeyStore(UnitTestUtil.getTestDataFile("metamatrix.keystore").getAbsolutePath(), null, "JKS"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testMissingKeyStore() throws Exception {
        try {
            SocketUtil.loadKeyStore("metamatrix.keystorefoo", null, "JKS"); //$NON-NLS-1$ //$NON-NLS-2$
            fail("expected exception"); //$NON-NLS-1$
        } catch (IOException ex) {
            assertEquals("Key store 'metamatrix.keystorefoo' was not found.", ex.getMessage()); //$NON-NLS-1$
        }
    }


}
