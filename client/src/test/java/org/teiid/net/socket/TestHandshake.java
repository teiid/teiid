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

import static org.junit.Assert.assertEquals;

import java.io.FileInputStream;
import java.io.ObjectInputStream;

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;

@SuppressWarnings("nls")
public class TestHandshake {

    @Test public void testCompatibility() throws Exception {
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(UnitTestUtil.getTestDataFile("handshake.ser")));
        Handshake hs = (Handshake)ois.readObject();
        assertEquals(AuthenticationType.USERPASSWORD, hs.getAuthType());
    }

    @Test public void testVersionNormalization() throws Exception {
        Handshake hs = new Handshake("11.2.3.a");
        assertEquals("11.02.03.a", hs.getVersion());
    }

}
