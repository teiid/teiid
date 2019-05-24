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

package org.teiid.core.crypto;

import static org.junit.Assert.*;

import java.nio.charset.Charset;

import org.junit.Test;


@SuppressWarnings("nls")
public class TestDhKeyGenerator {

    @Test
    public void testKeyGenerationDefault() throws CryptoException {
        DhKeyGenerator keyGenServer = new DhKeyGenerator();
        DhKeyGenerator keyGenClient = new DhKeyGenerator();
        byte[] serverKey = keyGenServer.createPublicKey(true);
        byte[] clientKey = keyGenClient.createPublicKey(true);
        SymmetricCryptor serverCryptor = keyGenServer.getSymmetricCryptor(clientKey, false, TestDhKeyGenerator.class.getClassLoader(), true, true);
        SymmetricCryptor clientCryptor = keyGenClient.getSymmetricCryptor(serverKey, false, TestDhKeyGenerator.class.getClassLoader(), true, true);

        String cleartext = "cleartext!"; //$NON-NLS-1$

        byte[] ciphertext = serverCryptor.encrypt(cleartext.getBytes(Charset.forName("UTF-8")));
        byte[] cleartext2 = clientCryptor.decrypt(ciphertext);

        assertArrayEquals(cleartext.getBytes(Charset.forName("UTF-8")), cleartext2);

        Object sealed = serverCryptor.sealObject(cleartext);
        Object unsealed = clientCryptor.unsealObject(sealed);

        assertEquals(cleartext, unsealed);
        assertTrue(!sealed.equals(unsealed));
    }

}
