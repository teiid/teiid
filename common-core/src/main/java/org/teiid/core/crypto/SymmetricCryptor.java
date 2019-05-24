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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.teiid.core.CorePlugin;
import org.teiid.core.util.ArgCheck;


/**
 * Provides a symmetric cryptor using AES
 */
public class SymmetricCryptor extends BasicCryptor {

    public static final String DEFAULT_SYM_KEY_ALGORITHM = "AES"; //$NON-NLS-1$
    public static final String ECB_SYM_ALGORITHM = "AES/ECB/PKCS5Padding"; //$NON-NLS-1$
    public static final String CBC_SYM_ALGORITHM = "AES/CBC/PKCS5Padding"; //$NON-NLS-1$
    public static final int DEFAULT_KEY_BITS = 128;
    public static final String DEFAULT_STORE_PASSWORD = "changeit"; //$NON-NLS-1$
    public static final String DEFAULT_ALIAS = "cluster_key"; //$NON-NLS-1$

    private static KeyGenerator keyGen;

    /**
     * Creates a new SymmetricCryptor with a new symmetric key
     *
     * @return a new SymmetricCryptor
     * @throws CryptoException
     */
    public static SymmetricCryptor getSymmectricCryptor(boolean cbc) throws CryptoException {
        Key key = generateKey();

        return new SymmetricCryptor(key, cbc);
    }

    public static SecretKey generateKey() throws CryptoException {
        try {
            synchronized(SymmetricCryptor.class) {
                if (keyGen == null) {
                    keyGen = KeyGenerator.getInstance(DEFAULT_SYM_KEY_ALGORITHM);
                }
                keyGen.init(DEFAULT_KEY_BITS);
                return keyGen.generateKey();
            }
        } catch (GeneralSecurityException e) {
              throw new CryptoException(CorePlugin.Event.TEIID10021, e);
        }
    }

    /**
     * Creates a SymmetricCryptor using the supplied URL contents as the key
     *
     * @param keyResource URL to the key
     * @return a new SymmetricCryptor
     * @throws CryptoException
     * @throws IOException
     */
    public static SymmetricCryptor getSymmectricCryptor(URL keyResource) throws CryptoException, IOException {
        ArgCheck.isNotNull(keyResource);
        InputStream stream = keyResource.openStream();
        try {
            KeyStore store = KeyStore.getInstance("JCEKS"); //$NON-NLS-1$
            store.load(stream, DEFAULT_STORE_PASSWORD.toCharArray());
            Key key = store.getKey(DEFAULT_ALIAS, DEFAULT_STORE_PASSWORD.toCharArray());
            return new SymmetricCryptor(key, true);
        } catch (GeneralSecurityException e) {
              throw new CryptoException(CorePlugin.Event.TEIID10022, e);
        } finally {
            stream.close();
        }
    }

    /**
     * Creates a SymmetricCryptor using the supplied byte array as the key
     *
     * @param key
     * @return a new SymmetricCryptor
     * @throws CryptoException
     */
    public static SymmetricCryptor getSymmectricCryptor(byte[] key, boolean cbc) throws CryptoException {
        Key secretKey = new SecretKeySpec(key, DEFAULT_SYM_KEY_ALGORITHM);
        return new SymmetricCryptor(secretKey, cbc);
    }

    public static SymmetricCryptor getSymmectricCryptor(byte[] key, String algorithm, String cipherAlgorithm, IvParameterSpec iv) throws CryptoException {
        Key secretKey = new SecretKeySpec(key, algorithm);
        return new SymmetricCryptor(secretKey, cipherAlgorithm, iv);
    }

    public static void generateAndSaveKey(String file) throws CryptoException, IOException {
        SecretKey key = generateKey();
        saveKey(file, key);
    }

    private static void saveKey(String file, SecretKey key) throws CryptoException, IOException {
        ArgCheck.isNotNull(file);
        FileOutputStream fos = new FileOutputStream(file);
        try {
            KeyStore store = KeyStore.getInstance("JCEKS"); //$NON-NLS-1$
            store.load(null,null);
            store.setKeyEntry(DEFAULT_ALIAS, key, DEFAULT_STORE_PASSWORD.toCharArray(),null);
            store.store(fos, DEFAULT_STORE_PASSWORD.toCharArray());
        } catch (GeneralSecurityException e) {
              throw new CryptoException(CorePlugin.Event.TEIID10023, e);
        } finally {
            fos.close();
        }
    }

    SymmetricCryptor(Key key, boolean cbc) throws CryptoException {
        super(key, key, cbc?CBC_SYM_ALGORITHM:ECB_SYM_ALGORITHM, cbc?new IvParameterSpec(new byte[] {0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf}):null);
    }

    SymmetricCryptor(Key key, String cipherAlgorithm, IvParameterSpec iv) throws CryptoException {
        super(key, key, cipherAlgorithm, iv);
    }

    public byte[] getEncodedKey() {
        return this.decryptKey.getEncoded();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("The file to create must be supplied as the only argument."); //$NON-NLS-1$
            System.exit(-1);
        }
        SymmetricCryptor.generateAndSaveKey(args[0]);
    }
}
