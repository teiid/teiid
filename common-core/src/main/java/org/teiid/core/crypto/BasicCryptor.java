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

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SealedObject;
import javax.crypto.spec.IvParameterSpec;

import org.teiid.core.CorePlugin;
import org.teiid.core.util.AccessibleByteArrayOutputStream;
import org.teiid.core.util.ObjectInputStreamWithClassloader;


/**
 * <p>Public methods in this class throw only <code>CryptoException</code>s.
 */
public class BasicCryptor implements Cryptor {

    /** The key to be used for decryption. */
    protected Key decryptKey;
    /** The <code>Cipher</code> to use for decryption. */
    private Cipher decryptCipher;
    /** The key to be used for encryption. */
    private Key encryptKey;
    /** The <code>Cipher</code> to use for encryption. */
    protected Cipher encryptCipher;
    protected String cipherAlgorithm;
    public static final String OLD_ENCRYPT_PREFIX = "{mm-encrypt}"; //$NON-NLS-1$
    public static final String ENCRYPT_PREFIX = "{teiid-encrypt}"; //$NON-NLS-1$

    private static final SecureRandom random = new SecureRandom();

    private ClassLoader classLoader = BasicCryptor.class.getClassLoader();
    private boolean useSealedObject = true;
    private IvParameterSpec iv;
    private byte[] randBuffer;

    public BasicCryptor( Key encryptKey, Key decryptKey, String algorithm, IvParameterSpec iv) throws CryptoException {
        this.encryptKey = encryptKey;
        this.cipherAlgorithm = algorithm;
        this.decryptKey = decryptKey;
        this.iv = iv;
        if (iv != null) {
            randBuffer = new byte[iv.getIV().length];
        }

        initEncryptCipher();
        initDecryptCipher();
    }

    public synchronized void setUseSealedObject(boolean useSealedObject) {
        this.useSealedObject = useSealedObject;
    }

    public synchronized void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    /**
     * Decrypt the ciphertext to yield the original cleartext.
     * @param ciphertext The text to be encrypted, in byte form
     * @return The decrypted cleartext, in byte form
     */
    public synchronized byte[] decrypt( byte[] ciphertext ) throws CryptoException {
        try {
            byte[] result = decryptCipher.doFinal(ciphertext);
            if (iv != null) {
                //throw away the first block
                return Arrays.copyOfRange(result, iv.getIV().length, result.length);
            }
            return result;
        } catch ( Exception e ) {
            try {
                initDecryptCipher();
            } catch (CryptoException err) {
                //shouldn't happen
            }
              throw new CryptoException(CorePlugin.Event.TEIID10006,  CorePlugin.Util.gs(CorePlugin.Event.TEIID10006, e.getClass().getName(), e.getMessage()));
        }
    }

    /**
     * Initialize the ciphers used for encryption and decryption.  The ciphers
     * define the algorithms to be used.  They are initialized with the
     * appropriate key to be used in the encryption or decryption operation.
     */
    protected void initDecryptCipher() throws CryptoException {

        // Create and initialize decryption cipher
        try {
            decryptCipher = Cipher.getInstance( cipherAlgorithm);
            decryptCipher.init( Cipher.DECRYPT_MODE, decryptKey, iv );
        } catch ( NoSuchAlgorithmException e ) {
              throw new CryptoException(CorePlugin.Event.TEIID10009,  e,  CorePlugin.Util.gs(CorePlugin.Event.TEIID10009, cipherAlgorithm ));
        } catch ( NoSuchPaddingException e ) {
              throw new CryptoException(CorePlugin.Event.TEIID10010,  CorePlugin.Util.gs(CorePlugin.Event.TEIID10010, cipherAlgorithm, e.getClass().getName(),  e.getMessage() ));
        } catch ( InvalidKeyException e ) {
              throw new CryptoException(CorePlugin.Event.TEIID10011,  e, CorePlugin.Util.gs(CorePlugin.Event.TEIID10011, e.getClass().getName(), e.getMessage()) );
        } catch (InvalidAlgorithmParameterException e) {
            throw new CryptoException(CorePlugin.Event.TEIID10009,  e,  CorePlugin.Util.gs(CorePlugin.Event.TEIID10009, cipherAlgorithm ));
        }
    }

    public synchronized Object unsealObject(Object object) throws CryptoException {
        if (useSealedObject) {
            if (!(object instanceof SealedObject)) {
                return object;
            }

            SealedObject so = (SealedObject)object;

            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            try {
                if (cl != classLoader) {
                    Thread.currentThread().setContextClassLoader(BasicCryptor.class.getClassLoader());
                }
                return so.getObject(decryptCipher);
            } catch ( Exception e ) {
                try {
                    initDecryptCipher();
                } catch (CryptoException err) {
                    //shouldn't happen
                }
                  throw new CryptoException(CorePlugin.Event.TEIID10006,  CorePlugin.Util.gs(CorePlugin.Event.TEIID10006, e.getClass().getName(), e.getMessage()));
            } finally {
                Thread.currentThread().setContextClassLoader(cl);
            }
        }
        if (!(object instanceof byte[])) {
            return object;
        }
        byte[] bytes = (byte[])object;
        bytes = decrypt(bytes);
        try {
            ObjectInputStream ois = new ObjectInputStreamWithClassloader(new ByteArrayInputStream(bytes), classLoader);
            return ois.readObject();
        } catch (Exception e) {
            throw new CryptoException(CorePlugin.Event.TEIID10006,  CorePlugin.Util.gs(CorePlugin.Event.TEIID10006, e.getClass().getName(), e.getMessage()));
        }
    }

    /**
     * Encrypt the cleartext in byte array format.
     * @param cleartext The text to be encrypted, in byte form
     * @return The encrypted ciphertext, in byte form
     */
    public byte[] encrypt( byte[] cleartext ) throws CryptoException {
        return encrypt(cleartext, 0, cleartext.length);
    }

    public synchronized byte[] encrypt(byte[] buffer, int offset, int length)
            throws CryptoException {
        try {
            byte[] initBlock = null;
            if (iv != null) {
                random.nextBytes(randBuffer);
                initBlock = encryptCipher.update(randBuffer);
            }
            byte[] result = encryptCipher.doFinal(buffer, offset, length);
            if (initBlock != null) {
                byte[] newResult = Arrays.copyOf(initBlock, initBlock.length + result.length);
                System.arraycopy(result, 0, newResult, initBlock.length, result.length);
                return newResult;
            }
            return result;
        } catch ( Exception e ) {
            try {
                initEncryptCipher();
            } catch (CryptoException err) {
                //shouldn't happen
            }
              throw new CryptoException(CorePlugin.Event.TEIID10013, CorePlugin.Util.gs(CorePlugin.Event.TEIID10013, e.getMessage()));
        }
    }

    /**
     * Initialize the cipher used for encryption.  The cipher defines the
     * algorithm to be used.  It is initialized with the appropriate key to
     * be used in the encryption operation.
     */
    protected void initEncryptCipher() throws CryptoException {

        // Create and initialize encryption cipher
        try {
            encryptCipher = Cipher.getInstance( cipherAlgorithm );
            encryptCipher.init( Cipher.ENCRYPT_MODE, encryptKey, iv );
        } catch ( NoSuchAlgorithmException e ) {
              throw new CryptoException(CorePlugin.Event.TEIID10016,  e, CorePlugin.Util.gs(CorePlugin.Event.TEIID10016, cipherAlgorithm ));
        } catch ( NoSuchPaddingException e ) {
              throw new CryptoException(CorePlugin.Event.TEIID10017, e, CorePlugin.Util.gs(CorePlugin.Event.TEIID10017, cipherAlgorithm , e.getMessage() ));
        } catch ( InvalidKeyException e ) {
              throw new CryptoException(CorePlugin.Event.TEIID10018,  e, CorePlugin.Util.gs(CorePlugin.Event.TEIID10018, e.getMessage() ));
        } catch (InvalidAlgorithmParameterException e) {
            throw new CryptoException(CorePlugin.Event.TEIID10016,  e, CorePlugin.Util.gs(CorePlugin.Event.TEIID10016, cipherAlgorithm ));
        }
    }

    public synchronized Object sealObject(Object object) throws CryptoException {
        try {
            if (useSealedObject) {
                return new SealedObject((Serializable)object, encryptCipher);
            }
            AccessibleByteArrayOutputStream baos = new AccessibleByteArrayOutputStream(1 << 13);
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            oos.flush();
            oos.close();
            return encrypt(baos.getBuffer(), 0, baos.getCount());
        } catch ( Exception e ) {
            try {
                initEncryptCipher();
            } catch (CryptoException err) {
                //shouldn't happen
            }
              throw new CryptoException(CorePlugin.Event.TEIID10013, CorePlugin.Util.gs(CorePlugin.Event.TEIID10013, e.getMessage()));
        }
    }

} // END CLASS
