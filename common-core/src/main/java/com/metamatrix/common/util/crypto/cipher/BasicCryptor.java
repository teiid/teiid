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

package com.metamatrix.common.util.crypto.cipher;

import java.io.Serializable;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SealedObject;

import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.common.util.crypto.CryptoUtil;
import com.metamatrix.common.util.crypto.Cryptor;
import com.metamatrix.common.util.crypto.Decryptor;
import com.metamatrix.common.util.crypto.Encryptor;
import com.metamatrix.core.CorePlugin;
import com.metamatrix.core.ErrorMessageKeys;
import com.metamatrix.core.util.Base64;

/**
 * <p>Implementation of <code>Cryptor</code> interface that can perform both
 * encryption and decryption.  Instances of this class can be cast to any
 * of the following interfaces: <code>Cryptor</code>, <code>Encryptor</code>,
 * or <code>Decryptor</code>. </p>
 *
 * <p>Public methods in this class throw only <code>CryptoException</code>s. </p>
 *
 * @see Encryptor
 * @see Decryptor
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
       
    public BasicCryptor( Key encryptKey, Key decryptKey, String algorithm) throws CryptoException {
    	this.encryptKey = encryptKey;
        this.cipherAlgorithm = algorithm;
        this.decryptKey = decryptKey;

        initEncryptCipher();
        initDecryptCipher();
    }

    /**
     * Decrypt the ciphertext to yield the original cleartext.
     * @param ciphertext The text to be encrypted, in byte form
     * @param The decrypted cleartext, in byte form
     */
    public synchronized byte[] decrypt( byte[] ciphertext ) throws CryptoException {
        try {
            return decryptCipher.doFinal(ciphertext);
        } catch ( Exception e ) {
            try {
                initDecryptCipher();
            } catch (CryptoException err) {
                //shouldn't happen
            }
            throw new CryptoException( ErrorMessageKeys.CM_UTIL_ERR_0071, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0071, e.getClass().getName(), e.getMessage()));
        }
    }

    public String decrypt( String ciphertext ) throws CryptoException {
        if ( ciphertext == null ) {
            throw new CryptoException( ErrorMessageKeys.CM_UTIL_ERR_0074, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0074));
        }
        
        ciphertext = stripEncryptionPrefix(ciphertext);
       
        // Decode the previously encoded text into bytes...
        byte[] cipherBytes = null;
        try {
            cipherBytes = Base64.decode(ciphertext);
        } catch ( IllegalArgumentException e ) {
            throw new CryptoException( ErrorMessageKeys.CM_UTIL_ERR_0075, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0075, e.getMessage()));
        }
        // Perform standard decryption
        byte[] cleartext = decrypt( cipherBytes );
        // Perform "standard" Java encoding and return the result
        return new String(cleartext);
    }

	public static String stripEncryptionPrefix(String ciphertext) {
        if (ciphertext.startsWith(CryptoUtil.ENCRYPT_PREFIX)) {
            ciphertext = ciphertext.substring(CryptoUtil.ENCRYPT_PREFIX.length()); 
        } else if (ciphertext.startsWith(CryptoUtil.OLD_ENCRYPT_PREFIX)) {
        	ciphertext = ciphertext.substring(CryptoUtil.OLD_ENCRYPT_PREFIX.length());
        }
		return ciphertext;
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
            decryptCipher.init( Cipher.DECRYPT_MODE, decryptKey );
        } catch ( NoSuchAlgorithmException e ) {
            throw new CryptoException( e,  ErrorMessageKeys.CM_UTIL_ERR_0076, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0076, cipherAlgorithm ));
        } catch ( NoSuchPaddingException e ) {
            throw new CryptoException( ErrorMessageKeys.CM_UTIL_ERR_0077, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0077, cipherAlgorithm, e.getClass().getName(),  e.getMessage() ));
        } catch ( InvalidKeyException e ) {
            throw new CryptoException( e, ErrorMessageKeys.CM_UTIL_ERR_0079, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0079, e.getClass().getName(), e.getMessage()) );
        }
    }
    
    public synchronized Serializable unsealObject(Serializable object) throws CryptoException {
        
        if (!(object instanceof SealedObject)) {
            return object;
        }
        
        SealedObject so = (SealedObject)object;
        
        try {
            return (Serializable)so.getObject(decryptCipher);
        } catch ( Exception e ) {
            try {
                initDecryptCipher();
            } catch (CryptoException err) {
                //shouldn't happen
            }
            throw new CryptoException( ErrorMessageKeys.CM_UTIL_ERR_0071, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0071, e.getClass().getName(), e.getMessage()));
        } 
    }
            
    /**
     * Encrypt the cleartext in byte array format.
     * @param cleartext The text to be encrypted, in byte form
     * @param The encrypted ciphertext, in byte form
     */
    public synchronized byte[] encrypt( byte[] cleartext ) throws CryptoException {
        try {
            return encryptCipher.doFinal(cleartext);
        } catch ( Exception e ) {
            try {
                initEncryptCipher();
            } catch (CryptoException err) {
                //shouldn't happen
            }
            throw new CryptoException(ErrorMessageKeys.CM_UTIL_ERR_0081, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0081, e.getMessage()));
        }
    }

    public String encrypt( String cleartext ) throws CryptoException {
        if ( cleartext == null ) {
            throw new CryptoException( ErrorMessageKeys.CM_UTIL_ERR_0072, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0072));
        }
        String clearString = new String(cleartext);
        if ( clearString.trim().length() == 0 && clearString.length() == 0 ) {
            throw new CryptoException( ErrorMessageKeys.CM_UTIL_ERR_0073, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0073));
        }
        // Turn char array into string and get its bytes using "standard" encoding
        byte[] clearBytes = clearString.getBytes();
        // Perform standard encryption
        byte[] cipherBytes = encrypt( clearBytes );
        // Perform specialized encoding now, and return result
        
        String encoded = Base64.encodeBytes( cipherBytes );
        return CryptoUtil.ENCRYPT_PREFIX + encoded;
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
            encryptCipher.init( Cipher.ENCRYPT_MODE, encryptKey );
        } catch ( NoSuchAlgorithmException e ) {
            throw new CryptoException( e, ErrorMessageKeys.CM_UTIL_ERR_0076, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0076, cipherAlgorithm ));
        } catch ( NoSuchPaddingException e ) {
            throw new CryptoException(e, ErrorMessageKeys.CM_UTIL_ERR_0072, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0077, cipherAlgorithm , e.getMessage() ));
        } catch ( InvalidKeyException e ) {
            throw new CryptoException( e, ErrorMessageKeys.CM_UTIL_ERR_0078, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0078, e.getMessage() ));
        } 
    }
    
    public synchronized Serializable sealObject(Serializable object) throws CryptoException {
        try {
            return new SealedObject(object, encryptCipher);        
        } catch ( Exception e ) {
            try {
                initEncryptCipher();
            } catch (CryptoException err) {
                //shouldn't happen
            }
            throw new CryptoException(ErrorMessageKeys.CM_UTIL_ERR_0081, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0081, e.getMessage()));
        }
    }

} // END CLASS
