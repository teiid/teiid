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

package org.teiid.core.crypto;

import java.io.Serializable;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SealedObject;

import org.teiid.core.CorePlugin;
import org.teiid.core.util.Base64;


/**
 * <p>Public methods in this class throw only <code>CryptoException</code>s. </p>
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
            throw new CryptoException( "ERR.003.030.0071", CorePlugin.Util.getString("ERR.003.030.0071", e.getClass().getName(), e.getMessage())); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    public String decrypt( String ciphertext ) throws CryptoException {
        if ( ciphertext == null ) {
            throw new CryptoException( "ERR.003.030.0074", CorePlugin.Util.getString("ERR.003.030.0074")); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
        ciphertext = stripEncryptionPrefix(ciphertext);
       
        // Decode the previously encoded text into bytes...
        byte[] cipherBytes = null;
        try {
            cipherBytes = Base64.decode(ciphertext);
        } catch ( IllegalArgumentException e ) {
            throw new CryptoException( "ERR.003.030.0075", CorePlugin.Util.getString("ERR.003.030.0075", e.getMessage())); //$NON-NLS-1$ //$NON-NLS-2$
        }
        // Perform standard decryption
        byte[] cleartext = decrypt( cipherBytes );
        // Perform "standard" Java encoding and return the result
        return new String(cleartext);
    }

	public static String stripEncryptionPrefix(String ciphertext) {
        if (ciphertext.startsWith(BasicCryptor.ENCRYPT_PREFIX)) {
            ciphertext = ciphertext.substring(BasicCryptor.ENCRYPT_PREFIX.length()); 
        } else if (ciphertext.startsWith(BasicCryptor.OLD_ENCRYPT_PREFIX)) {
        	ciphertext = ciphertext.substring(BasicCryptor.OLD_ENCRYPT_PREFIX.length());
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
            throw new CryptoException( e,  "ERR.003.030.0076", CorePlugin.Util.getString("ERR.003.030.0076", cipherAlgorithm )); //$NON-NLS-1$ //$NON-NLS-2$
        } catch ( NoSuchPaddingException e ) {
            throw new CryptoException( "ERR.003.030.0077", CorePlugin.Util.getString("ERR.003.030.0077", cipherAlgorithm, e.getClass().getName(),  e.getMessage() )); //$NON-NLS-1$ //$NON-NLS-2$
        } catch ( InvalidKeyException e ) {
            throw new CryptoException( e, "ERR.003.030.0079", CorePlugin.Util.getString("ERR.003.030.0079", e.getClass().getName(), e.getMessage()) ); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }
    
    public synchronized Object unsealObject(Object object) throws CryptoException {
        
        if (!(object instanceof SealedObject)) {
            return object;
        }
        
        SealedObject so = (SealedObject)object;
        
        try {
            return so.getObject(decryptCipher);
        } catch ( Exception e ) {
            try {
                initDecryptCipher();
            } catch (CryptoException err) {
                //shouldn't happen
            }
            throw new CryptoException( "ERR.003.030.0071", CorePlugin.Util.getString("ERR.003.030.0071", e.getClass().getName(), e.getMessage())); //$NON-NLS-1$ //$NON-NLS-2$
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
            throw new CryptoException("ERR.003.030.0081", CorePlugin.Util.getString("ERR.003.030.0081", e.getMessage())); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    public String encrypt( String cleartext ) throws CryptoException {
        if ( cleartext == null ) {
            throw new CryptoException( "ERR.003.030.0072", CorePlugin.Util.getString("ERR.003.030.0072")); //$NON-NLS-1$ //$NON-NLS-2$
        }
        String clearString = new String(cleartext);
        if ( clearString.trim().length() == 0 && clearString.length() == 0 ) {
            throw new CryptoException( "ERR.003.030.0073", CorePlugin.Util.getString("ERR.003.030.0073")); //$NON-NLS-1$ //$NON-NLS-2$
        }
        // Turn char array into string and get its bytes using "standard" encoding
        byte[] clearBytes = clearString.getBytes();
        // Perform standard encryption
        byte[] cipherBytes = encrypt( clearBytes );
        // Perform specialized encoding now, and return result
        
        String encoded = Base64.encodeBytes( cipherBytes );
        return BasicCryptor.ENCRYPT_PREFIX + encoded;
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
            throw new CryptoException( e, "ERR.003.030.0076", CorePlugin.Util.getString("ERR.003.030.0076", cipherAlgorithm )); //$NON-NLS-1$ //$NON-NLS-2$
        } catch ( NoSuchPaddingException e ) {
            throw new CryptoException(e, "ERR.003.030.0072", CorePlugin.Util.getString("ERR.003.030.0077", cipherAlgorithm , e.getMessage() )); //$NON-NLS-1$ //$NON-NLS-2$
        } catch ( InvalidKeyException e ) {
            throw new CryptoException( e, "ERR.003.030.0078", CorePlugin.Util.getString("ERR.003.030.0078", e.getMessage() )); //$NON-NLS-1$ //$NON-NLS-2$
        } 
    }
    
    public synchronized Object sealObject(Object object) throws CryptoException {
    	if (object != null && !(object instanceof Serializable)) {
    		throw new CryptoException("ERR.003.030.0081", CorePlugin.Util.getString("ERR.003.030.0081", "not Serializable")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    	}
        try {
            return new SealedObject((Serializable)object, encryptCipher);        
        } catch ( Exception e ) {
            try {
                initEncryptCipher();
            } catch (CryptoException err) {
                //shouldn't happen
            }
            throw new CryptoException("ERR.003.030.0081", CorePlugin.Util.getString("ERR.003.030.0081", e.getMessage())); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

} // END CLASS
