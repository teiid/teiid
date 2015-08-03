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

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SealedObject;

import org.teiid.core.CorePlugin;
import org.teiid.core.util.AccessibleByteArrayOutputStream;
import org.teiid.core.util.Base64;
import org.teiid.core.util.ObjectInputStreamWithClassloader;


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
	
	private ClassLoader classLoader = BasicCryptor.class.getClassLoader();
	private boolean useSealedObject = true;
	
    public BasicCryptor( Key encryptKey, Key decryptKey, String algorithm) throws CryptoException {
    	this.encryptKey = encryptKey;
        this.cipherAlgorithm = algorithm;
        this.decryptKey = decryptKey;

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
              throw new CryptoException(CorePlugin.Event.TEIID10006,  CorePlugin.Util.gs(CorePlugin.Event.TEIID10006, e.getClass().getName(), e.getMessage()));
        }
    }

    public String decrypt( String ciphertext ) throws CryptoException {
        if ( ciphertext == null ) {
              throw new CryptoException(CorePlugin.Event.TEIID10007,  CorePlugin.Util.gs(CorePlugin.Event.TEIID10007));
        }
        
        ciphertext = stripEncryptionPrefix(ciphertext);
       
        // Decode the previously encoded text into bytes...
        byte[] cipherBytes = null;
        try {
            cipherBytes = Base64.decode(ciphertext);
        } catch ( IllegalArgumentException e ) {
              throw new CryptoException(CorePlugin.Event.TEIID10008,  CorePlugin.Util.gs(CorePlugin.Event.TEIID10008, e.getMessage()));
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
              throw new CryptoException(CorePlugin.Event.TEIID10009,  e,  CorePlugin.Util.gs(CorePlugin.Event.TEIID10009, cipherAlgorithm ));
        } catch ( NoSuchPaddingException e ) {
              throw new CryptoException(CorePlugin.Event.TEIID10010,  CorePlugin.Util.gs(CorePlugin.Event.TEIID10010, cipherAlgorithm, e.getClass().getName(),  e.getMessage() ));
        } catch ( InvalidKeyException e ) {
              throw new CryptoException(CorePlugin.Event.TEIID10011,  e, CorePlugin.Util.gs(CorePlugin.Event.TEIID10011, e.getClass().getName(), e.getMessage()) );
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
     * @param The encrypted ciphertext, in byte form
     */
    public byte[] encrypt( byte[] cleartext ) throws CryptoException {
        return encrypt(cleartext, 0, cleartext.length);
    }
    
    public synchronized byte[] encrypt(byte[] buffer, int offset, int length)
    		throws CryptoException {
    	try {
            return encryptCipher.doFinal(buffer, offset, length);
        } catch ( Exception e ) {
            try {
                initEncryptCipher();
            } catch (CryptoException err) {
                //shouldn't happen
            }
              throw new CryptoException(CorePlugin.Event.TEIID10013, CorePlugin.Util.gs(CorePlugin.Event.TEIID10013, e.getMessage()));
        }
    }

    public String encrypt( String cleartext ) throws CryptoException {
        if ( cleartext == null ) {
              throw new CryptoException(CorePlugin.Event.TEIID10014,  CorePlugin.Util.gs(CorePlugin.Event.TEIID10014));
        }
        String clearString = new String(cleartext);
        if ( clearString.trim().length() == 0 && clearString.length() == 0 ) {
              throw new CryptoException(CorePlugin.Event.TEIID10015,  CorePlugin.Util.gs(CorePlugin.Event.TEIID10015));
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
              throw new CryptoException(CorePlugin.Event.TEIID10016,  e, CorePlugin.Util.gs(CorePlugin.Event.TEIID10016, cipherAlgorithm ));
        } catch ( NoSuchPaddingException e ) {
              throw new CryptoException(CorePlugin.Event.TEIID10017, e, CorePlugin.Util.gs(CorePlugin.Event.TEIID10017, cipherAlgorithm , e.getMessage() ));
        } catch ( InvalidKeyException e ) {
              throw new CryptoException(CorePlugin.Event.TEIID10018,  e, CorePlugin.Util.gs(CorePlugin.Event.TEIID10018, e.getMessage() ));
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
