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

package com.metamatrix.common.util.crypto;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

import com.metamatrix.common.util.crypto.cipher.SymmetricCryptor;
import com.metamatrix.core.CorePlugin;
import com.metamatrix.core.ErrorMessageKeys;

/**
 * <p>Utility that allows ciphertext that was encrypted with one key
 * to be decrypted with the old key and re-encrypted with a new key</p>
 * <p><i>This utility assumes that the keys have been created and verified to work
 * correctly.</i></p>
 */
public class PasswordChangeUtility {

    /** The name of the old key file name. */
    private String oldkeyName;

    /** The name of the new key file name. */
    private String newkeyName;

    /** Initailization state. */
    protected boolean initialized;

    /** The Cryptor to use for decrypting the old passwords. */
    protected Cryptor oldCryptor;

    /** The Cryptor to use for encrypting the new passwords. */
    protected Cryptor newCryptor;

    /**
     * Construct a <code>PasswordChangeUtility</code> with information for two different
     * keys.
     * @param oldkeyFileName The absolute path to the key with which you would like
     * to decrypt the old passwords.
     * @param newkeyFileName The absolute path to the key with which you would like
     * to encrypt the new passwords.
     * @throws IllegaArgumentException if any arguments are <code>null</code> or empty.
     */
    public PasswordChangeUtility(String oldkeyFileName,
                                 String newkeyFileName) {
        if (oldkeyFileName == null || oldkeyFileName.length() == 0) {
            throw new IllegalArgumentException(CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0062));
        }
        if (newkeyFileName == null || newkeyFileName.length() == 0) {
            throw new IllegalArgumentException(CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0064));
        }

        this.oldkeyName = oldkeyFileName;
        this.newkeyName = newkeyFileName;
    }
    
    // =========================================================================
    //                        I N I T I A L I Z A T I O N
    // =========================================================================

    /**
     * <p>Initialize this factory, bound to a specific key and key
     * entry in that store. </p>
     */
    public synchronized void init() throws CryptoException {

        if ( initialized == false ) {

            // Init and get the Cryptor for old passwords.
            try {
                this.oldCryptor = SymmetricCryptor.getSymmectricCryptor(new File(oldkeyName).toURL());
            } catch ( FileNotFoundException e ) {
                throw new CryptoException(e, ErrorMessageKeys.CM_UTIL_ERR_0066, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0066, e.getMessage()));
            } catch ( IOException e ) {
                throw new CryptoException(e, ErrorMessageKeys.CM_UTIL_ERR_0066, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0066, e.getMessage()));
            }

            // Init and get the Cryptor for new passwords.
            try {
                this.newCryptor = SymmetricCryptor.getSymmectricCryptor(new File(newkeyName).toURL());
            } catch ( FileNotFoundException e ) {
                throw new CryptoException(e, ErrorMessageKeys.CM_UTIL_ERR_0066, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0067, e.getMessage()));
            } catch ( IOException e ) {
                throw new CryptoException(e, ErrorMessageKeys.CM_UTIL_ERR_0066, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0067, e.getMessage()));
            }

            initialized = true;
        }
    }

    // =========================================================================
    //                     U T I L I T Y     M E T H O D S
    // =========================================================================

    /**
     * Use old Cryptor to encrypt a char[].
     * @param cleartext The cleartext that you want encrypted with the
     * old Cryptor.
     * @return The encrypted ciphertext chars.
     * @throws CryptoException If an error occurs durring initialization
     * of the Cryptor or during encryption.
     */
    public String oldEncrypt( String cleartext ) throws CryptoException {
        init();
        return oldCryptor.encrypt( cleartext );
    }

    
    /**
     * Use old Cryptor to encrypt properties.
     * Encrypts any properties that end with <code>match</code>  
     * @param match
     * @param props
     * @return Encrypted properties
     * @throws CryptoException
     * @since 4.3
     */
    public Properties oldEncrypt(String match, Properties props)
            throws CryptoException {
        init();
    	return doCrypt(oldCryptor, true, match, props);
    }
     


    /**
     * Use old Cryptor to decrypt a char[].
     * @param ciphertext The ciphertext that you want decrypted with the
     * old Cryptor.
     * @return The decrypted cleartext chars.
     * @throws CryptoException If an error occurs durring initialization
     * of the Cryptor or during decryption.
     */
    public String oldDecrypt( String ciphertext ) throws CryptoException {
        init();
        return oldCryptor.decrypt( ciphertext );
    }

    /**
     * Use old Cryptor to decrypt properties.
     * Decrypts any properties that end with <code>match</code>  
     * @param match
     * @param props
     * @return decrypted properties
     * @throws CryptoException
     * @since 4.3
     */
    public Properties oldDecrypt(String match, Properties props)
            throws CryptoException {
        init();
        return doCrypt(oldCryptor, false, match, props);
    }


    /**
     * Use new Cryptor to encrypt a char[].
     * @param cleartext The cleartext that you want encrypted with the
     * new Cryptor.
     * @return The encrypted ciphertext chars.
     * @throws CryptoException If an error occurs durring initialization
     * of the Cryptor or during encryption.
     */
    public String newEncrypt(String cleartext ) throws CryptoException {
        init();
        return newCryptor.encrypt( cleartext );
    }

    /**
     * Use new Cryptor to encrypt properties.
     * Encrypts any properties that end with <code>match</code>  
     * @param match
     * @param props
     * @return encrypted properties
     * @throws CryptoException
     * @since 4.3
     */
    public Properties newEncrypt(String match, Properties props)
            throws CryptoException {
        init();
        return doCrypt(newCryptor, true, match, props);
    }


    /**
     * Use new Cryptor to decrypt a char[].
     * @param ciphertext The ciphertext that you want decrypted with the
     * new Cryptor.
     * @return The decrypted cleartext chars.
     * @throws CryptoException If an error occurs durring initialization
     * of the Cryptor or during decryption.
     */
    public String newDecrypt( String ciphertext ) throws CryptoException {
        init();
        return newCryptor.decrypt( ciphertext );
    }

    /**
     * Use new Cryptor to decrypt properties.
     * Decrypts any properties that end with <code>match</code>  
     * @param match
     * @param props
     * @return decrypted properties
     * @throws CryptoException
     * @since 4.3
     */
    public Properties newDecrypt(String match, Properties props)
            throws CryptoException {
        init();
        return doCrypt(newCryptor, false, match, props);
    }
    
    /**
     * Encrypt or decrypt properties using the specified encryptor.
     * Operates on any properties that end with <code>match</code>
     * @param cryptor
     * @param encrypt If true, encrypt.  If false, decrypt.
     * @param match
     * @param props
     * @return
     * @throws CryptoException
     * @since 4.3
     */
    private Properties doCrypt(Cryptor cryptor, boolean encrypt, String match, Properties props)
            throws CryptoException {
        Properties modifiedProps = new Properties();

        Enumeration propEnum = props.propertyNames();

        String matchUpper = match.toUpperCase();
        while (propEnum.hasMoreElements()) {
            String propName = (String) propEnum.nextElement();
            if (propName.toUpperCase().endsWith(matchUpper)) {
                String propVal = props.getProperty(propName);
                if (propVal != null && propVal.length() > 0) {
                    String cryptValue;
                    if (encrypt) {
                        cryptValue = cryptor.encrypt(propVal);
                    } else {
                        cryptValue = cryptor.decrypt(propVal);                        
                    }
                    modifiedProps.setProperty(propName, cryptValue);
                }
            } else {
                modifiedProps.setProperty(propName, props.getProperty(propName));
            }
        }
        return modifiedProps;
   }

} // END CLASS

