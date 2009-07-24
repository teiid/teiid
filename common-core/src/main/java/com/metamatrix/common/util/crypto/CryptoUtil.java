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
import java.net.URL;
import java.util.Enumeration;
import java.util.Properties;

import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.common.util.crypto.cipher.BasicCryptor;
import com.metamatrix.common.util.crypto.cipher.SymmetricCryptor;
import com.metamatrix.core.CorePlugin;
import com.metamatrix.core.ErrorMessageKeys;
import com.metamatrix.core.util.Base64;

/**
 * Collection of Crypto utilities and helpers for use by the server and installers.
 * 
 */
public class CryptoUtil {
	/**
	* This property indicates the encryption provider, if set to none encryption is disabled.
	*/
	public static final String ENCRYPTION_ENABLED = "teiid.encryption.enabled"; //$NON-NLS-1$

	/** The name of the key. */
	public static final String KEY_NAME = "teiid.keystore"; //$NON-NLS-1$
    public static final URL KEY = CryptoUtil.class.getResource("/" + KEY_NAME); //$NON-NLS-1$
    public static final String OLD_ENCRYPT_PREFIX = "{mm-encrypt}"; //$NON-NLS-1$
    public static final String ENCRYPT_PREFIX = "{teiid-encrypt}"; //$NON-NLS-1$
    // Empty provider means encryption should be disabled
    public static final String NONE = "none"; //$NON-NLS-1$

    private static boolean encryptionEnabled = PropertiesUtils.getBooleanProperty(System.getProperties(), ENCRYPTION_ENABLED, true);

	private static Cryptor CRYPTOR;

	public static boolean isEncryptionEnabled() {
		return encryptionEnabled;
	}

    /**
     * Encrypts given set of property values based on occurrence of the property name in
     * the <code>match</code> collection.
     * @param match A Set of property names that, if found in <code>props</code>
     * property names, will modify the associated value in <code>props</code>.  <strong>
     * Note</strong>: This is a <i><b>case insensitive</b></i> match.
     * @param props The properties whose values are to be modified in place.
     * @returns A copy of the <code>props</code> with values modified.
     * @throws CryptoException if there's an error with the <code>Cryptor</code>.
     */
    public static Properties propertyEncrypt(String match, Properties props)
            throws CryptoException {
        Cryptor theCryptor = CryptoUtil.getCryptor();
        Properties modifiedProps = new Properties();

        Enumeration propEnum = props.propertyNames();

        while (propEnum.hasMoreElements()) {
            String propName = (String) propEnum.nextElement();
            if (match.equalsIgnoreCase(propName)) {
                String propVal = props.getProperty(propName);               
                if (propVal != null) {
                    if (propVal.trim().length() > 0) {
                        String cryptValue = theCryptor.encrypt(propVal);
                        modifiedProps.setProperty(propName, cryptValue);
                    } else {
                        modifiedProps.setProperty(propName, propVal);
                    }
                }
            } else {
                modifiedProps.setProperty(propName, props.getProperty(propName));
            }
        }
        return modifiedProps;
    }

    /**
     * Decrypts given set of property values based on occurrence of the property name in
     * the <code>match</code> collection.
     * @param match A Set of property names that, if found in <code>props</code>
     * property names, will modify the associated value in <code>props</code>.  <strong>
     * Note</strong>: This is a <i><b>case insensitive</b></i> match.
     * @param props The properties whose values are to be modified in place.
     * @returns A copy of the <code>props</code> with values modified.
     * @throws CryptoException if there's an error with the <code>Cryptor</code>.
     */
    public static Properties propertyDecrypt(String match, Properties props)
            throws CryptoException {
        Cryptor theCryptor = CryptoUtil.getCryptor();
        Properties modifiedProps = new Properties();

        Enumeration propEnum = props.propertyNames();

        while (propEnum.hasMoreElements()) {
            String propName = (String) propEnum.nextElement();
            if (match.equalsIgnoreCase(propName)) {
                String propVal = props.getProperty(propName);               
                if (propVal != null) {
                    if (propVal.trim().length() > 0 && CryptoUtil.isValueEncrypted(propVal)) {
        		      	String cryptValue = theCryptor.decrypt(propVal);
                     	modifiedProps.setProperty(propName, cryptValue);

                    } else {
                        modifiedProps.setProperty(propName, propVal);
                    }
                }
                
            } else {
                modifiedProps.setProperty(propName, props.getProperty(propName));
            }
        }
        return modifiedProps;
    }

    /**
     * Encrypts given set of property values based on occurrence of the property name ending
     * in with the given pattern, using the server-side encryptor.
     * This method requires that the server keystore is available.     
     * @param pattern A pattern that, if found at the end of a <code>props</code>
     * property name, will modify the associated value in <code>props</code>.  <strong>
     * Note</strong>: This is a <i><b>case insensitive</b></i> match.
     * @param props The properties whose values are to be modified.
     * @returns A copy of the <code>props</code> with values modified.
     * @throws CryptoException if there's an error with the <code>Cryptor</code>.
     */
    public static Properties propertyEncryptEndsWith(String pattern, Properties props)
            throws CryptoException {
        
        return propertyEncryptEndsWith(CryptoUtil.getCryptor(), pattern, props);
    }
    
    
    /**
     * Encrypts given set of property values based on occurance of the property name ending
     * in with the given pattern, using the specified encryptor.
     * @param encryptor Encryptor to use.
     * @param pattern A pattern that, if found at the end of a <code>props</code>
     * property name, will modify the associated value in <code>props</code>.  <strong>
     * Note</strong>: This is a <i><b>case insensitive</b></i> match.
     * @param props The properties whose values are to be modified.
     * @returns A copy of the <code>props</code> with values modified.
     * @throws CryptoException if there's an error with the <code>Cryptor</code>.
     */
    public static Properties propertyEncryptEndsWith(Encryptor encryptor, String pattern, Properties props)
            throws CryptoException {
        Properties modifiedProps = new Properties();

        Enumeration propEnum = props.propertyNames();
        pattern = pattern.toUpperCase();
        while (propEnum.hasMoreElements()) {
            String propName = (String) propEnum.nextElement();
            if (propName.toUpperCase().endsWith(pattern)) {
                String propVal = props.getProperty(propName);
                if (propVal != null) {
                    if (propVal.trim().length() > 0) {
                        String cryptValue = encryptor.encrypt(propVal);
                        modifiedProps.setProperty(propName, cryptValue);
                    } else {
                        modifiedProps.setProperty(propName, propVal);
                    }
                }
            } else {
                modifiedProps.setProperty(propName, props.getProperty(propName));
            }
        }
        return modifiedProps;
    }

    /**
     * Decrypts given set of property values based on occurance of the property name ending
     * in with the given pattern.
     * @param pattern A pattern that, if found at the end of a <code>props</code>
     * property name, will modify the associated value in <code>props</code>.  <strong>
     * Note</strong>: This is a <i><b>case insensitive</b></i> match.
     * @param props The properties whose values are to be modified.
     * @returns A copy of the <code>props</code> with values modified.
     * @throws CryptoException if there's an error with the <code>Cryptor</code>.
     */
    public static Properties propertyDecryptEndsWith(String pattern, Properties props)
            throws CryptoException {
        Cryptor theCryptor = CryptoUtil.getCryptor();
        Properties modifiedProps = new Properties();

        Enumeration propEnum = props.propertyNames();
        pattern = pattern.toUpperCase();
        while (propEnum.hasMoreElements()) {
            String propName = (String) propEnum.nextElement();
            if (propName.toUpperCase().endsWith(pattern)) {
                String propVal = props.getProperty(propName);
                // don't try to decrypt a value that doesn't have length
                // an encrypted password would have some content
                if (propVal != null) {
                    if (propVal.trim().length() > 0 && CryptoUtil.isValueEncrypted(propVal)) {
	                        String cryptValue = theCryptor.decrypt(propVal);
        	                modifiedProps.setProperty(propName, cryptValue);
                    } else {
                        modifiedProps.setProperty(propName, propVal);
                    }
                }
            } else {
                modifiedProps.setProperty(propName, props.getProperty(propName));
            }
        }
        return modifiedProps;
    }
    
    public static boolean isValueEncrypted(String value) {
    	if (value == null) {
    		return false;
    	}
        try {
        	if (value.trim().length() == 0) {
            	return false;
            }
        	String strippedValue = BasicCryptor.stripEncryptionPrefix(value);
        	if (strippedValue.length() != value.length()) {
        		try {
                	Base64.decode(strippedValue);
                } catch (IllegalArgumentException e) {
                    return false;
                }
                //if we have the encrypt prefix and the rest of the string is base64 encoded, then
                //we'll assume that it's properly encrypted
                return true;
        	}
            CryptoUtil.getDecryptor().decrypt(value);
            return true;
        } catch (CryptoException err) {
            return false;
        }
    }

	/**
	 * <p>Initialize this factory, bound to a specific keystore and key
	 * entry in that store. 
	 * <br>The keystore file name is well-known and must be in the applicaition's classpath.
	 * The key entry name is well-known.
	 * The keystore password is found in configuration.<\br></p>
	 */
	private static synchronized void init()
	    throws CryptoException {
	
		init(KEY);
	}

	/**
	 * <p>Initialize this factory, bound to the given keystore and keystore password.
	 * <br>This method <strong>will</strong> initialize the cryptor with the given
	 * keystore and <strong>will not</strong> use the application classpath to find
	 * the keystore to use.
	 * The key entry name is well-known.
	 * </br> </p>
	 * @param storeFilename url to the keystore file.
	 * @param storePass The password used to unlock the keystore.
	 */
	public static synchronized void init(URL keyResource)
	    throws CryptoException {
	
	    if (CRYPTOR == null) {
	        // If encryption is not enabled then do nothing.
	        if (!isEncryptionEnabled() || keyResource == null) {
	            CRYPTOR = new NullCryptor();
	            return;
	        }
	
	        try {
	            // Init given path to keystore, keystore pwd, name of the encrypt key
	            // and flag stating NOT to retrieve keystore from classpath
	            CRYPTOR = SymmetricCryptor.getSymmectricCryptor(keyResource);
	        } catch ( FileNotFoundException e ) {
	            throw new CryptoException(e, ErrorMessageKeys.CM_UTIL_ERR_0068, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0068, e.getMessage()));
	        } catch ( IOException e ) {
	            throw new CryptoException(e, ErrorMessageKeys.CM_UTIL_ERR_0068, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0068, e.getMessage()));
	        }
	    }
	}

	/**
	 * Construct and return a utility that can be used for either encryption
	 * or decryption.
	 *
	 * @return A utility that implements the <code>Cryptor</code> interface
	 *
	 * @throws CryptoException If there was a problem getting the keys
	 *         required to initialize the cipher, or if there was a
	 *         problem initializing the cipher utility
	 */
	public static Cryptor getCryptor() throws CryptoException {
	    init();
	    return CRYPTOR;
	}

	/**
	 * Construct and return a utility that can be used for only encryption.
	 *
	 * @return A utility that implements the <code>Encryptor</code> interface
	 *
	 * @throws CryptoException If there was a problem getting the key
	 *         required to initialize the cipher, or if there was a
	 *         problem initializing the cipher utility
	 */
	public static Encryptor getEncryptor() throws CryptoException {
		return getCryptor();
	}

	/**
	 * Construct and return a utility that can be used for only decryption.
	 *
	 * @return A utility that implements the <code>Decryptor</code> interface
	 *
	 * @throws CryptoException If there was a problem getting the key
	 *         required to initialize the cipher, or if there was a
	 *         problem initializing the cipher utility
	 */
	public static Decryptor getDecryptor() throws CryptoException {
	    return getCryptor();
	}
	
	public static String stringEncrypt(String clearText) throws CryptoException {
		return getCryptor().encrypt(clearText);
	}
	
	public static String stringDecrypt(String cipherText) throws CryptoException {
		return getCryptor().decrypt(cipherText);
	}
	
	public static boolean canDecrypt(String cipherText) {
		try {
            CryptoUtil.getDecryptor().decrypt(cipherText);
        } catch (CryptoException err) {
            return false;
        }
        return true;
	}

	/**
	 * Allows tests to reinit if necessary
	 */
	public static synchronized void reinit() {
	    CRYPTOR = null;
	}

	public static synchronized boolean isInitialized() {
	    return CRYPTOR == null;
	}
 
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			printUsage();			
		}
		
		int i = 0;
		if (args.length == 2 && args[i].equals("-genkey")) { //$NON-NLS-1$
			String keyName = args[++i];
			if (keyName == null) {
				printUsage();
			}
			SymmetricCryptor.generateAndSaveKey(keyName);
			return;
		}
		else if (args.length == 4 && args[i].equals("-key")) { //$NON-NLS-1$
			String keyFile = args[++i];
			if (keyFile == null) {
				printUsage();
			}
			
			File key = new File(keyFile);
			if (!key.exists()) {
				System.out.println("Key file does not exist at "+keyFile); //$NON-NLS-1$
			}
			else {
				CryptoUtil.init(key.toURI().toURL());
			}
			
			++i;
			if (args[i].equals("-encrypt")) { //$NON-NLS-1$
				String clearText = args[++i];
				if (clearText == null) {
					printUsage();
				}
				System.out.println("Encypted Text:"+stringEncrypt(clearText)); //$NON-NLS-1$
				return;
			}		
		}	
		printUsage();
	}
	
	private static void printUsage() {
		System.out.println("java CryptoUtil <option>"); //$NON-NLS-1$
		System.out.println("options:"); //$NON-NLS-1$
		System.out.println("\t-genkey <filename> # Generates password key file"); //$NON-NLS-1$
		System.out.println("\t-key <keyfile> -encrypt <cleartext>   # Encrypts the given clear text string"); //$NON-NLS-1$
		System.exit(0);
	}
}
