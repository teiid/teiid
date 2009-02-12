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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.Key;

import javax.crypto.KeyGenerator;
import javax.crypto.spec.SecretKeySpec;

import com.metamatrix.common.util.ByteArrayHelper;
import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.core.util.ArgCheck;

/**
 * Provides a symmetric cryptor
 * 
 */
public class SymmetricCryptor extends BasicCryptor {
    
    public static final String DEFAULT_SYM_KEY_ALGORITHM = "AES"; //$NON-NLS-1$
    public static final String DEFAULT_SYM_ALGORITHM = "AES/ECB/PKCS5Padding"; //$NON-NLS-1$
    public static final int DEFAULT_KEY_BITS = 128; 
    private static KeyGenerator keyGen;
    
    /**
     * Creates a new SymmetricCryptor with a new symmetric key 
     *  
     * @return a new SymmetricCryptor
     * @throws CryptoException
     */
    public static SymmetricCryptor getSymmectricCryptor() throws CryptoException {
        Key key = generateKey();
        
        return new SymmetricCryptor(key);
    }

	private static Key generateKey() throws CryptoException {
		try {
            synchronized(SymmetricCryptor.class) {
                if (keyGen == null) {
                    keyGen = KeyGenerator.getInstance(DEFAULT_SYM_KEY_ALGORITHM);
                }
                keyGen.init(DEFAULT_KEY_BITS);
                return keyGen.generateKey();
            }
        } catch (GeneralSecurityException e) {
            throw new CryptoException(e);
        }
	}
    
    /**
     * Creates a SymmetricCryptor using the supplied URL contents as the key
     *  
     * @param key
     * @return a new SymmetricCryptor
     * @throws CryptoException
     * @throws IOException 
     */
    public static SymmetricCryptor getSymmectricCryptor(URL keyResource) throws CryptoException, IOException {
		return getSymmectricCryptor(loadKey(keyResource));
    }
    
    /**
     * Creates a SymmetricCryptor using the supplied byte array as the key
     *  
     * @param key
     * @return a new SymmetricCryptor
     * @throws CryptoException
     */
    public static SymmetricCryptor getSymmectricCryptor(byte[] key) throws CryptoException {
        Key secretKey = new SecretKeySpec(key, DEFAULT_SYM_KEY_ALGORITHM);
        return new SymmetricCryptor(secretKey);
    }
    
    public static void generateAndSaveKey(String file) throws CryptoException, IOException {
    	Key key = generateKey();
    	FileOutputStream fos = new FileOutputStream(file);
    	try {
    		fos.write(key.getEncoded());
    	} finally {
    		fos.close();
    	}
    }
    
    SymmetricCryptor(Key key) throws CryptoException {
        super(key, key, DEFAULT_SYM_ALGORITHM);
    }

    public byte[] getEncodedKey() {
        return this.decryptKey.getEncoded();
    }
    
	private static byte[] loadKey(URL keyResource) throws IOException {
		ArgCheck.isNotNull(keyResource);
		InputStream stream = keyResource.openStream();
		try {
			return ByteArrayHelper.toByteArray(keyResource.openStream());
		} finally {
			stream.close();
		}
	}
    
	public static void main(String[] args) throws Exception {
		SymmetricCryptor.generateAndSaveKey("cluster.key");
	}
}
