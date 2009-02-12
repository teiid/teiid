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

import java.io.Serializable;

/**
 * This class implements the {@link com.metamatrix.common.util.crypto.Cryptor Cryptor}
 * interface with methods that return fake data.
 * <br></br>
 * It is used to provide testing of cryptographic functionality without actually
 * doing encryption or decryption.
 */
public class FakeCryptor implements Cryptor, Serializable {
    private String fakeEncrypted;  
    private String fakeDecrypted;  
    
    public FakeCryptor(String fakeEncrypted, String fakeDecrypted) {
        this.fakeEncrypted = fakeEncrypted;
        this.fakeDecrypted = fakeDecrypted;
    }
    
    /**
     * Decrypt the ciphertext in byte array format to yield the original
     * cleartext.
     * @param ciphertext The text to be encrypted, in byte form
     * @param The decrypted cleartext, in byte form
     */
    public byte[] decrypt(byte[] ciphertext) throws CryptoException {
        if (ciphertext.length > 100) {
            throw new CryptoException("can't decrypt: too long"); //$NON-NLS-1$
        } 
        return fakeDecrypted.getBytes(); 
    }

    /**
     * Decrypt the ciphertext in character array format to yield the original
     * cleartext.  This requires a byte-to-char encoding.
     * @param ciphertext The text to be encrypted, in character form
     * @param The decrypted cleartext, in character form
     */
    public String decrypt(String ciphertext) throws CryptoException {
        if (ciphertext.length() > 100) {
            throw new CryptoException("can't decrypt: too long"); //$NON-NLS-1$
        } 
        return fakeDecrypted; 
    }

    /**
     * Encrypt the cleartext in character array format.  This requires
     * a byte-to-char encoding.
     * @param cleartext The text to be encrypted, in character form
     * @param The encrypted ciphertext, in character form
     */
    public String encrypt(String cleartext) throws CryptoException {
        if (cleartext.length() > 100) {
            throw new CryptoException("can't encrypt: too long"); //$NON-NLS-1$
        } 
        return fakeEncrypted; 
    }

    /**
     * Encrypt the cleartext in byte array format.
     * @param cleartext The text to be encrypted, in byte form
     * @param The encrypted ciphertext, in byte form
     */
    public byte[] encrypt(byte[] cleartext) throws CryptoException {
        if (cleartext.length > 100) {
            throw new CryptoException("can't encrypt: too long"); //$NON-NLS-1$
        } 
        return fakeEncrypted.getBytes(); 
    }

    /** 
     * @see com.metamatrix.common.util.crypto.Encryptor#sealObject(java.io.Serializable)
     */
    public Serializable sealObject(Serializable object) throws CryptoException {
        return fakeEncrypted;
    }

    /** 
     * @see com.metamatrix.common.util.crypto.Decryptor#unsealObject(java.io.Serializable)
     */
    public Serializable unsealObject(Serializable object) throws CryptoException {
        return fakeDecrypted;
    }
}
