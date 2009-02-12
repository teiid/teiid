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
 * Interface defining a utility that can perform decryption of ciphertext to
 * yield the original cleartext.
 */
public interface Decryptor {

    /**
     * Decrypt the ciphertext in byte array format to yield the original
     * cleartext.
     * @param ciphertext The text to be encrypted, in byte form
     * @param The decrypted cleartext, in byte form
     */
    byte[] decrypt( byte[] ciphertext ) throws CryptoException;

    /**
     * Decrypt the ciphertext to yield the original
     * cleartext.
     * @param ciphertext The text to be encrypted
     * @param The decrypted cleartext
     */
    String decrypt( String ciphertext ) throws CryptoException;
    
    Serializable unsealObject(Serializable object) throws CryptoException;
}

