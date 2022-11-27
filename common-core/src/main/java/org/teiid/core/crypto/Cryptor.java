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


/**
 * Interface defining a utility that can perform both encryption and decryption.
 */
public interface Cryptor {

    /**
     * Encrypt the cleartext in byte array format.
     * @param cleartext The text to be encrypted, in byte form
     * @return The encrypted ciphertext, in byte form
     */
    byte[] encrypt( byte[] cleartext ) throws CryptoException;

    Object sealObject(Object object) throws CryptoException;

    /**
     * Decrypt the ciphertext in byte array format to yield the original
     * cleartext.
     * @param ciphertext The text to be encrypted, in byte form
     * @return The decrypted cleartext, in byte form
     */
    byte[] decrypt( byte[] ciphertext ) throws CryptoException;

    Object unsealObject(Object object) throws CryptoException;

}
