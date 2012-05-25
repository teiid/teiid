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

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;


/**
 * <p>Test cases for {@link CryptoFactory} and carious <code>Encryptor</code>
 * and <code>Decryptor</code> implementations. </p>
 */
@SuppressWarnings("nls")
public class TestEncryptDecrypt {

    // Some strings for testing...
    private static final String ALPHA_U   = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"; //$NON-NLS-1$
    private static final String ALPHA_L   = ALPHA_U.toLowerCase();
    private static final String NUMBERS   = "0123456789"; //$NON-NLS-1$
    private static final String MISC_CHAR = "<>,.:;'{}[][]|`~!@#$%^&*()_+-="; //$NON-NLS-1$

    /** String to encrypt and decrypt. */
    private static final String CLEARTEXT = ALPHA_U + ALPHA_L + NUMBERS + MISC_CHAR;
    
    private static Cryptor cryptor;
    
    @BeforeClass public static void oneTimeSetup() throws CryptoException, IOException {
    	cryptor = SymmetricCryptor.getSymmectricCryptor(TestEncryptDecrypt.class.getResource("/teiid.keystore")); //$NON-NLS-1$
    }

    // =========================================================================
    //                      H E L P E R    M E T H O D S
    // =========================================================================

    /**
     * Test encryption (and decryption) for specified string.
     */
    public void helpTestEncryptDecrypt( String cleartext ) throws CryptoException {
//      Encrypt the cleartext into ciphertext
        String ciphertext = cryptor.encrypt( cleartext );
        String cleartext2 = cryptor.decrypt( ciphertext );

        assertTrue(ciphertext.startsWith(BasicCryptor.ENCRYPT_PREFIX));
        
        assertEquals(cleartext, cleartext2);
    }

    // =========================================================================
    //                         T E S T     C A S E S
    // =========================================================================

    /**
     * Test the {@link Cryptor#encrypt} method.
     * @throws CryptoException 
     */
    @Test public void testPos_EncryptDecryptLongString() throws CryptoException {
        helpTestEncryptDecrypt( CLEARTEXT );
    }

    /**
     * Test the {@link Cryptor#encrypt} method.
     * @throws CryptoException 
     */
    @Test public void testPos_EncryptDecryptHalfLongString() throws CryptoException {
        helpTestEncryptDecrypt( CLEARTEXT.substring(0,CLEARTEXT.length()/2) );
    }

    /**
     * Test the {@link Cryptor#encrypt} method.
     * @throws CryptoException 
     */
    @Test public void testPos_EncryptDecryptStringsOfVariousLengths() throws CryptoException {
        for ( int k = 1; k < CLEARTEXT.length()/4; k++ ) {
            // Use substring starting at index k, and 'k' characters long
            String cleartext = CLEARTEXT.substring(k,k+k);
            helpTestEncryptDecrypt( cleartext );
        }
    }

    /**
     * Test the {@link Cryptor#encrypt} method.
     * @throws CryptoException 
     */
    @Test public void testPos_EncryptDecryptStringsOfBlanks() throws CryptoException {
        String BLANKS = "          "; //$NON-NLS-1$
        for ( int k = 1; k < BLANKS.length(); k++ ) {
            // Use substring starting at index 0, and 'k' characters long
            String cleartext = BLANKS.substring(0,k);
            helpTestEncryptDecrypt( cleartext );
        }
    }

    /**
     * Test the {@link Cryptor#encrypt} method.
     * @throws CryptoException 
     */
    @Test public void testNeg_DecryptNonEncryptedStringLen10() throws CryptoException {
        String ciphertext = "abcdefghij";    // Will not decode //$NON-NLS-1$

        try {
            cryptor.decrypt( ciphertext );
            fail("expected exception"); //$NON-NLS-1$
        } catch ( CryptoException e ) {
        } 
    }

    /**
     * Test the {@link Cryptor#encrypt} method.
     */
    @Test public void testNeg_DecryptNullString() throws Exception {
        // Decrypt the Base64 encoded ciphertext back to the original cleartext
        try {
            cryptor.decrypt( (String)null );
            fail("expected exception"); //$NON-NLS-1$
        } catch ( CryptoException e ) {
            //expected
        } 
    }


    /**
     * Test the {@link Cryptor#encrypt} method.
     * @throws CryptoException 
     */
    @Test public void testNeg_EncryptZeroLengthString() throws CryptoException {
        // Encrypt the cleartext and leave ciphertext in Base64 encoded char array
        try {
            cryptor.encrypt( "" ); //$NON-NLS-1$
            fail("expected exception"); //$NON-NLS-1$
        } catch ( CryptoException e ) {
            assertEquals("TEIID10015 Attempt to encrypt zero-length cleartext.", e.getMessage()); //$NON-NLS-1$
        } 
    }

    /**
     * Test the {@link Cryptor#encrypt} method.
     * @throws CryptoException 
     */
    @Test public void testNeg_EncryptNullCharArray() throws CryptoException {
        // Encrypt the cleartext and leave ciphertext in Base64 encoded char array
        try {
            cryptor.encrypt( (String)null );
            fail("expected exception"); //$NON-NLS-1$
        } catch ( CryptoException e ) {
            assertEquals("TEIID10014 Attempt to encrypt null cleartext.", e.getMessage()); //$NON-NLS-1$
        } 
    }

    /**
     * Test the {@link Cryptor#encrypt} method.
     */
    @Test public void testPos_EncryptAfterException() throws Exception {
        try {
            cryptor.encrypt( "" );
        } catch ( CryptoException e ) {
            // This valid test case should work after a failure!
            helpTestEncryptDecrypt( CLEARTEXT );
        } 
    }
        
    @Test public void testLongEncryption() throws Exception {
        helpTestEncryptDecrypt(CLEARTEXT + CLEARTEXT + CLEARTEXT);
    }
    
    @Test public void testSymmetricEncryptionWithRandomKey() throws Exception {
        
        SymmetricCryptor randomSymCryptor = SymmetricCryptor.getSymmectricCryptor();
        
        ArrayList test = new ArrayList(Arrays.asList(new String[] {ALPHA_L, ALPHA_U, CLEARTEXT, NUMBERS}));
        
        Object result = randomSymCryptor.sealObject(test);

        //ensure that we can serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(result);
        
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        result = ois.readObject();
        
        ArrayList clearObject = (ArrayList)randomSymCryptor.unsealObject(result);
        
        assertEquals(test, clearObject);
        
        SymmetricCryptor cryptor1 = SymmetricCryptor.getSymmectricCryptor(randomSymCryptor.getEncodedKey());
        
        clearObject = (ArrayList)cryptor1.unsealObject(result);
        
        assertEquals(test, clearObject);
    }
    
} 

