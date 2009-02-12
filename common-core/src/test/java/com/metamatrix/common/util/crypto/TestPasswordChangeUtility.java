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

import java.util.Properties;

import junit.framework.TestCase;

/**
 * <p>Test cases for PasswordChangeUtility
 */
public class TestPasswordChangeUtility extends TestCase {

    private static final String PASSWORD = "PASSWORD"; //$NON-NLS-1$
    
    private static final String KEY1 = "key1"; //$NON-NLS-1$
    private static final String KEY2 = "password"; //$NON-NLS-1$
    private static final String KEY3 = "a.paSSword"; //$NON-NLS-1$
    private static final String KEY4 = "password.b"; //$NON-NLS-1$
    
    private static final String VALUE1 = "value1"; //$NON-NLS-1$
    private static final String VALUE2 = "value2"; //$NON-NLS-1$
    private static final String VALUE3 = "value3"; //$NON-NLS-1$
    private static final String VALUE4 = "value4"; //$NON-NLS-1$
    
    
    public static final String FAKE_DECRYPTED_OLD = "fake-decrypted-old"; //$NON-NLS-1$
    public static final String FAKE_ENCRYPTED_OLD = "fake-encrypted-old"; //$NON-NLS-1$
    public static final String FAKE_DECRYPTED_NEW = "fake-decrypted-new"; //$NON-NLS-1$
    public static final String FAKE_ENCRYPTED_NEW = "fake-encrypted-new"; //$NON-NLS-1$
    
    
    
    
    private PasswordChangeUtility utility;
    private Properties properties;

    /** Construct test case. */
    public TestPasswordChangeUtility(String name) {
        super(name);
    }

    public void setUp() {
        utility = new FakePasswordChangeUtility();
        
        properties = new Properties();
        properties.put(KEY1, VALUE1);
        properties.put(KEY2, VALUE2);
        properties.put(KEY3, VALUE3);
        properties.put(KEY4, VALUE4);
    }
  
    /**Test PasswordChangeUtility.oldEncrypt()*/
    public void testOldEncryptProperties() throws Exception {
        properties = utility.oldEncrypt(PASSWORD, properties);
        
        assertEquals(VALUE1, properties.get(KEY1));
        assertEquals(FAKE_ENCRYPTED_OLD, properties.get(KEY2));
        assertEquals(FAKE_ENCRYPTED_OLD, properties.get(KEY3));
        assertEquals(VALUE4, properties.get(KEY4));
    }
    
    /**Test PasswordChangeUtility.newEncrypt()*/
    public void testNewEncryptProperties() throws Exception {
        properties = utility.newEncrypt(PASSWORD, properties);
        
        assertEquals(VALUE1, properties.get(KEY1));
        assertEquals(FAKE_ENCRYPTED_NEW, properties.get(KEY2));
        assertEquals(FAKE_ENCRYPTED_NEW, properties.get(KEY3));
        assertEquals(VALUE4, properties.get(KEY4));
    }
    
    /**Test PasswordChangeUtility.oldDecrypt()*/
    public void testOldDecryptProperties() throws Exception {
        properties = utility.oldDecrypt(PASSWORD, properties);
        
        assertEquals(VALUE1, properties.get(KEY1));
        assertEquals(FAKE_DECRYPTED_OLD, properties.get(KEY2));
        assertEquals(FAKE_DECRYPTED_OLD, properties.get(KEY3));
        assertEquals(VALUE4, properties.get(KEY4));
    }
    
    /**Test PasswordChangeUtility.newDecrypt()*/
    public void testNewDecryptProperties() throws Exception {
        properties = utility.newDecrypt(PASSWORD, properties);
        
        assertEquals(VALUE1, properties.get(KEY1));
        assertEquals(FAKE_DECRYPTED_NEW, properties.get(KEY2));
        assertEquals(FAKE_DECRYPTED_NEW, properties.get(KEY3));
        assertEquals(VALUE4, properties.get(KEY4));
    }

    
    
    /**
     * Subclass of PasswordChangeUtility, extended to use fake cryptors.
     * @since 4.3
     */
    public static class FakePasswordChangeUtility extends PasswordChangeUtility {
        public FakePasswordChangeUtility() {
            super("fake", "fake"); //$NON-NLS-1$//$NON-NLS-2$//$NON-NLS-3$//$NON-NLS-4$
        }
        
        public void init() throws CryptoException {
            this.oldCryptor = new FakeCryptor(FAKE_ENCRYPTED_OLD, FAKE_DECRYPTED_OLD);
            this.newCryptor = new FakeCryptor(FAKE_ENCRYPTED_NEW, FAKE_DECRYPTED_NEW);
            this.initialized = true;
        }
    }
} 

