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

import org.junit.Test;



public class TestDhKeyGenerator {
	
	@Test
	public void testKeyGenerationDefault() throws CryptoException {
		DhKeyGenerator keyGenServer = new DhKeyGenerator();
		DhKeyGenerator keyGenClient = new DhKeyGenerator();
		byte[] serverKey = keyGenServer.createPublicKey();
		byte[] clientKey = keyGenClient.createPublicKey();
		SymmetricCryptor serverCryptor = keyGenServer.getSymmetricCryptor(clientKey, false, TestDhKeyGenerator.class.getClassLoader());
		SymmetricCryptor clientCryptor = keyGenClient.getSymmetricCryptor(serverKey, false, TestDhKeyGenerator.class.getClassLoader());
		
		String cleartext = "cleartext!"; //$NON-NLS-1$
		
		String ciphertext = serverCryptor.encrypt(cleartext);
		String cleartext2 = clientCryptor.decrypt(ciphertext);
		
		assertEquals(cleartext, cleartext2);
		assertTrue(!ciphertext.equals(cleartext));
		
		Object sealed = serverCryptor.sealObject(cleartext);
		Object unsealed = clientCryptor.unsealObject(sealed);
		
		assertEquals(cleartext, unsealed);
		assertTrue(!sealed.equals(unsealed));
	}

}
