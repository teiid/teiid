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

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Properties;

import javax.crypto.KeyAgreement;
import javax.crypto.spec.DHParameterSpec;

import org.teiid.core.CorePlugin;
import org.teiid.core.TeiidRuntimeException;


/**
 * Helper class that supports anonymous ephemeral Diffie-Hellman
 * 
 * Parameters are stored in the dh.properties file
 */
public class DhKeyGenerator {

	private static String ALGORITHM = "DiffieHellman"; //$NON-NLS-1$
	private static String DIGEST = "SHA-256"; //$NON-NLS-1$
	private static DHParameterSpec DH_SPEC;
	private static DHParameterSpec DH_SPEC_2048;

	static {
		DH_SPEC = loadKeySpecification("dh.properties"); //$NON-NLS-1$
		DH_SPEC_2048 = loadKeySpecification("dh-2048.properties"); //$NON-NLS-1$
	}

	private static DHParameterSpec loadKeySpecification(String propsFile) {
		Properties props = new Properties();
		InputStream is = null;
		try {
			is = DhKeyGenerator.class.getResourceAsStream(propsFile); 
			props.load(is); 
		} catch (IOException e) {
			  throw new TeiidRuntimeException(CorePlugin.Event.TEIID10000, e);
		} finally {
			try {
				if (is != null) {
					is.close();
				}
			} catch (IOException e) {
			}
		}
		BigInteger p = new BigInteger(props.getProperty("p")); //$NON-NLS-1$
		BigInteger g = new BigInteger(props.getProperty("g")); //$NON-NLS-1$
		DHParameterSpec result = new DHParameterSpec(p, g, Integer.parseInt(props
				.getProperty("l"))); //$NON-NLS-1$
		return result;
	}
	
	private PrivateKey privateKey;
	private PrivateKey privateKeyLarge;

	/*
	 * TODO: add support for configurable key sizes
	 */
	private int keySize = SymmetricCryptor.DEFAULT_KEY_BITS;

	public byte[] createPublicKey(boolean large) throws CryptoException {
		try {
			KeyPairGenerator keyGen = KeyPairGenerator.getInstance(ALGORITHM);
			if (large) {
				keyGen.initialize(DH_SPEC_2048);
			} else {
				keyGen.initialize(DH_SPEC);
			}
			KeyPair keypair = keyGen.generateKeyPair();

			if (large) {
				privateKeyLarge = keypair.getPrivate();
			} else {
				privateKey = keypair.getPrivate();
			}
			
			PublicKey publicKey = keypair.getPublic();

			return publicKey.getEncoded();
		} catch (NoSuchAlgorithmException e) {
			  throw new CryptoException(CorePlugin.Event.TEIID10001, e);
		} catch (InvalidAlgorithmParameterException e) {
			  throw new CryptoException(CorePlugin.Event.TEIID10002, e);
		}
	}

	public SymmetricCryptor getSymmetricCryptor(byte[] peerPublicKeyBytes, boolean useSealedObject, ClassLoader classLoader, boolean large, boolean cbc)
			throws CryptoException {
		PrivateKey privKey = large?privateKeyLarge:privateKey;
		if (privKey == null) {
			throw new IllegalStateException(
					"KeyGenerator did not successfully generate public key"); //$NON-NLS-1$
		}
		try {
			X509EncodedKeySpec x509KeySpec = new X509EncodedKeySpec(
					peerPublicKeyBytes);
			KeyFactory keyFact = KeyFactory.getInstance(ALGORITHM);
			PublicKey publicKey = keyFact.generatePublic(x509KeySpec);

			KeyAgreement ka = KeyAgreement.getInstance(ALGORITHM);
			ka.init(privKey);
			ka.doPhase(publicKey, true);
			byte[] secret = ka.generateSecret();
			//we expect a 1024-bit DH key, but vms handle leading zeros differently
			if (secret.length < 128) {
				byte[] temp = new byte[128];
				System.arraycopy(secret, 0, temp, 128-secret.length, secret.length);
				secret = temp;
			}
			//convert to expected bit length for AES
			MessageDigest sha = MessageDigest.getInstance(DIGEST);
			byte[] hash = sha.digest(secret);
			byte[] symKey = new byte[keySize / 8];
			System.arraycopy(hash, 0, symKey, 0, symKey.length);
			SymmetricCryptor sc = SymmetricCryptor.getSymmectricCryptor(symKey, cbc);
			sc.setUseSealedObject(useSealedObject);
			sc.setClassLoader(classLoader);
			return sc;
		} catch (NoSuchAlgorithmException e) {
			  throw new CryptoException(CorePlugin.Event.TEIID10003, e);
		} catch (InvalidKeySpecException e) {
			  throw new CryptoException(CorePlugin.Event.TEIID10004, e);
		} catch (InvalidKeyException e) {
			  throw new CryptoException(CorePlugin.Event.TEIID10005, e);
		}
	}

	/**
	 * Can be used to generate new parameters
	 */
	public static void main(String[] args) throws Exception {
		AlgorithmParameterGenerator paramGen = AlgorithmParameterGenerator
				.getInstance(ALGORITHM);
		paramGen.init(2048);

		AlgorithmParameters params = paramGen.generateParameters();

		DHParameterSpec dhSpec = params.getParameterSpec(DHParameterSpec.class);
		System.out.println("l=" + dhSpec.getL());
		System.out.println("g=" + dhSpec.getG());
		System.out.println("p=" +dhSpec.getP());
	}

}
