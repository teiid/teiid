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

package org.teiid.netty.handler.codec.serialization;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.SocketTimeoutException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.Streamable;
import org.teiid.core.util.AccessibleBufferedInputStream;
import org.teiid.core.util.ReaderInputStream;

public class TestObjectDecoderInputStream {

	@Test public void testTimeoutException() throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectEncoderOutputStream oeos = new ObjectEncoderOutputStream(new DataOutputStream(baos), 512);
		List<Integer> obj = Arrays.asList(1, 2, 3);
		oeos.writeObject(obj);
		oeos.close();
		final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		InputStream is = new InputStream() {
			int count;
			@Override
			public int read() throws IOException {
				if (count++%2==0) { 
					throw new SocketTimeoutException();
				}
				return bais.read();
			}
		};
		ObjectDecoderInputStream odis = new ObjectDecoderInputStream(new AccessibleBufferedInputStream(is, 1024), Thread.currentThread().getContextClassLoader(), 1024);
		Object result = null;
		do {
			try {
				result = odis.readObject();
			} catch (IOException e) {
				
			}
		} while (result == null);
		assertEquals(obj, result);
	}
	
	@Test public void testReplaceObject() throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectEncoderOutputStream out = new ObjectEncoderOutputStream(new DataOutputStream(baos), 512);
		
		ClobImpl clob = new ClobImpl(new InputStreamFactory() {
			@Override
			public InputStream getInputStream() throws IOException {
				return new ReaderInputStream(new StringReader("Clob contents"),  Charset.forName(Streamable.ENCODING)); //$NON-NLS-1$
			}
			
		}, -1);
		
		out.writeObject(clob);
		
		ObjectDecoderInputStream in = new ObjectDecoderInputStream(new AccessibleBufferedInputStream(new ByteArrayInputStream(baos.toByteArray()), 1024), Thread.currentThread().getContextClassLoader(), 1024);
		Object result = in.readObject();
		assertTrue(result instanceof ClobImpl);
	}	
}
