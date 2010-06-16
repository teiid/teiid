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

package org.teiid.core.util;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.StringReader;
import java.nio.charset.Charset;

import org.junit.Test;

public class TestReaderInputStream {
	
	@Test public void testUTF8() throws Exception {
		FileInputStream fis = new FileInputStream(UnitTestUtil.getTestDataFile("legal_notice.xml")); //$NON-NLS-1$
		ReaderInputStream ris = new ReaderInputStream(new FileReader(UnitTestUtil.getTestDataFile("legal_notice.xml")), Charset.forName("UTF-8")); //$NON-NLS-1$ //$NON-NLS-2$
		
		int value;
		while (true) {
			value = fis.read();
			assertEquals(value, ris.read());
			if (value == -1) {
				break;
			}
		}
	}
	
	@Test public void testUTF16() throws Exception {
		String actual = "!?abc"; //$NON-NLS-1$
		ReaderInputStream ris = new ReaderInputStream(new StringReader(actual), Charset.forName("UTF-16"), 1); //$NON-NLS-1$
		byte[] result = new byte[(actual.length()) * 2 + 2];
		ris.read(result);
		String resultString = new String(result, "UTF-16"); //$NON-NLS-1$
		assertEquals(resultString, actual);
	}
	
	@Test public void testASCII() throws Exception  {
		String actual = "!?abc"; //$NON-NLS-1$
		ReaderInputStream ris = new ReaderInputStream(new StringReader(actual), Charset.forName("US-ASCII"), 1); //$NON-NLS-1$
		byte[] result = new byte[actual.length()];
		ris.read(result);
		String resultString = new String(result, "US-ASCII"); //$NON-NLS-1$
		assertEquals(resultString, actual);		
	}

}
