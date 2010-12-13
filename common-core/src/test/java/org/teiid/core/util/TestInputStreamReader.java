package org.teiid.core.util;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;

import org.junit.Test;

@SuppressWarnings("nls")
public class TestInputStreamReader {

	@Test public void testMultiByte() throws Exception {
		InputStreamReader isr = new InputStreamReader(new ByteArrayInputStream(new byte[] {(byte)80, (byte)-61, (byte)-70}), Charset.forName("UTF-8").newDecoder(), 2);
		assertEquals(80, isr.read());
		assertEquals(250, isr.read());
	}
}
