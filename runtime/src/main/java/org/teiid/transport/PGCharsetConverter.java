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
package org.teiid.transport;

import java.nio.charset.Charset;
import java.util.HashMap;

/**
 * These are mappings between the Postgres  supported character sets to the Java character sets.
 */
public class PGCharsetConverter {
	private static HashMap<String, Charset> charSetMap = new HashMap<String, Charset>();

	static {
		charSetMap.put("BIG5", Charset.forName("Big5")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("EUC_CN", Charset.forName("GB2312")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("EUC_JP", Charset.forName("EUC-JP")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("EUC_KR", Charset.forName("EUC-KR")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("EUC_TW", Charset.forName("EUC-TW")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("GB18030", Charset.forName("GB18030")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("GBK", Charset.forName("GBK")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("JOHAB", Charset.forName("JOHAB")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("KOI8", Charset.forName("KOI8-U")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("ISO_8859_5", Charset.forName("ISO-8859-5")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("ISO_8859_5", Charset.forName("ISO-8859-6")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("ISO_8859_5", Charset.forName("ISO-8859-7")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("ISO_8859_5", Charset.forName("ISO-8859-8")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("LATIN1", Charset.forName("ISO-8859-1")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("LATIN2", Charset.forName("ISO-8859-2")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("LATIN3", Charset.forName("ISO-8859-3")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("LATIN4", Charset.forName("ISO-8859-4")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("LATIN5", Charset.forName("ISO-8859-9")); //$NON-NLS-1$ //$NON-NLS-2$
		//charSetMap.put("LATIN6", Charset.forName("ISO-8859-10")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("LATIN7", Charset.forName("ISO-8859-13")); //$NON-NLS-1$ //$NON-NLS-2$
		//charSetMap.put("LATIN8", Charset.forName("ISO-8859-14")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("LATIN9", Charset.forName("ISO-8859-15")); //$NON-NLS-1$ //$NON-NLS-2$
		//charSetMap.put("LATIN10", Charset.forName("ISO-8859-16")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("SJIS", Charset.forName("windows-932")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("UHC", Charset.forName("windows-949")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("UTF8", Charset.forName("UTF-8")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("WIN866", Charset.forName("cp866")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("WIN874", Charset.forName("cp874")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("WIN1250", Charset.forName("windows-1250")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("WIN1251", Charset.forName("windows-1251")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("WIN1252", Charset.forName("windows-1252")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("WIN1256", Charset.forName("windows-1256")); //$NON-NLS-1$ //$NON-NLS-2$
		charSetMap.put("WIN1258", Charset.forName("windows-1258")); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	public static Charset getCharset(String name) {
		return charSetMap.get(name);
	}
}
