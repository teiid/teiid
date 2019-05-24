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
package org.teiid.transport;

import java.nio.charset.Charset;
import java.util.HashMap;

/**
 * These are mappings between the Postgres  supported character sets to the Java character sets.
 */
public class PGCharsetConverter {
    private static HashMap<String, Charset> charSetMap = new HashMap<String, Charset>();

    static {
        mapCharset("BIG5", Charset.forName("Big5")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("EUC_CN", Charset.forName("GB2312")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("EUC_JP", Charset.forName("EUC-JP")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("EUC_KR", Charset.forName("EUC-KR")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("EUC_TW", Charset.forName("EUC-TW")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("GB18030", Charset.forName("GB18030")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("GBK", Charset.forName("GBK")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("JOHAB", Charset.forName("JOHAB")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("KOI8", Charset.forName("KOI8-U")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("ISO_8859_5", Charset.forName("ISO-8859-5")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("ISO_8859_5", Charset.forName("ISO-8859-6")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("ISO_8859_5", Charset.forName("ISO-8859-7")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("ISO_8859_5", Charset.forName("ISO-8859-8")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("LATIN1", Charset.forName("ISO-8859-1")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("LATIN2", Charset.forName("ISO-8859-2")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("LATIN3", Charset.forName("ISO-8859-3")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("LATIN4", Charset.forName("ISO-8859-4")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("LATIN5", Charset.forName("ISO-8859-9")); //$NON-NLS-1$ //$NON-NLS-2$
        //mapCharset("LATIN6", Charset.forName("ISO-8859-10")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("LATIN7", Charset.forName("ISO-8859-13")); //$NON-NLS-1$ //$NON-NLS-2$
        //mapCharset("LATIN8", Charset.forName("ISO-8859-14")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("LATIN9", Charset.forName("ISO-8859-15")); //$NON-NLS-1$ //$NON-NLS-2$
        //mapCharset("LATIN10", Charset.forName("ISO-8859-16")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("SJIS", Charset.forName("windows-932")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("UHC", Charset.forName("windows-949")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("UTF8", Charset.forName("UTF-8")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("UNICODE", Charset.forName("UTF-8")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("WIN866", Charset.forName("cp866")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("WIN874", Charset.forName("cp874")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("WIN1250", Charset.forName("windows-1250")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("WIN1251", Charset.forName("windows-1251")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("WIN1252", Charset.forName("windows-1252")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("WIN1256", Charset.forName("windows-1256")); //$NON-NLS-1$ //$NON-NLS-2$
        mapCharset("WIN1258", Charset.forName("windows-1258")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    private static void mapCharset(String name, Charset cs) {
        charSetMap.put(name, cs);
    }

    public static Charset getCharset(String name) {
        Charset cs = charSetMap.get(name);
        if (cs == null) {
            try {
                return Charset.forName(name);
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
        return cs;
    }

}
