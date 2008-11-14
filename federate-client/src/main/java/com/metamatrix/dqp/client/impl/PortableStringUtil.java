/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.dqp.client.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import com.metamatrix.core.util.Base64;


/** 
 * @since 4.3
 */
class PortableStringUtil {

    static char EQUALS = '=';
    static char PROP_SEPARATOR = ';';
    static char CTX_SEPARATOR = '|';
    private static char ESCAPE = '\\';
    
    static String escapeString(String val) {
        // escape =, ;, | and the escape char
        StringBuffer buf = new StringBuffer(val);
        for (int i = 0; i < buf.length();i++) {
            char c = buf.charAt(i);
            if (c == EQUALS || c == PROP_SEPARATOR || c == CTX_SEPARATOR || c == ESCAPE) {
                buf.insert(i, ESCAPE);
                i++;
            }
        }
        return buf.toString();
    }
    
    static String unescapeString(String val) {
        StringBuffer buf = new StringBuffer(val);
        for (int i = 0; i < buf.length(); i++) {
            if (buf.charAt(i) == ESCAPE) {
                buf.deleteCharAt(i);
            }
        }
        return buf.toString();
    }
    
    static String encode(Object val) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bout);
        out.writeObject(val);
        out.flush();
        String encodedString = Base64.encodeBytes(bout.toByteArray());
        out.close();
        return escapeString(encodedString);
    }
    
    static Object decode(String encodedString) throws IOException, ClassNotFoundException {
        String unescapedString = unescapeString(encodedString);
        byte[] bytes = Base64.decode(unescapedString);
        ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
        ObjectInputStream in = new ObjectInputStream(bin);
        Object obj = in.readObject();
        in.close();
        return obj;
    }
    
    static String[] getParts(String whole, char separator) {
        if (whole == null || whole.length() == 0) {
            return null;
        }
        char[] chars = whole.toCharArray();
        ArrayList parts = new ArrayList();
        char previous = chars[0];
        char current;
        int currentOffset = 0;
        for (int i = 1; i < chars.length; i++, previous = current) {
            current = chars[i];
            if (current == separator && previous != ESCAPE) {
                parts.add(new String(chars, currentOffset, i-currentOffset));
                currentOffset = i+1;
            }
        }
        if (currentOffset < chars.length) {
            parts.add(new String(chars, currentOffset, chars.length-currentOffset));
        }
        return (String[])parts.toArray(new String[parts.size()]);
    }
}
