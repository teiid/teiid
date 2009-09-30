/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 *
 * Copyright (c) 2000, 2008 IBM Corporation and others.
 * All rights reserved.
 * This code is made available under the terms of the Eclipse Public
 * License, version 1.0.
 */

package org.teiid.test.util;


/**
 * This is a common place to put String utility methods.
 */
public final class StringUtil {


    
    public static String removeChars(final String value, final char[] chars) {
        final StringBuffer result = new StringBuffer();
        if (value != null && chars != null && chars.length > 0) {
            final String removeChars = String.valueOf(chars);
            for (int i = 0; i < value.length(); i++) {
                final String character = value.substring(i, i + 1);
                if (removeChars.indexOf(character) == -1) {
                    result.append(character);
                }
            }
        } else {
            result.append(value);
        }
        return result.toString();
    }


}
