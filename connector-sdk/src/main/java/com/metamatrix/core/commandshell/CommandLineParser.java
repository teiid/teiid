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

package com.metamatrix.core.commandshell;

import java.util.ArrayList;

/**
 * Break a String of the form:
 * <command> <arg1> <arg2> ...
 * into a substring array with this structure {<command>, <arg1>, <arg2>...}.
 * Handles spaces in arguments if the argument is enclosed in ""
 */
public class CommandLineParser {

    public CommandLineParser() {
        super();
    }

    public String[] parse(String commandLine) {
        ArrayList result = new ArrayList();
        char[] data = commandLine.toCharArray();
        int index = 0;
        String token = ""; //$NON-NLS-1$
        boolean inString = false;
        while (index < data.length) {
            if (isWhiteSpace(data[index]) && !inString) {
                if (token.length() > 0) {
                    result.add(token);
                    token = ""; //$NON-NLS-1$
                }
            } else if (data[index] == '\"') {
                if (inString) {
                    inString = false;
                    result.add(token);
                    token = ""; //$NON-NLS-1$
                } else {
                    inString = true;
                }
            } else {
                token += data[index];
            }
            index++;
        }
        if (token.length() > 0) {
            result.add(token);
        }
        return (String[]) result.toArray(new String[0]);
    }
    
    private boolean isWhiteSpace(char character) {
        return Character.isWhitespace(character);
    }
}
