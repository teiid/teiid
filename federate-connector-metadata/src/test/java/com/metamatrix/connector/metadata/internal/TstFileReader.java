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

package com.metamatrix.connector.metadata.internal;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;

import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.StringUtil;

public class TstFileReader {

    private BufferedReader reader;

    public TstFileReader(String fileName) {
        try {
            reader = new BufferedReader(new FileReader(fileName));
        } catch (FileNotFoundException e) {
            throw new MetaMatrixRuntimeException(e);
        }
    }

    public String readQuery(){
        return readLine();
    }
    
    public String readResults(){
        StringWriter writer = new StringWriter();
        String nextLine = readLine();
        while(nextLine != null && nextLine.trim().length()>0){
            writer.write(nextLine);
            writer.write(StringUtil.LINE_SEPARATOR);
            nextLine = readLine();
        }
        return writer.toString();
    }
    
    private String readLine() {
        try {
            return reader.readLine();
        } catch (IOException e) {
            throw new MetaMatrixRuntimeException(e);
        }
    }
}
