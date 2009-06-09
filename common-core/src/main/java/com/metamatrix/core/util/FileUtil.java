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

package com.metamatrix.core.util;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;

import com.metamatrix.core.MetaMatrixRuntimeException;

/**
 * Utility class for dealing with files.  Hides exception handling and file resource management.
 */
public class FileUtil {
    private File file;
    
    public FileUtil(String fileName) {
        this.file = new File(fileName);
    }
    
    public FileUtil(File file) {
        this.file = file;
    }
    
    private FileWriter getWriter(boolean append){
        try {
            return new FileWriter(this.file, append);
        } catch (IOException e) {
            throw new MetaMatrixRuntimeException(e);
        }
    }

    public void append(String text) {
        FileWriter writer = null;
        try {
            try {
                writer = getWriter(true);
                writer.write(text);
            } finally {
                if (writer != null) {
                    writer.close();
                }
            }
        } catch (IOException e) {
            throw new MetaMatrixRuntimeException(e);
       }
    }
    
    public void write(String text) {
        delete();
        append(text);
    }
    
    public void delete() {
        this.file.delete();
    }
    
    public void writeBytes(byte[] bytes) {
        delete();
        FileOutputStream stream = null;
        try {
            try {
                stream = new FileOutputStream(this.file);
                stream.write(bytes);
                stream.flush();
            } finally {
                if (stream != null) {
                    stream.close();
                }
            }
        } catch (FileNotFoundException e) {
            throw new MetaMatrixRuntimeException(e);
        } catch (IOException e) {
            throw new MetaMatrixRuntimeException(e);
        }
    }
    
    public String read() {
        try {
            return readSafe();
        } catch (FileNotFoundException e) {
            throw new MetaMatrixRuntimeException(e);
        }
    }
    
    public String readSafe() throws FileNotFoundException {
        String result;
        FileReader reader = null;
        try {
            reader = new FileReader(this.file);
            result = read(reader);
        } finally {
        	if (reader != null) {
	            try {
	                reader.close();
	            } catch (Exception e) {                
	            }
        	}
        }
        
        return result;
    }
    
    public static String read(Reader reader) {
        StringWriter writer = new StringWriter();
        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(reader);
            while (bufferedReader.ready()) {
                String line = bufferedReader.readLine();     
                writer.write(line);  
                writer.write(StringUtil.LINE_SEPARATOR);
            }
        } catch (IOException e) {
            throw new MetaMatrixRuntimeException(e);
        } finally {
        	if (bufferedReader != null) {
	            try {
	                bufferedReader.close();
	            } catch (Exception e) {                
	            }
        	}
        }
        return writer.toString();
    }
    
    public byte[] readBytes() {
        try {
            return readBytesSafe();
        } catch (FileNotFoundException e) {
            throw new MetaMatrixRuntimeException(e);
        } catch (IOException e) {
            throw new MetaMatrixRuntimeException(e);
        }
    }
    
    public byte[] readBytesSafe() throws FileNotFoundException, IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        FileInputStream input = null;
        try {
            input = new FileInputStream(this.file);
            byte[] buffer = new byte[1024];
            int readCount = input.read(buffer);
            while (readCount > 0) {
                result.write(buffer, 0, readCount);
                readCount = input.read(buffer);
            }            
            return result.toByteArray();
        } finally {
            if (input != null) {
                input.close();
            }
        }
        
    }
}
