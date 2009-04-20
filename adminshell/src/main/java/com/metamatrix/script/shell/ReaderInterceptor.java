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

package com.metamatrix.script.shell;
/*
 * Copyright � 2000-2005 MetaMatrix, Inc.
 * All rights reserved.
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;

import bsh.Interpreter;

/** 
 * This class used in conjunction with BeanShell, to hijack the System.in
 * and write a wrapper on top it,so that any commands entered in its console
 * be passed through this code before any thing else happens.
 * 
 * This will let us write code such as
 * "select * from foo"
 * on the bean shell command line, and we can intercept and issue right beanshell
 * command to execute the same.
 * 
 */
public class ReaderInterceptor extends Reader {
    private BufferedReader in = null;     // Stream to Sniff
    private CustomParser parser = null;
    private Writer logger;
    
    public ReaderInterceptor(CustomParser parser, Writer logger) {
        this(new InputStreamReader(System.in), parser, logger);
    }
    
    public ReaderInterceptor(Reader in, CustomParser parser, Writer logger) {
        this.parser = parser;
        this.in = new BufferedReader(in);
        this.logger = logger;
    }
    
    /** 
     * @see java.io.Reader#close()
     * @since 4.3
     */
    public void close() throws IOException {
        in.close();
    }

    private String currentStr = null;
    private int currentIndex = 0;
    
    /** 
     * @see java.io.Reader#read(char[], int, int)
     * @since 4.3
     */
    public int read(char[] cbuf, int off, int len) throws IOException {
        int newLine=0;
        while(true) {
            // if no line has been read then read the line
            if (currentStr == null) {
                
                StringBuffer sb = new StringBuffer();
                String line = in.readLine();             
                
                while (line != null){
                    
                    if (line.length() > 0) {
                        newLine = 0;
                        sb.append(line);
                    }
                    else {
                        newLine++;
                        if (newLine > 0) {
                          sb.append(";"); //$NON-NLS-1$
                          line = ";"; //$NON-NLS-1$
                        }
                    }
                    
                    currentStr = parser.convert(sb.toString().trim());
                    
                    if (currentStr.endsWith(";")) {   //$NON-NLS-1$                                     
                        if (logger != null && !currentStr.equals(";")) { //$NON-NLS-1$
                        	currentStr += "\n"; //$NON-NLS-1$
                            logger.write(currentStr);
                            logger.write(System.getProperty("line.separator")); //$NON-NLS-1$
                            logger.flush();
                        }
                        currentIndex = 0;                        
                        break;
                    }
                    
                    if (line.length() > 0) {
                        sb.append(" "); //$NON-NLS-1$
                    }
                    line = in.readLine();
                }
            }
            else {
                int lengthToCopy = ((currentStr.length()-currentIndex) < len)?(currentStr.length()-currentIndex):len;
                if (lengthToCopy == 0) {
                    currentStr = System.getProperty("line.separator")+System.getProperty("line.separator"); //$NON-NLS-1$ //$NON-NLS-2$
                    lengthToCopy = ((currentStr.length()-currentIndex) < len)?(currentStr.length()-currentIndex):len;
                }
                System.arraycopy(currentStr.toCharArray(), currentIndex, cbuf, off, lengthToCopy);
                currentIndex = currentIndex+lengthToCopy;
                if (currentIndex == currentStr.length()) {
                    currentStr = null;                
                }
                return lengthToCopy;           
            }
        }
    }
        
     
    public static void main(String[] args) {
        final String ls = System.getProperty("line.separator"); //$NON-NLS-1$
        CustomParser p = new CustomParser() {
            public String convert(String str) {
                System.out.println(str);
                if (str.matches("select .+|insert into.+|delete .+|update table .*|exec .+")){ //$NON-NLS-1$
                    return "execute(\""+str.substring(0,str.length()-1)+"\");"+ls; //$NON-NLS-1$ //$NON-NLS-2$
                }
                return "foo:"+str+ls; //$NON-NLS-1$
            }

            public void setInterpreter(Interpreter i) {
            }            
        };
        
        BufferedReader i = new BufferedReader(new ReaderInterceptor(p, null));
        
        try {
            String line = i.readLine();            
            while(line != null) {
                System.out.println(line); 
                line = i.readLine();
            }
        } catch (IOException err) {
            err.printStackTrace();
        }
    }
}
