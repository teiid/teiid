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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

/** 
 * File based decorator for PrintStream
 */
public class FilePrintStream extends PrintStream {
    PrintStream delegate;
    PrintStream logger;
    
    public FilePrintStream(PrintStream out, String logFile) {
        super(out);
        delegate = out;
        try {
            logger = new PrintStream(new FileOutputStream(logFile));
        } catch (IOException e) {
            logger = null;
        }
    }

    public void close() {
        if (logger != null) {
            this.logger.close();
        }
        this.delegate.close();
    }

    public void flush() {
        if (logger != null) {
            this.logger.flush();
        }        
        this.delegate.flush();
    }

    public void print(boolean b) {
        if (logger != null) {
            this.logger.print(b);
            this.logger.flush();
        }        
        this.delegate.print(b);
    }

    public void print(char c) {
        if (logger != null) {
            this.logger.print(c);
            this.logger.flush();
        }        
        this.delegate.print(c);
    }


    public void print(char[] s) {
        if (logger != null) {
            this.logger.print(s);
            this.logger.flush();
        }        
        this.delegate.print(s);
    }

    public void print(double d) {
        if (logger != null) {
            this.logger.print(d);
            this.logger.flush();
        }        
        this.delegate.print(d);
    }

    public void print(float f) {
        if (logger != null) {
            this.logger.print(f);
            this.logger.flush();
        }        
        this.delegate.print(f);
    }

    public void print(int i) {
        if (logger != null) {
            this.logger.print(i);
            this.logger.flush();
        }        
        this.delegate.print(i);
    }

    public void print(long l) {
        if (logger != null) {
            this.logger.print(l);
            this.logger.flush();
        }        
        this.delegate.print(l);
    }

    public void print(Object obj) {
        if (logger != null) {
            this.logger.print(obj);
            this.logger.flush();
        }        
        this.delegate.print(obj);
    }

    public void print(String s) {
        if (logger != null) {
            this.logger.print(s);
            this.logger.flush();
        }
        this.delegate.print(s);
    }

    public void println() {
        if (logger != null) {
            this.logger.println();
            this.logger.flush();
        }        
        this.delegate.println();
    }

    public void println(boolean x) {
        if (logger != null) {
            this.logger.println(x);
            this.logger.flush();
        }        
        this.delegate.println(x);
    }

    public void println(char x) {
        if (logger != null) {
            this.logger.println(x);
            this.logger.flush();
        }        
        this.delegate.println(x);
    }

    public void println(char[] x) {
        if (logger != null) {
            this.logger.println(x);
            this.logger.flush();
        }        
        this.delegate.println(x);
    }

    public void println(double x) {
        if (logger != null) {
            this.logger.println(x);
            this.logger.flush();
        }        
        this.delegate.println(x);
    }

    public void println(float x) {
        if (logger != null) {
            this.logger.println(x);
            this.logger.flush();
        }        
        this.delegate.println(x);
    }

    public void println(int x) {
        if (logger != null) {
            this.logger.println(x);
            this.logger.flush();
        }        
        this.delegate.println(x);
    }

    public void println(long x) {
        if (logger != null) {
            this.logger.println(x);
            this.logger.flush();
        }        
        this.delegate.println(x);
    }

    public void println(Object x) {
        if (logger != null) {
            this.logger.println(x);
            this.logger.flush();
        }        
        this.delegate.println(x);
    }

    public void println(String x) {
        if (logger != null) {
            this.logger.println(x);
            this.logger.flush();
        }        
        this.delegate.println(x);
    }

    public void write(int c) {
        if (logger != null) {
            this.logger.write(c);
            this.logger.flush();
        }        
        this.delegate.write(c);
    }

    public void write(byte[] buf,
                      int off,
                      int len) {
        if (this.logger != null) {
            this.logger.write(buf, off, len);
            this.logger.flush();
        }
        delegate.write(buf, off, len);
    }
}
