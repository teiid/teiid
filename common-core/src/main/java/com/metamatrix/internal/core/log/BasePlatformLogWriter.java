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

package com.metamatrix.internal.core.log;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.metamatrix.core.log.LogListener;
import com.metamatrix.core.log.LogMessage;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.ArgCheck;

/**
 * BasePlatformLogWriter provides the common writer methods
 * used so that information is consistently written across
 * applications.
 */
public abstract class BasePlatformLogWriter implements LogListener {
    //============================================================================================================================
    // Constants
    
    protected static final String PASSWORD = "-password";  // Same as InternalPlatform.PASSWORD //$NON-NLS-1$

    public static final String TIMESTAMP_FORMAT = "MMM dd, yyyy HH:mm:ss.SSS"; //$NON-NLS-1$
    static DateFormat DATE_FORMATTER = new SimpleDateFormat(TIMESTAMP_FORMAT);

    static final String SESSION = "Session";//$NON-NLS-1$
    static final String LEFT_BRACKET = " ["; //$NON-NLS-1$
    static final String RIGHT_BRACKET = "] "; //$NON-NLS-1$
    static final String RIGHT = "> "; //$NON-NLS-1$
    static final String LEFT = " <"; //$NON-NLS-1$
    static final char CENTER = '|';

    static final String LINE_SEPARATOR;
    static final String TAB_STRING = "\t";//$NON-NLS-1$

    //============================================================================================================================
    // Static Initializer

    static {
        String s = System.getProperty("line.separator");//$NON-NLS-1$
        LINE_SEPARATOR = s == null ? "\n" : s;//$NON-NLS-1$
    }

    //============================================================================================================================
    // Variables

    private File logFile = null;

   /**
    * Construct this LogWrier using this constructor
    * indicates the file output may not be used
    */
   protected BasePlatformLogWriter() {}      
   
    /**
     * Construct this LogWriter to write to the supplied {@link java.io.File}
     * @param file the file to which this logger should write.
     */
    public BasePlatformLogWriter( final File file) {
        ArgCheck.isNotNull(file, "BasePlatformLogWriter requires the file to be specified.");       //$NON-NLS-1$
        this.logFile = file;                  
     }
    

    public abstract void logMessage(final LogMessage msg);

    /**
     * Shuts down the platform log.
     */
    public abstract void shutdown();
    
    protected File getLogFile() {
        return this.logFile;
    }
    
    protected void setLogFile(File f) {
        this.logFile = f;
    }

    
    protected void writeHeader(Writer log) throws IOException {
        write(log,SESSION);
        writeSpace(log);
        String date = getDate(System.currentTimeMillis());
        write(log,date);
        writeSpace(log);
        for (int i=SESSION.length()+date.length(); i<78; i++) {
            write(log,"-");//$NON-NLS-1$
        }
        writeln(log);

        // Write out certain values found in System.getProperties()
        try {
            String key = "java.fullversion";//$NON-NLS-1$
            String value = System.getProperty(key);
            if (value == null) {
                key = "java.version";//$NON-NLS-1$
                value = System.getProperty(key);
                writeln(log,key + "=" + value);//$NON-NLS-1$
                key = "java.vendor";//$NON-NLS-1$
                value = System.getProperty(key);
                writeln(log,key + "=" + value);//$NON-NLS-1$
            } else {
                writeln(log,key + "=" + value);//$NON-NLS-1$
            }
        } catch (Exception e) {
            // If we're not allowed to get the values of these properties
            // then just skip over them.
        }

        // The Bootloader has some information that we might be interested in.
        try {
            write(log,"BootLoader constants: OS=" + System.getProperty("os.name"));//$NON-NLS-1$ //$NON-NLS-2$
            writeln(log,", ARCH=" + System.getProperty("os.arch"));//$NON-NLS-1$ //$NON-NLS-2$ 
        } catch (IOException e1) {
            throw e1;
        } catch (Throwable e1) {
            // may be because the boot loader hasn't been started (outside of Eclipse)
        }

    }
    protected static String getDate( final long timestamp ) {
        try {
            return DATE_FORMATTER.format(new Date(timestamp));
        } catch (Exception e) {
            // If there were problems writing out the date, ignore and
            // continue since that shouldn't stop us from losing the rest
            // of the information
        }
        return Long.toString(timestamp);
    }


    protected static void write( final Writer log,final LogMessage msg,
                                 final String depth, final int indexAtDepth ) throws IOException {
        write(log, getDate(msg.getTimestamp()) );
        write(log,LEFT_BRACKET);
        write(log, msg.getThreadName());
        write(log,CENTER);
        if ( depth != null ) {
            write(log,depth);
            write(log,'.');
        }
        write(log,indexAtDepth);
        write(log,RIGHT_BRACKET);
        write(log, MessageLevel.getLabelForLevel(msg.getLevel()));        
        write(log,LEFT);
        final String pluginName = msg.getContext();
        if ( pluginName != null ) {
            write(log, pluginName );
        }
        write(log,CENTER);
        write(log, msg.getErrorCode());
        write(log,RIGHT);
        writeln(log, msg.getText() );
        
        if (msg.getLevel() != MessageLevel.NONE) {
            write(log,msg.getException());
        }
    }

    static void write( final Writer log, final Throwable throwable) throws IOException {
        if (throwable == null) {
            return;
        }
        throwable.printStackTrace(new PrintWriter(log));
    }

    protected static void writeln(Writer log) throws IOException {
        log.write(LINE_SEPARATOR);
    }
    protected static void writeln(Writer log, String s) throws IOException {
        write(log,s);
        writeln(log);
    }
    protected static void write(Writer log, char c) throws IOException {
        log.write(c);      
    }
    protected static void write(Writer log, int i) throws IOException {
        log.write(Integer.toString(i)); 
    }
    protected static void write(Writer log, String message) throws IOException {
        if (message != null)
            log.write(message);                    
    }
    protected static void writeSpace(Writer log) throws IOException {
        log.write(' ');
    }
    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public abstract boolean equals(Object obj);
 
    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    public abstract int hashCode();

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    public abstract String toString();
     

}

