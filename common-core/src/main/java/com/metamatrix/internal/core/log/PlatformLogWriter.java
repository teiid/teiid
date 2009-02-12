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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;

import com.metamatrix.core.log.LogMessage;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.HashCodeUtil;

/**
 * PlatformLogWriter
 */
public class PlatformLogWriter extends BasePlatformLogWriter {

    //============================================================================================================================
	// Variables

    protected Writer outLog = null;
    protected Writer errorLog = null;
    protected boolean newSession = true;
    private boolean outLogIsSystem = false;
    private boolean errLogIsSystem = false;

    /**
     * Construct this LogWriter to write to the supplied {@link java.io.File}
     * @param file the file to which this logger should write.
     */
    public PlatformLogWriter( final File file) {
        super(file);
    }
    /**
     * Construct this LogWriter to write to the supplied {@link java.io.OutputStream}
     * @param out the stream to which this logger should write.
     */
    public PlatformLogWriter(OutputStream out) {
        this(out,out);
        
    }
    /**
     * Construct this LogWriter to write to the supplied {@link java.io.OutputStream}
     * @param out the stream to which this logger should write.
     */
    public PlatformLogWriter(OutputStream out, OutputStream error ) {
        super();

        this.outLog = logForStream(out);
        this.errorLog = logForStream(error);
        if ( out == System.out || out == System.err ) {
            outLogIsSystem = true;
        }
        if ( error == System.out || error == System.err ) {
            errLogIsSystem = true;
        }
        
    }
    
    
    public synchronized void logMessage( final LogMessage msg) {
        // thread safety: (Concurrency003)
 
        if (getLogFile() != null)
            openLogFile();
        if (outLog == null)
            outLog = logForStream(System.out);
        if (errorLog == null)
            errorLog = logForStream(System.err);
        try {
            boolean isError = false;
            try {
                if ( msg.getLevel() == MessageLevel.ERROR ) {
                    write(errorLog, msg, null, 0);   
                    isError = true;
                } else {
                    write(outLog, msg, null,0);   
                }
            } finally {
//                if (logFile != null) {
//                    closeLogFile();
//                } else {
                    if ( isError ) {
                        errorLog.flush();
                    } else {
                        outLog.flush();
                    }
//                }
            }           
        } catch (Exception e) {
            System.err.println("An exception occurred while writing to the platform log:");//$NON-NLS-1$
            e.printStackTrace(System.err);
            System.err.println("Logging to the console instead.");//$NON-NLS-1$
            //we failed to write, so dump log entry to console instead
            try {
                Writer log = logForStream(System.err);
                write(log, msg, null,0);   
                log.flush();
            } catch (Exception e2) {
                System.err.println("An exception occurred while logging to the console:");//$NON-NLS-1$
                e2.printStackTrace(System.err);
            }
//        } finally {
//            outLog = null;
        }
    }

    protected void closeLogFile() throws IOException {
        try {
            if (outLog != null) {
                outLog.flush();
                outLog.close();
            }
        } finally {
            outLog = null;
        }
    }
    protected void openLogFile() {
        try {
            
            outLog = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(getLogFile().getAbsolutePath(), true), "UTF-8"));//$NON-NLS-1$
            errorLog = outLog;
            if (newSession) {
                writeHeader(outLog);
                newSession = false;
            }
            
        } catch (IOException e) {
            // there was a problem opening the log file so log to the console
            outLog = logForStream(System.err);
            errorLog = outLog;
        }
    }
    
    protected Writer logForStream(OutputStream output) {
        try {
            return new BufferedWriter(new OutputStreamWriter(output, "UTF-8"));//$NON-NLS-1$
        } catch (UnsupportedEncodingException e) {
            return new BufferedWriter(new OutputStreamWriter(output));
        }
    }
    /**
     * Shuts down the platform log.
     */
    public synchronized void shutdown() {
        try {
            if (getLogFile() != null) {
                closeLogFile();
                setLogFile(null);
            } else {
                
                if (outLog != null && !outLogIsSystem ) {  // don't close System.out
                    Writer old = outLog;
                    outLog = null;
                    old.flush();
                    old.close();
                }
                if (errorLog != null && !errLogIsSystem ) {  // don't close System.out
                    Writer old = errorLog;
                    errorLog = null;
                    old.flush();
                    old.close();
                }
            }
        } catch (Exception e) {
            //we've shutdown the log, so not much else we can do!
            e.printStackTrace();
        }
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object obj) {
        if ( obj == null || !(obj instanceof PlatformLogWriter) ) {
            return false;
        }
        if ( obj == this ) {
            return true;
        }
        PlatformLogWriter that = (PlatformLogWriter)obj;
        if ( getLogFile() != null ) {
            return getLogFile().equals(that.getLogFile());
        }
        if ( that.getLogFile() != null ) {
            return false;   // this.logFile is null
        }
        return ( this.outLog == that.outLog && this.errorLog == that.errorLog );
    }    
    
    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        int hash = 0;
        if ( this.getLogFile() != null ) {
            hash = HashCodeUtil.hashCode(hash,this.getLogFile());
        } else {
            hash = HashCodeUtil.hashCode(hash,this.outLog);
            hash = HashCodeUtil.hashCode(hash,this.errorLog);
        }
        //hash = HashCodeUtil.hashCode(hash,this.newSession);
        return hash;
    }    


    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    public String toString() {
        if ( this.getLogFile() != null ) {
            return "PlatformLogWriter " + this.getLogFile(); //$NON-NLS-1$
        }
        if ( this.outLog != this.errorLog ) {
            return "PlatformLogWriter " + this.outLog + "/" + this.errorLog; //$NON-NLS-1$ //$NON-NLS-2$
        }
        return "PlatformLogWriter " + this.outLog; //$NON-NLS-1$
    }

}
