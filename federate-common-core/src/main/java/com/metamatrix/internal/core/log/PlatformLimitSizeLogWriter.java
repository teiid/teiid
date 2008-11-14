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

package com.metamatrix.internal.core.log;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.io.Writer;

import com.metamatrix.core.log.LogMessage;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.HashCodeUtil;

/**
 * PlatformLogWriter
 * 
 * The file-rollover feature will control the logfile from getting
 * to big.  This will be accomplished by:
 * <li>using the {@link #FILE_SIZE_LIMIT} property, it will control
 * the max. size the file will grow to.  The default is {@link #DEFUALT_ROLLOVER_SIZE}
 * </li>
 * <li>when max. size is reached, the file is closed and renamed
 * </li>
 * <li>the file new name will be formatted using the format
 *      filename_{@link #FILE_GROUP_NAME}_occurance.extension
 * This will allow multiple files in the same day and have them grouped
 * </li>
 * 
* 
 */
public class PlatformLimitSizeLogWriter extends BasePlatformLogWriter {
    //============================================================================================================================
    // Variables

    protected Writer outLog = null;
    protected Writer errorLog = null;
    
    protected FileOutputStream fos = null;
    protected OutputStreamWriter osw = null;     
    
    protected BufferedWriter logBufferedWriter = null;
    protected OutputStreamWriter logStreamWriter = null;
    
    // used for System.out System.err
    protected PrintStream ps = null;

    private boolean resetSystemStreams = true;

    /**
     * Construct this LogWriter to write to the supplied {@link java.io.File}
     * @param file the file to which this logger should write.
     * @param systemOut is the PrintStream that the System.out and .err are 
     * currently pointing to
     */
    public PlatformLimitSizeLogWriter(final File file)  {
        super(file);

        ps = System.out;
        openLogFile();
     }
  
    public PlatformLimitSizeLogWriter(final File file, boolean resetSystemStreams) {
        super(file);
        
        this.resetSystemStreams = resetSystemStreams;
        openLogFile();
    }

    public void  logMessage( final LogMessage msg) {
        try {
           
            boolean isError = false;
            try {
                if ( msg.getLevel() == MessageLevel.ERROR ) {
                    write(errorLog, msg, null, 0);   
                    isError = true;
                } else {
                    write(outLog,msg,null, 0);   
                }
                
            } finally {
                if ( isError ) {
                    errorLog.flush();
                } else {
                    outLog.flush();
                }
            }           
        } catch (Exception e) {
            System.err.println("An exception occurred while writing to the platform log:");//$NON-NLS-1$
            e.printStackTrace(System.err);
            System.err.println("Logging to the console instead.");//$NON-NLS-1$
            //we failed to write, so dump log entry to console instead
            try {
                Writer log = logForStream(System.err);
                write(log,msg,null,0);   
                log.flush();
            } catch (Exception e2) {
                System.err.println("An exception occurred while logging to the console:");//$NON-NLS-1$
                e2.printStackTrace(System.err);
            }
        }
    }
    
    protected static void write(Writer log, String message) throws IOException {
        if (message != null) {
            log.write(message.trim());
            log.flush();
        }
    }    
    
    protected void openLogFile()  {
          
        try {
            
            // if the parent directory does not exist then create them.
            File parent = getLogFile().getParentFile();
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }
            
            fos = new FileOutputStream(getLogFile().getAbsolutePath(), false);
            ps = new PrintStream(fos);
            if(this.resetSystemStreams) {
                System.setOut(ps);
                System.setErr(ps);
            }
             
            osw = new OutputStreamWriter(fos, "UTF-8");//$NON-NLS-1$     
            outLog = new BufferedWriter(osw);
                       
            errorLog = outLog;          
            
            writeHeader(outLog);

        } catch (IOException e) {
            e.printStackTrace();
//            try {
                closeLogFile();
//            } catch (IOException e1) {
//            }
            // there was a problem opening the log file so log to the console
//            outLog = logForStream(System.err);
//            errorLog = outLog;
            
                try {
                    outLog = logForStream(System.out);
                    errorLog = logForStream(System.err);            

                } catch (Exception e2) {
                    e2.printStackTrace();
                }
        }
    }
    protected Writer logForStream(OutputStream output) throws Exception {
        try {
            this.logStreamWriter = new OutputStreamWriter(output, "UTF-8");  //$NON-NLS-1$
            this.logBufferedWriter = new BufferedWriter(this.logStreamWriter);
            return this.logBufferedWriter;
    
        } catch (UnsupportedEncodingException e) {
            this.logStreamWriter = new OutputStreamWriter(output); 
            this.logBufferedWriter = new BufferedWriter(this.logStreamWriter);
            return this.logBufferedWriter;
            
        }
    }
    
    /**
     * Shuts down the platform log.
     */
    public void shutdown() {
       
        try {
            closeLogFile();
            setLogFile(null);

        } catch (Exception e) {
            //we've shutdown the log, so not much else we can do!
            e.printStackTrace();
        }

    }

    protected void closeLogFile()  {
       if (outLog != null ) {
            try {
                    outLog.flush();
             } catch (Exception e) { 
            } 
        }
        if (errorLog != null) {
            try {
                errorLog.flush();
            } catch (Exception e) { 
            } 
        }  
        
        if (logBufferedWriter != null) {
            try {
                    logBufferedWriter.flush();
            } catch (Exception e) { 
            } 
        }   
        
        if (logStreamWriter != null) { 
            try {
                    logStreamWriter.flush();
            } catch (Exception e) { 
            } 
        }
         
        if (osw != null) {
            try {
                osw.flush();
            } catch (IOException e1) {
            }
        }  
        if (fos != null) {
            try {
                fos.flush();
            } catch (IOException e1) {
            }
        }
        
        if (outLog != null ) {
            try {
                    outLog.close();
            } catch (Exception e) { 
                e.printStackTrace();
            } finally {
                outLog = null;
            }
        }    
                  

        if (errorLog != null) {
            try {
                errorLog.close();
            } catch (Exception e) { 
                e.printStackTrace();
            } finally {
                errorLog = null;
            }
        }        
            
        if (logBufferedWriter != null) {
            try {
                    logBufferedWriter.close();
            } catch (Exception e) { 
                e.printStackTrace();
            } finally {
                logBufferedWriter = null;
            }
        }        
            
        if (logStreamWriter != null) { 
            try {
                    logStreamWriter.close();   
            } catch (Exception e) { 
                e.printStackTrace();
            } finally {
                logStreamWriter = null;   
            }
        }        
         
        if (osw != null) {
            try {
                osw.close();                    
            } catch (IOException e1) {
                e1.printStackTrace();
            }finally {
                osw = null;
            }
        }         
      
        
//        if (bos != null) {
//            bos.flush();
//            bos.close();
//            bos = null;
//         }         
        if (ps != null) {
                ps.flush();
                ps.close();
                ps = null;
         } 
         
        if (fos != null) {
            try {
                fos.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }finally {
                fos = null;
            }
        }         
                
                 
    }
   
    
    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object obj) {
        if ( obj == null || !(obj instanceof PlatformLimitSizeLogWriter) ) {
            return false;
        }
        if ( obj == this ) {
            return true;
        }

        PlatformLimitSizeLogWriter that = (PlatformLimitSizeLogWriter)obj;
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
            return "PlatformLimitSizeLogWriter " + this.getLogFile(); //$NON-NLS-1$
        }
        if ( this.outLog != this.errorLog ) {
            return "PlatformLimitSizeLogWriter " + this.outLog + "/" + this.errorLog; //$NON-NLS-1$ //$NON-NLS-2$
        }
        return "PlatformLimitSizeLogWriter " + this.outLog; //$NON-NLS-1$
    }

    /** 
     * @param resetSystemStreams The resetSystemStreams to set.
     * @since 4.3
     */
    public void setResetSystemStreams(boolean resetSystemStreams) {
        this.resetSystemStreams = resetSystemStreams;
    }    
    
}

