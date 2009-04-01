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

package com.metamatrix.core.log;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.metamatrix.core.CorePlugin;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.internal.core.log.PlatformLimitSizeLogWriter;

/**
 * The FileLogWriter is a {@link LogListener} that writes (appends) to a supplied file.
 * 
 */
public class FileLimitSizeLogWriter  implements LogListener {
    
    /**
     * The FILE_SIZE_LIMIT controls the file size limit at which the
     * file will rolled-over (i.e., when it closed and renamed due
     * to size).  The default size limit will be 5 Mbs.  
     * {@see #FILE_GROUP_NAME}
     */
    public static final String FILE_SIZE_LIMIT = "metamatrix.log.size.limit.kbs"; //$NON-NLS-1$
    /**
     * the FILE_SIZE_MONITOR_TIME interval is in minutes and 
     * controls when the monitoring thread will determine if the 
     * file has reach the
     * {@see #FILE_SIZE_LIMIT LIMIT} to be swapped out.
     */
    public static final String FILE_SIZE_MONITOR_TIME = "metamatrix.log.size.monitor.mins"; //$NON-NLS-1$
    
    /**
     * The ARCHIVE_PREFIX is a prefixed value to the renamed log file.
     * This is used so that the savelogs.cmd - .sh scripts can
     * find the archived files, zip them up and then delete
     */
    public static final String ARCHIVE_PREFIX =  "a_"; //$NON-NLS-1$
    
    // defaults for the file size limit and interval
    private static final long DEFUALT_ROLLOVER_SIZE = 1000; // in kbs in size
    private static final int DEFAULT_WAIT_TIME = 60; // 60 secs or 1 min
               
    // these are the muliplier applied to the parameter values
    // for FILE_SIZE_LIMIT and FILE_SIZE_MONITOR_TIME
    private static final long SIZE_MULTIPLIER = 1000;
    private static final long MIN_MULTIPLIER = 60000;
   
    // the formatted date used in the naming of the 
    // swapped out file
    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd_HH-mm"; //$NON-NLS-1$

    static DateFormat DATE_FORMATTER = new SimpleDateFormat(TIMESTAMP_FORMAT);
   
    // monitoring thread
    private FileSizeMonitorThread rollOverThread = null;
    // the primary log file written to
    private File currentLogFile = null;
    private PlatformLimitSizeLogWriter limitwriter = null;

    
    private String prefixName = null;
    private String suffixName = null;
    private File parentFile = null;
    private long filecnt = 1;
    private Properties props;    
    // List locking
    private ReadWriteLock rwlock = new ReentrantReadWriteLock();
   
   
    public FileLimitSizeLogWriter( final File file ) {
        if (file == null) {
            final String msg = CorePlugin.Util.getString("FileLogWriter.The_File_reference_may_not_be_null"); //$NON-NLS-1$
            throw new IllegalArgumentException(msg);
        }

        currentLogFile = file;
        rolloverPreviousLogFile(currentLogFile);
        createPlatformLogWriter(currentLogFile) ;       
        init();        
    }   
    
    // For use in DQP - allows the user to pass a parameter through to reset the System output streams
    public FileLimitSizeLogWriter( final File file, boolean resetSystemStreams) {
        if (file == null) {
            final String msg = CorePlugin.Util.getString("FileLogWriter.The_File_reference_may_not_be_null"); //$NON-NLS-1$
            throw new IllegalArgumentException(msg);
        }

        currentLogFile = file;
        rolloverPreviousLogFile(currentLogFile);
        createPlatformLogWriter(currentLogFile, resetSystemStreams) ;       
        init();        
    }    
    
    public FileLimitSizeLogWriter( final File file, Properties properties ) {
        if (file == null) {
            final String msg = CorePlugin.Util.getString("FileLogWriter.The_File_reference_may_not_be_null"); //$NON-NLS-1$
            throw new IllegalArgumentException(msg);
        }
        props = properties;
        currentLogFile = file;
        rolloverPreviousLogFile(currentLogFile);
        createPlatformLogWriter(currentLogFile) ;       
        init();        
    }
    
    public FileLimitSizeLogWriter( final File file, Properties properties, boolean rollover ) {
        if (file == null) {
            final String msg = CorePlugin.Util.getString("FileLogWriter.The_File_reference_may_not_be_null"); //$NON-NLS-1$
            throw new IllegalArgumentException(msg);
        }
        props = properties;
        currentLogFile = file;
        if (rollover) {
            rolloverPreviousLogFile(currentLogFile);
        }
        createPlatformLogWriter(currentLogFile) ;       
        init();        
    }    
    
    /**
     * initialize the variables used to keep track and
     * assign the next log file name
     *
     */
    private void init() {
        
        parentFile = currentLogFile.getParentFile();
        String name = currentLogFile.getName();
            
        int pos = name.lastIndexOf(PERIOD);
        String first = null;
        String ext = null;
        String period = EMPTY;
        if (pos > 0) {
            ext = name.substring(pos + 1);
            first = name.substring(0, pos);
            period = PERIOD;
        } else {
            first = name;
            ext = EMPTY;
        }
        long fileSizeLimit = DEFUALT_ROLLOVER_SIZE;
        long timeLimit = DEFAULT_WAIT_TIME;
        if (props != null) {
    
            String limit = props.getProperty(FILE_SIZE_LIMIT);
            try {
                if (limit != null) {
                    fileSizeLimit = Long.parseLong(limit);    
                }            
            } catch(Throwable t) {
                fileSizeLimit = DEFUALT_ROLLOVER_SIZE;
            }
            
            String time = props.getProperty(FILE_SIZE_MONITOR_TIME);
            try {
                if (time != null) {
                    timeLimit = Long.parseLong(time);    
                }            
            } catch(Throwable t) {
                timeLimit = DEFAULT_WAIT_TIME;
            }            
        }
          
        prefixName = first;            
            
        StringBuffer suffix = new StringBuffer(period);
        suffix.append(ext);
        suffixName = suffix.toString();
        
        rollOverThread  = new FileSizeMonitorThread(fileSizeLimit, timeLimit, this );
        rollOverThread.start();
            
    }
    
   
    protected File getLogFile() {
        return this.currentLogFile;
    }
    
    
    protected void createPlatformLogWriter(File file) {
        limitwriter = new PlatformLimitSizeLogWriter(file);
    }  
    
    protected void createPlatformLogWriter(File file, boolean resetSystemStreams) {
        limitwriter = new PlatformLimitSizeLogWriter(file, resetSystemStreams);
    }  
    
    protected PlatformLimitSizeLogWriter getPlatformLogWriter() {
        return this.limitwriter;
    }      
    

    public void logMessage(LogMessage msg) {
        rwlock.readLock().lock();
        try {
            if (getPlatformLogWriter() != null) {
                getPlatformLogWriter().logMessage(msg);
            }

        } finally {
            rwlock.readLock().unlock();
        }
    }
    /**
     * perform the file rollover process
     *
     */
    protected void performRollOver() {
        rwlock.writeLock().lock();
        try {
            File rf = getNextRolloverFile(); 
            rolloverFile(rf);

        } finally {
           rwlock.writeLock().unlock();
        }
         
    }
    
    /**
     * Close the stream to the file.
     */
    public void shutdown() {
         rollOverThread.stopThread();
         if (limitwriter != null) {
             limitwriter.shutdown();
         }
         limitwriter = null;
    }
    
    private static final String PERIOD = "."; //$NON-NLS-1$
    private static final String UNDERSCORE = "_"; //$NON-NLS-1$
    private static final String EMPTY = ""; //$NON-NLS-1$
   
    /**
     * create the name to call the newly created swapped file
     * @return
     */
    
    private File getNextRolloverFile() {
        
//        String mmdd = FileLimitSizeLogWriter.getDate();
//        
//        StringBuffer fileName = new StringBuffer(ARCHIVE_PREFIX);
//        fileName.append(prefixName);        
//        fileName.append(mmdd);        
//        fileName.append(UNDERSCORE);
//        
//        fileName.append(filecnt);
//        fileName.append(suffixName);

        String suffix = String.valueOf(filecnt) + suffixName;
        String archiveName = buildArchiveFileName(prefixName, suffix);
        File f = new File(parentFile, archiveName); 
        
        ++filecnt;
        if (f.exists()) {
            return getNextRolloverFile();
        }
        
        return f;

    } 
    
    
    // This only occurs at the creation of the log file.  We check to see if a log file from a
    // previous execution exist and we roll it over.
    private void rolloverPreviousLogFile(File logFile) {
        if (logFile.exists()) {
            String path = logFile.getAbsolutePath();
            String logFileName = logFile.getName();
            path = path.substring(0, path.indexOf(logFileName));
            int index = logFileName.lastIndexOf("."); //$NON-NLS-1$
            String logFile_begin = logFileName.substring(0, index);
            String logFile_end = logFileName.substring(index);

            String archiveName = buildArchiveFileName(logFile_begin, logFile_end);

            // tmpFile.renameTo(new File(logFile_begin + logFile_date + logFile_end));
            logFile.renameTo(new File(path+archiveName));
        }
    }
    
    protected void rolloverFile(final File rollToFile) {
        
        try {
           
            if (this.getPlatformLogWriter() != null) {
                this.getPlatformLogWriter().shutdown();
            }
                                   
            FileInputStream fis = null;
            try {
            
                fis = new FileInputStream(currentLogFile);
                FileUtils.write(fis, rollToFile);
            } catch (IOException e) {
                 e.printStackTrace();
             } finally {
                 fis.close();
             }
             
            boolean deleted = currentLogFile.delete();
            if (!deleted) {
                Exception e = new Exception("File " + currentLogFile + " was not DELETED"); //$NON-NLS-1$ //$NON-NLS-2$
                e.printStackTrace();
            }
            createPlatformLogWriter(this.currentLogFile);
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String buildArchiveFileName(String prefix, String suffix) {
        String mmdd = FileLimitSizeLogWriter.getDate();
        
        StringBuffer fileName = new StringBuffer(ARCHIVE_PREFIX);
        fileName.append(prefix);        
        fileName.append(mmdd);        
        fileName.append(UNDERSCORE);
        
        fileName.append(suffix);
        return fileName.toString();
    }

    public synchronized static String getDate(  ) {
        try {
            Date d = Calendar.getInstance().getTime();
            return DATE_FORMATTER.format(d);
        } catch (Exception e) {
            return ""; //$NON-NLS-1$
            // If there were problems writing out the date, ignore and
            // continue since that shouldn't stop us from losing the rest
            // of the information
        }
    }
    
    
    class FileSizeMonitorThread extends Thread
    {
        private volatile boolean go = true;
        private FileLimitSizeLogWriter writer;
        private long max;
        private long time;



        public FileSizeMonitorThread(long maxSize, long waitTime, FileLimitSizeLogWriter logwriter )
        {
            super("LogFileSizeMonitor"); //$NON-NLS-1$
            writer = logwriter;          
            max = maxSize;
            time = waitTime;
            this.setDaemon(true);
        }

        public void run()
        {
            long max_size = max * SIZE_MULTIPLIER;
            long monitor_time = time * MIN_MULTIPLIER;
            
            while (go) {
                 try {
                    Thread.sleep(monitor_time);
                 }catch(Exception e) {
                     
                 }
                 try {
                    File f = writer.getLogFile();
                    if (f != null && f.exists()) {
                        long l = f.length();
                        if (l >= (max_size)) { 
//                            System.out.println("Rollever, size : " + l + " max: " + max);
                            
                            writer.performRollOver();
                        } else {
//                           System.out.println("NO Rollever, size not big enough: " + l + " max: " + max);
                            
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(CorePlugin.Util.getString("FileLimitSizeLogWriter.Error_Checking_logwriter_rollover__10") + e.getMessage()); //$NON-NLS-1$
                }
            }                
        }
        
        public void stopThread() {
            go = false;
            this.interrupt();
        }
    }        

}
