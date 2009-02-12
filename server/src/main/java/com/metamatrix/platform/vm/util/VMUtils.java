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

package com.metamatrix.platform.vm.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.core.log.FileLimitSizeLogWriter;
import com.metamatrix.internal.core.log.PlatformLog;

public class VMUtils {

    /**
     *  initializeVMLogFile is called by the different controllers (i.e., SocketVMController, HostController, etc)
     *  to setup the log file using the LimitSize log file writer.
     * @param fileName
     * @throws Exception
     * @since 4.2
     */
   public static void startLogFile(String path, String logFile) throws ConfigurationException, IOException{
       File tmpFile = new File(path, logFile);
       tmpFile.getParentFile().mkdirs();
       
       // if log file exists then create a archive
       if (tmpFile.exists()) {
           int index = logFile.lastIndexOf("."); //$NON-NLS-1$
           String archiveName = FileLimitSizeLogWriter.buildArchiveFileName(logFile.substring(0, index), logFile.substring(index));
           tmpFile.renameTo(new File(path, archiveName));
       }
       
       FileOutputStream fos = new FileOutputStream(tmpFile);
       PrintStream ps = new PrintStream(fos);

       System.setOut(ps);
       System.setErr(ps);

       Properties logProps = new Properties();
       Properties configProps = CurrentConfiguration.getInstance().getProperties();
       if (configProps.containsKey(FileLimitSizeLogWriter.FILE_SIZE_LIMIT)) {
           logProps.setProperty(FileLimitSizeLogWriter.FILE_SIZE_LIMIT,configProps.getProperty(FileLimitSizeLogWriter.FILE_SIZE_LIMIT));
       }
       if (configProps.containsKey(FileLimitSizeLogWriter.FILE_SIZE_MONITOR_TIME)) {
           logProps.setProperty(FileLimitSizeLogWriter.FILE_SIZE_MONITOR_TIME,configProps.getProperty(FileLimitSizeLogWriter.FILE_SIZE_MONITOR_TIME));
       }

       FileLimitSizeLogWriter flw = new FileLimitSizeLogWriter(tmpFile, logProps, false);

       PlatformLog.getInstance().addListener(flw);

   }
    
}
