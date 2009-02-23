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

package com.metamatrix.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Properties;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.log.FileLimitSizeLogWriter;
import com.metamatrix.core.log.LogListener;
import com.metamatrix.internal.core.log.PlatformLog;

public class FileLogListenerProvider implements Provider<LogListener> {

	@Inject
	@Named(com.metamatrix.server.Configuration.LOGFILE)
	String logFile;

	@Inject
	@Named(com.metamatrix.server.Configuration.LOGDIR)	
	String path;
	
	@Override
	public LogListener get() {
		final PlatformLog log = new PlatformLog();
		try {
			FileLimitSizeLogWriter flw = buildFileLogger();		
			log.addListener(flw);
		} catch (FileNotFoundException e) {
			throw new MetaMatrixRuntimeException(e);
		}
		return log;
	}

	
	FileLimitSizeLogWriter buildFileLogger() throws FileNotFoundException {
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

		FileLimitSizeLogWriter flw = new FileLimitSizeLogWriter(tmpFile,logProps, false);
		return flw;
	}
	
}
