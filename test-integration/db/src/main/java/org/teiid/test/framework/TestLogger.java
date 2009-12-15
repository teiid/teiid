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
package org.teiid.test.framework;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;


/**
 * @author vanhalbert
 *
 */
public class TestLogger {
    
    public static final Level INFO = Level.FINER;
    public static final Level DEBUG = Level.FINEST;
    public static final Level IMPORTANT = Level.FINE;
    
   private static final Logger LOGGER = Logger.getLogger("org.teiid.test");
    
    static {
	BasicConfigurator.configure(new ConsoleAppender());

	LOGGER.setLevel(INFO);

    }
    
    public static final void setLogLevel(Level level) {
	LOGGER.setLevel(level);
    }
    
    public static final void logDebug(String msg) {
	log(Level.ALL, msg, null);
    }
    
    public static final void logDebug(String msg, Throwable t) {
	log(Level.ALL, msg, t);
    }
    
    // info related messages, which
    public static final void logInfo(String msg) {
	log(Level.INFO, msg, null);
    }
    
    // configuration related messages
    public static final void logConfig(String msg) {
	log(Level.CONFIG, msg, null);
    }
    
    // most important messages
    public static final void log(String msg) {
	log(Level.INFO, msg, null);
    }
    
    private static final void log(Level javaLevel, Object msg, Throwable t) {
//	System.out.println(msg);
    	if (LOGGER.isLoggable(javaLevel)) {

    		LOGGER.log(javaLevel, msg.toString(), t);
    	}
    }

}
