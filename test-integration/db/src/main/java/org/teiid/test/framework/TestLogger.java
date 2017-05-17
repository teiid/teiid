/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
@SuppressWarnings("nls")
public class TestLogger {
    
    public static final Level INFO = Level.INFO;
    public static final Level DEBUG = Level.FINE;
    public static final Level CONFIG = Level.CONFIG;
    
  private static final Logger LOGGER = Logger.getLogger("org.teiid.test");
    
    static {
	BasicConfigurator.configure(new ConsoleAppender());

	LOGGER.setLevel(INFO);

    }
    
    public static final void setLogLevel(Level level) {
	LOGGER.setLevel(level);
    }
    
    public static final void logDebug(String msg) {
	log(DEBUG, msg, null);
    }
    
    public static final void logDebug(String msg, Throwable t) {
	log(DEBUG, msg, t);
    }
    
    // info related messages, which
    public static final void logInfo(String msg) {
	log(INFO, msg, null);
    }
    
    // configuration related messages
    public static final void logConfig(String msg) {
	log(CONFIG, msg, null);
    }
    
    // most important messages
    public static final void log(String msg) {
	log(INFO, msg, null);
    }
    
    private static final void log(Level javaLevel, Object msg, Throwable t) {
    	if (LOGGER.isLoggable(javaLevel)) {

    		LOGGER.log(javaLevel, msg.toString(), t);
    	}
    }

}
