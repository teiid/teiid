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

package org.teiid.logging;

import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.teiid.core.util.StringUtil;

/**
 * Write to Java logging
 */
public class JavaLogger implements org.teiid.logging.Logger {

    private static ConcurrentHashMap<String, Logger> loggers = new ConcurrentHashMap<String, Logger>();

    @Override
    public boolean isEnabled(String context, int msgLevel) {
        Logger logger = getLogger(context);

        Level javaLevel = convertLevel(msgLevel);
        return logger.isLoggable(javaLevel);
    }

    private Logger getLogger(String context) {
        Logger logger = loggers.get(context);
        if (logger == null) {
            logger = Logger.getLogger(context);
            loggers.put(context, logger);
        }
        return logger;
    }

    public void log(int level, String context, Object... msg) {
        log(level, context, null, msg);
    }

    public void log(int level, String context, Throwable t, Object... msg) {
        Logger logger = getLogger(context);

        Level javaLevel = convertLevel(level);

        if (msg.length == 0) {
            logger.log(javaLevel, null, t);
        }
        else if (msg.length == 1 && !(msg[0] instanceof String)) {
            String msgStr = StringUtil.toString(msg, " ", false); //$NON-NLS-1$
            LogRecord record = new LogRecord(javaLevel, msgStr);
            record.setParameters(msg);
            record.setThrown(t);
            record.setLoggerName(context);
            logger.log(record);
        }
        else {
            logger.log(javaLevel, StringUtil.toString(msg, " ", false), t); //$NON-NLS-1$
        }
    }

    public Level convertLevel(int level) {
        switch (level) {
        case MessageLevel.CRITICAL:
        case MessageLevel.ERROR:
            return Level.SEVERE;
        case MessageLevel.WARNING:
            return Level.WARNING;
        case MessageLevel.INFO:
            return Level.FINE;
        case MessageLevel.DETAIL:
            return Level.FINER;
        case MessageLevel.TRACE:
            return Level.FINEST;
        }
        return Level.ALL;
    }

    public void shutdown() {
    }

    @Override
    public void putMdc(String key, String val) {

    }

    @Override
    public void removeMdc(String key) {

    }

}
