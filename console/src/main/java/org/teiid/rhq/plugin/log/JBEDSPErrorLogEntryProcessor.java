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
package org.teiid.rhq.plugin.log;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.rhq.core.domain.event.EventSeverity;
import org.rhq.core.pluginapi.event.log.MultiLineLogEntryProcessor;


/** 
 * @since 4.3
 */
public class JBEDSPErrorLogEntryProcessor extends MultiLineLogEntryProcessor {

    
    /**
     * The regex for the primary log line: '['date']' '['severityLevel']' '['clientIP']' message
     * e.g.: [Wed Oct 11 14:32:52 2008] [error] [client 127.0.0.1] client denied by server configuration
     * NOTE: The message portion may contain multiple lines.
     */
    private static final String REGEX = "(.*) (.*) (INFO|WARNING|ERROR|DEBUG) (.*)"; //$NON-NLS-1$
    private static final Pattern PATTERN = Pattern.compile(REGEX);

    private static final String DATE_PATTERN = "MMM dd, yyyy kk:mm:ss.SSS"; // e.g.:  Aug 26, 2008 13:10:11.371  //$NON-NLS-1$
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat(DATE_PATTERN);

    private static final Map<SeverityLevel, EventSeverity> LEVEL_TO_SEVERITY_MAP = new LinkedHashMap();
    static {
        LEVEL_TO_SEVERITY_MAP.put(SeverityLevel.DEBUG, EventSeverity.DEBUG);
        LEVEL_TO_SEVERITY_MAP.put(SeverityLevel.INFO, EventSeverity.INFO);
        LEVEL_TO_SEVERITY_MAP.put(SeverityLevel.WARNING, EventSeverity.WARN);
        LEVEL_TO_SEVERITY_MAP.put(SeverityLevel.ERROR, EventSeverity.ERROR);        
    }

    public JBEDSPErrorLogEntryProcessor(String eventType, File logFile) {
        super(eventType, logFile);
    }

    protected Pattern getPattern() {
        return PATTERN;
    }

    protected DateFormat getDefaultDateFormat() {
        return DATE_FORMAT;
    }

    protected LogEntry processPrimaryLine(Matcher matcher) throws ParseException {
        String dateString = matcher.group(1);
        Date  timestamp = parseDateString(dateString);
        String severityLevelString = matcher.group(3);
        SeverityLevel severityLevel;
        try {
            severityLevel = SeverityLevel.valueOf(severityLevelString.toUpperCase());
        }
        catch (IllegalArgumentException e) {
            throw new ParseException("Unknown severity level: " + severityLevelString); //$NON-NLS-1$
        }
        EventSeverity severity = LEVEL_TO_SEVERITY_MAP.get(severityLevel);
        String detail = matcher.group(4);
        return new LogEntry(timestamp, severity, detail);
    }

    private enum SeverityLevel {
        DEBUG,
        INFO,
        WARNING,
        ERROR
    }
}
