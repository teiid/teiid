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

package com.metamatrix.platform.security.audit.destination;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.metamatrix.platform.security.audit.AuditMessage;
import com.metamatrix.platform.security.audit.format.ReadableAuditMessageFormat;

/**
 * This is the default logging destination - System.out.
 */
public class ConsoleAuditDestination extends AbstractAuditDestination {

    protected static final String DEFAULT_CONSOLE_AUDIT_FORMAT_PROPERTY_NAME = ReadableAuditMessageFormat.class.getName();

    /**
     * The name of the property that contains the name of the LogMessageFormat
     * class that is used to format messages sent to the console.
     * This is an optional property; if not specified then the
     * {@link com.metamatrix.common.logging.format.DelimitedLogMessageFormat DelimitedLogMessageFormat}
     * is used.
     */
    public static final String MESSAGE_FORMAT_PROPERTY_NAME = AuditDestination.PROPERTY_PREFIX + "consoleFormat"; //$NON-NLS-1$

    private static final String DESCRIPTION = "System.out"; //$NON-NLS-1$

    /**
     * Construct a ConsoleDestination.
     */
    public ConsoleAuditDestination() {
        super();
    }

	/**
	 * Return description
	 * @return Description
	 */
	public String getDescription() {
		return DESCRIPTION;
	}

	/**
	 * Initialize this destination with the specified properties.
     * @param props the properties that this destination should use to initialize
     * itself.
     * @throws LogDestinationInitFailedException if there was an error during initialization.
	 */
	public void initialize(Properties props) throws AuditDestinationInitFailedException {
        super.initialize(props);
        super.setFormat( props.getProperty(MESSAGE_FORMAT_PROPERTY_NAME) );
	}

	/**
	 * Get names of all properties used for this destination.
	 */
	public List getPropertyNames() {
		List pnames = new ArrayList();
		pnames.add(MESSAGE_FORMAT_PROPERTY_NAME);
		return pnames;
	}

	/**
	 * Print to System.out.
	 * @param message Message to print
	 */
	public void record(AuditMessage message) {
		System.out.println( this.getFormat().formatMessage(message));
	}

	/**
	 * Shutdown - nothing to do.
	 */
	public void shutdown() {
	}

    protected String getDefaultFormatClassName() {
        return DEFAULT_CONSOLE_AUDIT_FORMAT_PROPERTY_NAME;
    }
}
