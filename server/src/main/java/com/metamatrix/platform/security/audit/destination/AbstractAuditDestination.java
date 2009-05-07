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

package com.metamatrix.platform.security.audit.destination;

import java.util.Properties;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.security.audit.format.AuditMessageFormat;
import com.metamatrix.platform.security.util.LogSecurityConstants;
import com.metamatrix.platform.util.ErrorMessageKeys;

/**
 * This is a logging destination that logs to a single file.
 */
public abstract class AbstractAuditDestination implements AuditDestination {

    protected static final String DEFAULT_LOG_FORMAT_PROPERTY_NAME = "com.metamatrix.security.audit.format.DelimitedAuditMessageFormat"; //$NON-NLS-1$

    private AuditMessageFormat formatter;

	public AbstractAuditDestination() {
	}

	/**
	 * Set properties and set up fileWriter.
	 */
	public void setFormat( String formatterClassName ) throws AuditDestinationInitFailedException {

        if ( formatterClassName == null || formatterClassName.trim().length() == 0 ) {
            formatterClassName = this.getDefaultFormatClassName();
            LogManager.logTrace(LogSecurityConstants.CTX_AUDIT,
                    new Object[]{"No log message format specified for audit destination class \"", //$NON-NLS-1$
                                 this.getClass().getName(),
                                 "\"; using default class \"",formatterClassName,"\""}); //$NON-NLS-1$ //$NON-NLS-2$
        }
        try {
            LogManager.logTrace(LogSecurityConstants.CTX_AUDIT, new Object[]{"Initializing audit message format class \"",formatterClassName,"\""}); //$NON-NLS-1$ //$NON-NLS-2$
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            Class formatterClass = classLoader.loadClass(formatterClassName);
            formatter = (AuditMessageFormat) formatterClass.newInstance();
        } catch ( ClassNotFoundException e ) {
            throw new AuditDestinationInitFailedException(e, ErrorMessageKeys.SEC_AUDIT_0016, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0016, formatterClassName));
        } catch ( ClassCastException e ) {
            throw new AuditDestinationInitFailedException(e, ErrorMessageKeys.SEC_AUDIT_0017, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0017, formatterClassName, AuditMessageFormat.class.getName()));
        } catch ( Exception e ) {
            throw new AuditDestinationInitFailedException(e, ErrorMessageKeys.SEC_AUDIT_0018, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0018, formatterClassName));
        }
	}

	/**
	 * Init the destination.
	 */
	public void initialize(Properties props) throws AuditDestinationInitFailedException {
        LogManager.logTrace(LogSecurityConstants.CTX_AUDIT, new Object[]{"Initializing audit destination class \"",this.getClass().getName(),"\""}); //$NON-NLS-1$ //$NON-NLS-2$
	}

	/**
	 * Get the AuditMessageFormat.
	 */
	public AuditMessageFormat getFormat() {
		return formatter;
	}

    protected String getDefaultFormatClassName() {
        return DEFAULT_LOG_FORMAT_PROPERTY_NAME;
    }
}
