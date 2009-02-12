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

package com.metamatrix.platform.security.audit.config;

import java.util.*;

import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.security.audit.AuditConfiguration;
import com.metamatrix.platform.security.audit.AuditManager;
import com.metamatrix.platform.util.ErrorMessageKeys;

public class CurrentConfigAuditConfigurationFactory implements AuditConfigurationFactory {

    /**
     * The name of the property that contains the set of comma-separated
     * context names for audit messages <i>not</i> to be recorded.  A message context is simply
     * some string that identifies something about the component that generates
     * the message.  The value for the contexts is application specific.
     * <p>
     * This is an optional property that defaults to no contexts (i.e., messages
     * with any context are recorded).
     */
    public static final String AUDIT_CONTEXT_PROPERTY_NAME = "metamatrix.audit.contexts"; //$NON-NLS-1$

    public static final String CONTEXT_DELIMETER = ","; //$NON-NLS-1$

    /**
     * Obtain the set of contexts for messages that are to be discarded.
     * If this method returns an empty set, then messages in all contexts
     * are being recorded; if not empty, then messages with a context in the
     * returned set are discarded and messages for all other contexts recorded.
     * @return the set of contexts for messages that are to be discarded
     */
    public AuditConfiguration getConfiguration( Properties p ) throws AuditConfigurationException {
        if ( p == null ) {
            return new BasicAuditConfiguration();
        }
        AuditConfiguration result = null;

        // Create a configuration with the specified level ...
        String logValue = p.getProperty(AuditManager.SYSTEM_AUDIT_LEVEL_PROPERTY_NAME);
        if ( logValue != null && logValue.trim().length() > 0 ) {
            if ( logValue.equalsIgnoreCase("true") ) { //$NON-NLS-1$
                result = new BasicAuditConfiguration(1);
            } else if ( logValue.equalsIgnoreCase("false") ) { //$NON-NLS-1$
                result = new BasicAuditConfiguration(0);
            } else {
                throw new AuditConfigurationException(
                        PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0013, logValue));
            }
        }
        if ( result == null ) {
            result = new BasicAuditConfiguration();
        }

        // Get the descarded contexts ...
        String contextValues = p.getProperty(AUDIT_CONTEXT_PROPERTY_NAME);
        if ( contextValues != null ) {
            Collection discardedCtxs = new HashSet();
            StringTokenizer tokenizer = new StringTokenizer(contextValues,CONTEXT_DELIMETER);
            while ( tokenizer.hasMoreElements() ) {
                discardedCtxs.add(tokenizer.nextElement().toString());
            }
            result.discardContexts(discardedCtxs);
        }
        return result;
    }
}
