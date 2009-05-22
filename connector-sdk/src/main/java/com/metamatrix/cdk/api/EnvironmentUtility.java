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

package com.metamatrix.cdk.api;

import java.io.Serializable;
import java.util.Properties;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.ConnectorPropertyNames;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.dqp.internal.datamgr.impl.ConnectorEnvironmentImpl;
import org.teiid.dqp.internal.datamgr.impl.ExecutionContextImpl;


/**
 * A utility factory class to create connector environment objects that are normally supplied
 * by the MetaMatrix Server.  This utility will create objects that can be used for testing 
 * of your connector outside the context of the MetaMatrix Server.
 */
public class EnvironmentUtility {

    /**
     * Can't construct - this is a utility class. 
     */
    private EnvironmentUtility() {
    }
    
    /**
     * Create a ConnectorLogger that prints to STDOUT at the specified log level (and above).  
     * @param logLevel The logLevel as defined in {@link SysLogger}.
     * @return A logger
     */
    public static ConnectorLogger createStdoutLogger(int logLevel) {
        SysLogger logger = new SysLogger();
        logger.setLevel(logLevel);
        return logger;         
    }

    /**
     * Create a ConnectorEnvironment with the specified properties and logger.  
     * @param props The properties to put in the environment
     * @param logger The logger to use
     * @return A ConnectorEnvironment instance
     */
    public static ConnectorEnvironment createEnvironment(Properties props, ConnectorLogger logger) {
        if(props.getProperty(ConnectorPropertyNames.CONNECTOR_BINDING_NAME) == null) {
            props.setProperty(ConnectorPropertyNames.CONNECTOR_BINDING_NAME, "test"); //$NON-NLS-1$
        }
        return new ConnectorEnvironmentImpl(props, logger, null);
    } 
    
    /**
     * Create a ConnectorEnvironment with the specified properties.  A default logger will be 
     * created that prints logging to STDOUT for INFO level and above. 
     * @param props The properties to put in the environment
     * @return A ConnectorEnvironment instance
     */
    public static ConnectorEnvironment createEnvironment(Properties props) {
        return createEnvironment(props, true);
    }
    
    /**
     * Create a ConnectorEnvironment with the specified properties.  A default logger will be 
     * created that prints logging to STDOUT for INFO level and above if stdoutLog is true. 
     * @param props The properties to put in the environment
     * @param stdoutLog
     * @return A ConnectorEnvironment instance
     */
    public static ConnectorEnvironment createEnvironment(Properties props, boolean stdoutLog) {
        return EnvironmentUtility.createEnvironment(props, stdoutLog?createStdoutLogger(SysLogger.INFO):new SysLogger(false));
    }
    
    /**
     * Create an ExecutionContext and set just the user name. Dummy information will be
     * created for the other parts of the context. 
     * @param user User name
     * @return A SecurityContext / ExecutionContext instance
     */
    public static ExecutionContext createSecurityContext(String user) {
        return new ExecutionContextImpl("vdb", "1", user, null, null, "Connection", "ConnectorID<CDK>", "Request", "1", "0");  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
    }

    /**
     * Create an ExecutionContext and set just the security parts. Dummy information will be
     * created for the other parts of the context. 
     * @param vdbName Virtual database name
     * @param vdbVersion Virtual database version
     * @param user User name
     * @param trustedToken Trusted token (passed when creating JDBC Connection)
     * @return A SecurityContext / ExecutionContext instance
     */
    public static ExecutionContext createSecurityContext(String vdbName, String vdbVersion, String user, Serializable trustedToken) {
        return new ExecutionContextImpl(vdbName, vdbVersion, user, trustedToken, null, "Connection", "ConnectorID<CDK>", "Request", "1", "0");  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }
    
    /**
     * Create an ExecutionContext and set just the requestID and partID.  Dummy information will be
     * created for the other parts of the context. 
     * @param requestID Unique identifier for the user command within the server
     * @param partID Unique identifier for the source command within the context of a requestID
     * @return A SecurityContext / ExecutionContext instance
     */
    public static ExecutionContext createExecutionContext(String requestID, String partID) {
        return new ExecutionContextImpl("vdb", "1", "user", null, null, "Connection", "ConnectorID<CDK>", requestID, partID, "0");   //$NON-NLS-1$//$NON-NLS-2$//$NON-NLS-3$//$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
    }

    /**
     * Create an ExecutionContext and set all of the parts. 
     * @param vdbName Virtual database name
     * @param vdbVersion Virtual database version
     * @param user User name
     * @param trustedToken Trusted token (passed when creating JDBC Connection)
     * @param executionPayload Command payload (passed for each command executed on JDBC Statement)
     * @param requestID Unique identifier for the user command within the server
     * @param partID Unique identifier for the source command within the context of a requestID
     * @param connectionID Unique identifier for the connection through which the command is executed
     * @param useResultSetCache Whether to use ResultSet cache if it is enabled. 
     * @return A SecurityContext / ExecutionContext instance
     * @since 4.2
     */
    public static ExecutionContext createExecutionContext(String vdbName, String vdbVersion, String user,
                                                        Serializable trustedToken, Serializable executionPayload,                                                         
														String connectionID, String connectorID, String requestID, String partID, boolean useResultSetCache) {
        return new ExecutionContextImpl(vdbName, vdbVersion, user, trustedToken, executionPayload, connectionID, connectorID, requestID, partID, "0"); //$NON-NLS-1$
    }

}
