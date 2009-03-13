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

package com.metamatrix.dqp.embedded.services;

import java.util.Properties;

import org.teiid.connector.api.ExecutionContext;

import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.dqp.service.TrackingService;

public class EmbeddedTrackingService extends EmbeddedBaseDQPService implements TrackingService {
    //public static HashMap traceData = new HashMap();

    /* 
     * @see com.metamatrix.dqp.service.TrackingService#log(java.lang.String, long, java.lang.String, short, java.lang.String, java.lang.String, short, java.lang.String, java.lang.String, java.lang.String, int)
     */
    public void log(
        String requestId,
        long nodeID,
        String subTxnUid,
        short status,
        String modelName,
        String cbName,
        short cmdPoint,
        String sessionUid,
        String principal,
        String sql,
        int rowCount,
        ExecutionContext context) {

    }

    /* 
     * @see com.metamatrix.dqp.service.TrackingService#log(java.lang.String, short, short, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public void log(
        String txnUid,
        short txnPoint,
        short status,
        String sessionUid,
        String principal,
        String vdbName,
        String vdbVersion) {

    }

    /* 
     * @see com.metamatrix.dqp.service.TrackingService#log(java.lang.String, java.lang.String, short, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public void log(
        String requestId,
        String txnUid,
        short cmdPoint,
		short status,
        String sessionUid,
        String applicationName,
        String principal,
        String vdbName,
        String vdbVersion,
        String sql,
        int rowCount) {

    }

    public boolean willRecordMMCmd() {
        return false;
    }
    public boolean willRecordSrcCmd() {
        return false;
    }
    public boolean willRecordTxn() {
        return false;
    }

    /** 
     * @see com.metamatrix.dqp.embedded.services.EmbeddedBaseDQPService#initializeService(java.util.Properties)
     * @since 4.3
     */
    public void initializeService(Properties properties) throws ApplicationInitializationException {
    }

    /** 
     * @see com.metamatrix.dqp.embedded.services.EmbeddedBaseDQPService#startService(com.metamatrix.common.application.ApplicationEnvironment)
     * @since 4.3
     */
    public void startService(ApplicationEnvironment environment) throws ApplicationLifecycleException {
    }

    /** 
     * @see com.metamatrix.dqp.embedded.services.EmbeddedBaseDQPService#stopService()
     * @since 4.3
     */
    public void stopService() throws ApplicationLifecycleException {
    }    
}
