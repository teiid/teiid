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

package com.metamatrix.dqp.embedded.services;

import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.common.xa.XATransactionException;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;
import com.metamatrix.dqp.internal.transaction.TransactionServerImpl;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.DQPServiceRegistry;
import com.metamatrix.dqp.service.TransactionService;
import com.metamatrix.dqp.transaction.TransactionServer;
import com.metamatrix.dqp.transaction.XAServer;
import com.metamatrix.xa.arjuna.ArjunaTransactionProvider;

public class EmbeddedTransactionService extends EmbeddedBaseDQPService implements TransactionService {

    public static final String TRANSACTIONS_ENABLED = "metamatrix.xatxnmgr.enabled"; //$NON-NLS-1$
    private TransactionServerImpl arjunaTs = new TransactionServerImpl();
    private TransactionServer ts;
    
    public EmbeddedTransactionService(DQPServiceRegistry svcRegistry) 
        throws MetaMatrixComponentException{
        super(DQPServiceNames.TRANSACTION_SERVICE, svcRegistry);
    }

    /**  
     * @param props
     * @throws ApplicationInitializationException
     */
    public void initializeService(Properties props) throws ApplicationInitializationException {
        try {
            props.put(TransactionService.HOSTNAME, "dqp"); //$NON-NLS-1$
            props.put(TransactionService.VMNAME, props.getProperty(DQPEmbeddedProperties.DQP_IDENTITY));
            props.setProperty(TransactionService.TXN_STORE_DIR, props.getProperty(TransactionService.TXN_STORE_DIR, TransactionService.DEFAULT_TXN_STORE_DIR));
            
            arjunaTs.init(props, new ArjunaTransactionProvider());
            
            final Class[] interfaces = new Class[] {TransactionServer.class, XAServer.class};
            
            ts = (TransactionServer)LogManager.createLoggingProxy(LogCommonConstants.CTX_XA_TXN, arjunaTs, interfaces, MessageLevel.DETAIL);
        } catch (XATransactionException e) {
            throw new ApplicationInitializationException(e);
        } 
    }

    /* 
     * @see com.metamatrix.common.application.ApplicationService#start(com.metamatrix.common.application.ApplicationEnvironment)
     */
    public void startService(ApplicationEnvironment environment) throws ApplicationLifecycleException {

    }
   
    /* 
     * @see com.metamatrix.common.application.ApplicationService#bind()
     */
    public void bindService() throws ApplicationLifecycleException {
    }

    /* 
     * @see com.metamatrix.common.application.ApplicationService#unbind()
     */
    public void unbindService() throws ApplicationLifecycleException {
    }

    /* 
     * @see com.metamatrix.common.application.ApplicationService#stop()
     */
    public void stopService() throws ApplicationLifecycleException {
        arjunaTs.shutdown(true);
    }

    /** 
     * @see com.metamatrix.dqp.service.TransactionService#getTransactionServer()
     */
    public TransactionServer getTransactionServer() {
        return ts;
    }

    /** 
     * @see com.metamatrix.dqp.service.TransactionService#getXAServer()
     */
    public XAServer getXAServer() {
        return (XAServer)ts;
    }
    
}
