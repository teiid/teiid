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

import org.teiid.dqp.internal.transaction.TransactionServerImpl;

import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.xa.XATransactionException;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;
import com.metamatrix.dqp.service.TransactionService;
import com.metamatrix.xa.arjuna.ArjunaTransactionProvider;

public class EmbeddedTransactionService extends TransactionServerImpl {

    @Override
    public void initialize(Properties props)
    		throws ApplicationInitializationException {
        try {
        	props = new Properties(props);
            props.setProperty(TransactionService.PROCESSNAME, props.getProperty(DQPEmbeddedProperties.PROCESSNAME));
            props.setProperty(TransactionService.TXN_STORE_DIR, props.getProperty(DQPEmbeddedProperties.DQP_WORKDIR));
            this.setTransactionProvider(ArjunaTransactionProvider.getInstance(props));
        } catch (XATransactionException e) {
            throw new ApplicationInitializationException(e);
        } 
    }
    
}
