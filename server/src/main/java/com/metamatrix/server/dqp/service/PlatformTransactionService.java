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

/*
 */
package com.metamatrix.server.dqp.service;

import java.util.Properties;

import org.teiid.dqp.internal.transaction.TransactionServerImpl;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.ResourceNames;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.xa.XATransactionException;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.dqp.service.TransactionService;
import com.metamatrix.server.Configuration;
import com.metamatrix.xa.arjuna.ArjunaTransactionProvider;

/**
 */
public class PlatformTransactionService extends TransactionServerImpl {

    private Host host;
    
    @Inject
    public PlatformTransactionService(@Named(Configuration.HOST) Host host) {
    	this.host = host;
    }
    
    /*
     * @see com.metamatrix.common.application.ApplicationService#initialize(java.util.Properties)
     */
    public void initialize(Properties props) throws ApplicationInitializationException {
        try {
            Properties env = null;
            try {
                env = CurrentConfiguration.getInstance().getResourceProperties(ResourceNames.XA_TRANSACTION_MANAGER);
            } catch ( ConfigurationException e ) {
                throw new ApplicationInitializationException(e);
            }
            
            props.putAll(env);
            props.setProperty(TransactionService.PROCESSNAME, CurrentConfiguration.getInstance().getProcessName());
            props.setProperty(TransactionService.TXN_STORE_DIR, host.getDataDirectory()); 

            this.setTransactionProvider(ArjunaTransactionProvider.getInstance(props));
            this.setProcessName(CurrentConfiguration.getInstance().getProcessName());
        } catch (XATransactionException err) {
            throw new ApplicationInitializationException(err);
        }
    }

}
