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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import org.teiid.dqp.internal.transaction.TransactionServerImpl;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.protocol.URLHelper;
import com.metamatrix.common.xa.XATransactionException;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;
import com.metamatrix.dqp.service.TransactionService;
import com.metamatrix.xa.arjuna.ArjunaTransactionProvider;

public class EmbeddedTransactionService extends TransactionServerImpl {

    public static final String TRANSACTIONS_ENABLED = "metamatrix.xatxnmgr.enabled"; //$NON-NLS-1$
    
    @Inject @Named("BootstrapURL") URL bootstrapURL;
    
    @Override
    public void initialize(Properties props)
    		throws ApplicationInitializationException {
        try {
        	props = new Properties(props);
            props.put(TransactionService.HOSTNAME, "dqp"); //$NON-NLS-1$
            props.put(TransactionService.VMNAME, props.getProperty(DQPEmbeddedProperties.DQP_IDENTITY));
            
            String dir = props.getProperty(TransactionService.TXN_STORE_DIR);
            if (dir != null) {
            	props.setProperty(TXN_STORE_DIR, URLHelper.buildURL(bootstrapURL, dir).getPath());
            } else {
            	props.setProperty(TXN_STORE_DIR, props.getProperty(DQPEmbeddedProperties.DQP_TMPDIR));
            }
            this.setTransactionProvider(ArjunaTransactionProvider.getInstance(props));
        } catch (XATransactionException e) {
            throw new ApplicationInitializationException(e);
        } catch (MalformedURLException e) {
        	throw new ApplicationInitializationException(e);
		} 
    }
    
}
