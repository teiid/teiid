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

package com.metamatrix.xa.arjuna;

import java.io.File;
import java.util.Properties;

import javax.resource.spi.XATerminator;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;

import org.teiid.dqp.internal.transaction.TransactionProvider;

import com.arjuna.ats.arjuna.common.Configuration;
import com.arjuna.ats.arjuna.common.arjPropertyManager;
import com.arjuna.ats.arjuna.coordinator.TxControl;
import com.arjuna.ats.arjuna.recovery.RecoveryManager;
import com.arjuna.ats.internal.jta.recovery.arjunacore.XARecoveryModule;
import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionImple;
import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple;
import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple;
import com.arjuna.ats.internal.jta.transaction.arjunacore.UserTransactionImple;
import com.arjuna.ats.internal.jta.transaction.arjunacore.jca.TxImporter;
import com.arjuna.ats.internal.jta.transaction.arjunacore.jca.XATerminatorImple;
import com.arjuna.ats.jta.common.Environment;
import com.arjuna.ats.jta.common.jtaPropertyManager;
import com.metamatrix.common.xa.MMXid;
import com.metamatrix.common.xa.XATransactionException;
import com.metamatrix.dqp.service.TransactionService;

public class ArjunaTransactionProvider implements TransactionProvider {
    
    private static final String NO = "NO"; //$NON-NLS-1$
    
    private XATerminatorImple terminator = new XATerminatorImple();

    private RecoveryManager recoveryManager;
    
    private static ArjunaTransactionProvider INSTANCE;
    
    private ArjunaTransactionProvider() {
    	
    }
    
    public static synchronized ArjunaTransactionProvider getInstance(Properties props) throws XATransactionException {
    	if (INSTANCE == null) {
    		ArjunaTransactionProvider atp = new ArjunaTransactionProvider();
    		atp.init(props);
    		INSTANCE = atp;
    	}
    	return INSTANCE;
    }
    
    /** 
     * @see org.teiid.dqp.internal.transaction.TransactionProvider#init(java.lang.String)
     */
    public void init(Properties props) throws XATransactionException {
        // unique name for this txn manager
        String vmName = props.getProperty(TransactionService.PROCESSNAME);        
        String txnMgrUniqueName = "txnmgr_" + "_" + vmName; //$NON-NLS-1$ //$NON-NLS-2$

        // set the directory for storing the in-flight transactions
        String baseDir = props.getProperty(TransactionService.TXN_STORE_DIR, System.getProperty("java.io.tmpdir")); //$NON-NLS-1$
        Configuration.setObjectStoreRoot(baseDir + File.separator +  "TeiidTxnStore" + File.separator + txnMgrUniqueName); //$NON-NLS-1$        
        
        // common properties
        arjPropertyManager.propertyManager.setProperty(com.arjuna.ats.arjuna.common.Environment.XA_NODE_IDENTIFIER, txnMgrUniqueName);
        arjPropertyManager.propertyManager.setProperty("com.arjuna.ats.arjuna.recovery.transactionStatusManagerPort",  props.getProperty(TransactionService.TXN_STATUS_PORT, TransactionService.DEFAULT_TXN_STATUS_PORT)) ; //$NON-NLS-1$        

        // jta specific properties
        jtaPropertyManager.propertyManager.setProperty(Environment.JTA_TSR_IMPLEMENTATION, TransactionSynchronizationRegistryImple.class.getName());
        jtaPropertyManager.propertyManager.setProperty(Environment.SUPPORT_SUBTRANSACTIONS, NO);
        jtaPropertyManager.propertyManager.setProperty(Environment.JTA_TM_IMPLEMENTATION, TransactionManagerImple.class.getName());
        jtaPropertyManager.propertyManager.setProperty(Environment.JTA_UT_IMPLEMENTATION, UserTransactionImple.class.getName());
        jtaPropertyManager.propertyManager.setProperty(Environment.XA_RECOVERY_NODE, "*"); //$NON-NLS-1$        
        jtaPropertyManager.propertyManager.setProperty(XARecoveryModule.XARecoveryPropertyNamePrefix+"MM", XAConnectorRecovery.class.getName()); //$NON-NLS-1$ 

        // get the timeout property
        int timeout = Integer.parseInt(props.getProperty(TransactionService.MAX_TIMEOUT, TransactionService.DEFAULT_TXN_TIMEOUT));
        TxControl.setDefaultTimeout(timeout);

        boolean startRecovery = Boolean.valueOf(props.getProperty(TransactionService.TXN_ENABLE_RECOVERY, "true")).booleanValue(); //$NON-NLS-1$
        if (startRecovery) {
            RecoveryManager.delayRecoveryManagerThread();
            recoveryManager = RecoveryManager.manager();
            recoveryManager.startRecoveryManagerThread();
        }
    }
    
    /** 
     * @see org.teiid.dqp.internal.transaction.TransactionProvider#getXATerminator()
     */
    public XATerminator getXATerminator() {
        return terminator;
    }

    /** 
     * @see org.teiid.dqp.internal.transaction.TransactionProvider#getTransactionManager()
     */
    public TransactionManager getTransactionManager() {
        return com.arjuna.ats.jta.TransactionManager.transactionManager();
    }
    
    /** 
     * @see org.teiid.dqp.internal.transaction.TransactionProvider#importTransaction(com.metamatrix.common.xa.MMXid, int)
     */
    public Transaction importTransaction(MMXid xid, int timeout) throws XAException {
        return TxImporter.importTransaction(xid, timeout);
    }

    /** 
     * @see org.teiid.dqp.internal.transaction.TransactionProvider#getTransactionID(javax.transaction.Transaction)
     */
    public String getTransactionID(Transaction tx) {
        TransactionImple arjunaTx = (TransactionImple)tx;
        return arjunaTx.get_uid().stringForm();
    }

    /** 
     * @see org.teiid.dqp.internal.transaction.TransactionProvider#shutdown()
     */
    public void shutdown() {
        if (this.recoveryManager != null) {
            this.recoveryManager.stop();
        }
    }

    public void registerRecoverySource(String name, XAConnectionSource connector) {
        XAConnectorRecovery.addConnector(name, connector);
    }
    
    public void removeRecoverySource(String name) {
        XAConnectorRecovery.removeConnector(name);
    }
}
