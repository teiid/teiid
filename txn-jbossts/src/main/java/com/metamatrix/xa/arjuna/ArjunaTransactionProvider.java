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
import java.io.IOException;
import java.util.Properties;

import javax.resource.spi.XATerminator;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.spi.LoggingEvent;

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
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.common.xa.MMXid;
import com.metamatrix.common.xa.XATransactionException;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.dqp.internal.transaction.TransactionProvider;
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
     * @see com.metamatrix.dqp.internal.transaction.TransactionProvider#init(java.lang.String)
     */
    public void init(Properties props) throws XATransactionException {
        // unique name for this txn manager
        String vmName = props.getProperty(TransactionService.VMNAME);        
        String txnMgrUniqueName = "txnmgr_" + props.getProperty(TransactionService.HOSTNAME, "").replace('.', '_') + "_" + vmName; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        // set the directory for storing the in-flight transactions
        String baseDir = props.getProperty(TransactionService.TXN_STORE_DIR, TransactionService.DEFAULT_TXN_STORE_DIR);
        Configuration.setObjectStoreRoot(baseDir + File.separator +  "MetaMatrixTxnStore" + File.separator + txnMgrUniqueName); //$NON-NLS-1$        
        
        configureLogging(txnMgrUniqueName, props);

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
    
    private void configureLogging(String uniqueName, Properties props) throws XATransactionException{
        // depending upon the logging type either log to the MM log file or create separate file for
        // transaction logging.
        AppenderSkeleton appender = null;        
        if (Boolean.valueOf(props.getProperty(TransactionService.SEPARATE_TXN_LOG, TransactionService.DEFAULT_SEPARATE_TXN_LOG)).booleanValue()){
            RollingFileAppender fileAppender;
            try {
                String baseDir = props.getProperty(TransactionService.TXN_MGR_LOG_DIR);
                File directory = new File(baseDir);
                if (!directory.exists()) {
                    directory.mkdirs();
                }
                String filename = baseDir + "/" + uniqueName + ".log"; //$NON-NLS-1$ //$NON-NLS-2$
                fileAppender = new RollingFileAppender(new PatternLayout("%d [%t] %-5p %c - %m%n"), filename, true); //$NON-NLS-1$
                fileAppender.setMaxFileSize(props.getProperty(TransactionService.MAX_FILESIZE_MB, TransactionService.DEFAULT_LOGFILE_SIZE)+"MB"); //$NON-NLS-1$
                fileAppender.setMaxBackupIndex(Integer.parseInt(props.getProperty(TransactionService.MAX_ROLLINGFILES, TransactionService.DEFAULT_MAX_ROLLOVER_FILES)));
            } catch (IOException e) {
                throw new XATransactionException(e);
            }            
            appender = fileAppender;
        }
        else {
            appender = new MMLogAppender();            
        }
        appender.setThreshold(Level.DEBUG);

        Logger root = Logger.getLogger("com.arjuna"); //$NON-NLS-1$
        root.addAppender(appender);
    }
    
    /** 
     * @see com.metamatrix.dqp.internal.transaction.TransactionProvider#getXATerminator()
     */
    public XATerminator getXATerminator() {
        return terminator;
    }

    /** 
     * @see com.metamatrix.dqp.internal.transaction.TransactionProvider#getTransactionManager()
     */
    public TransactionManager getTransactionManager() {
        return com.arjuna.ats.jta.TransactionManager.transactionManager();
    }
    
    /** 
     * @see com.metamatrix.dqp.internal.transaction.TransactionProvider#importTransaction(com.metamatrix.common.xa.MMXid, int)
     */
    public Transaction importTransaction(MMXid xid, int timeout) throws XAException {
        return TxImporter.importTransaction(xid, timeout);
    }

    /** 
     * @see com.metamatrix.dqp.internal.transaction.TransactionProvider#getTransactionID(javax.transaction.Transaction)
     */
    public String getTransactionID(Transaction tx) {
        TransactionImple arjunaTx = (TransactionImple)tx;
        return arjunaTx.get_uid().stringForm();
    }

    /** 
     * @see com.metamatrix.dqp.internal.transaction.TransactionProvider#shutdown()
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
    
    static class MMLogAppender extends AppenderSkeleton{

        protected void append(LoggingEvent event) {
            int level = MessageLevel.ERROR;
            
            switch(event.getLevel().toInt()) {
                case Level.DEBUG_INT:
                    level = MessageLevel.DETAIL;
                    break;
                case Level.INFO_INT:
                    level = MessageLevel.INFO;
                    break;                    
                case Level.WARN_INT:
                    level = MessageLevel.WARNING;
                    break;
                case Level.ERROR_INT:
                    level = MessageLevel.ERROR;
                    break;
                case Level.FATAL_INT:
                    level = MessageLevel.CRITICAL;
                    break;
            }
            
            if (event.getThrowableInformation() != null) {
                LogManager.log(level, LogCommonConstants.CTX_XA_TXN, event.getThrowableInformation().getThrowable(), event.getRenderedMessage());
            }
            else {
                LogManager.log(level, LogCommonConstants.CTX_XA_TXN, event.getRenderedMessage());              
            }
        }

        public void close() {
        }

        public boolean requiresLayout() {
            return false;
        }
    }
}
