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
package com.metamatrix.dqp.service;

import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.dqp.transaction.TransactionServer;
import com.metamatrix.dqp.transaction.XAServer;

/**
 */
public interface TransactionService extends ApplicationService {
    public static final String MAX_TIMEOUT = "metamatrix.xatxnmgr.max_timeout"; //$NON-NLS-1$
    public static final String MAX_FILESIZE_MB= "metamatrix.xatxnmgr.max_log_filesize_in_mb"; //$NON-NLS-1$
    public static final String MAX_ROLLINGFILES= "metamatrix.xatxnmgr.max_rolled_log_files"; //$NON-NLS-1$
    public static final String TXN_MGR_LOG_DIR = "metamatrix.xatxnmgr.log_base_dir"; //$NON-NLS-1$
    public static final String SEPARATE_TXN_LOG = "metamatrix.xatxnmgr.separate_log"; //$NON-NLS-1$
    public static final String TXN_STORE_DIR = "metamatrix.xatxnmgr.txnstore_dir"; //$NON-NLS-1$
    public static final String TXN_STATUS_PORT = "metamatrix.xatxnmgr.txnstatus_port"; //$NON-NLS-1$
    public static final String TXN_ENABLE_RECOVERY = "metamatrix.xatxnmgr.enable_recovery"; //$NON-NLS-1$
    
    
    public static final String VMNAME = "metamatrix.xatxnmgr.vmname"; //$NON-NLS-1$
    public static final String HOSTNAME = "metamatrix.xatxnmgr.hostname"; //$NON-NLS-1$
    
    public static final String DEFAULT_TXN_MGR_LOG_DIR = "txnlog"; //$NON-NLS-1$
    public static final String DEFAULT_TXN_TIMEOUT = "120"; //$NON-NLS-1$ //2 mins
    public static final String DEFAULT_LOGFILE_SIZE= "10"; //$NON-NLS-1$ 
    public static final String DEFAULT_MAX_ROLLOVER_FILES = "10"; //$NON-NLS-1$ 
    public static final String DEFAULT_SEPARATE_TXN_LOG = "false"; //$NON-NLS-1$ 
    public static final String DEFAULT_TXN_STORE_DIR = System.getProperty("metamatrix.xatxnmgr.txnstore_dir", System.getProperty("user.dir")); //$NON-NLS-1$ //$NON-NLS-2$
    public static final String DEFAULT_TXN_STATUS_PORT = "0"; //$NON-NLS-1$

    TransactionServer getTransactionServer();
    
    XAServer getXAServer();
}
