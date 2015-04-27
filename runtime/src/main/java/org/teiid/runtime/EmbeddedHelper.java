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
package org.teiid.runtime;

import java.io.File;
import java.security.AccessController;
import java.security.PrivilegedAction;

import javax.transaction.TransactionManager;

import com.arjuna.ats.arjuna.common.ObjectStoreEnvironmentBean;
import com.arjuna.ats.arjuna.common.arjPropertyManager;
import com.arjuna.common.internal.util.propertyservice.BeanPopulator;

/**
 * A Helper class for running Teiid Embedded. 
 * 
 * The 'getTransactionManager()' method return a Narayana JTA TransactionManager.
 * 
 *
 */
public class EmbeddedHelper {
	
	public static TransactionManager getTransactionManager() throws Exception {
		
		arjPropertyManager.getCoreEnvironmentBean().setNodeIdentifier("1");
		arjPropertyManager.getCoreEnvironmentBean().setSocketProcessIdPort(0);
		arjPropertyManager.getCoreEnvironmentBean().setSocketProcessIdMaxPorts(10);
		
		arjPropertyManager.getCoordinatorEnvironmentBean().setEnableStatistics(false);
		arjPropertyManager.getCoordinatorEnvironmentBean().setDefaultTimeout(300);
		arjPropertyManager.getCoordinatorEnvironmentBean().setTransactionStatusManagerEnable(false);
		arjPropertyManager.getCoordinatorEnvironmentBean().setTxReaperTimeout(120000);
		
		String storeDir = getStoreDir();
		
		arjPropertyManager.getObjectStoreEnvironmentBean().setObjectStoreDir(storeDir);
		BeanPopulator.getNamedInstance(ObjectStoreEnvironmentBean.class, "communicationStore").setObjectStoreDir(storeDir); //$NON-NLS-1$
		
		return com.arjuna.ats.jta.TransactionManager.transactionManager();
	}

	
	private static String getStoreDir() {
		String defDir = getSystemProperty("user.home") + File.separator + ".teiid/embedded/data"; //$NON-NLS-1$ //$NON-NLS-2$
		return getSystemProperty("teiid.embedded.txStoreDir", defDir);
	}


	private static String getSystemProperty(final String name, final String value) {
		return AccessController.doPrivileged(new PrivilegedAction<String>(){

			@Override
			public String run() {
				return System.getProperty(name, value);
			}});
	}
	
	private static String getSystemProperty(final String name) {
		return AccessController.doPrivileged(new PrivilegedAction<String>(){

			@Override
			public String run() {
				return System.getProperty(name);
			}});
	}

}
