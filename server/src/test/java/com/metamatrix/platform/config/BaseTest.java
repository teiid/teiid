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

package com.metamatrix.platform.config;

import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.platform.config.spi.xml.XMLConfigurationConnector;

public abstract class BaseTest extends TestCase {

	protected boolean printMessages = false;

	// the resources in this config file are not set to JDBC
	protected static final String CONFIG_FILE = ConfigUpdateMgr.CONFIG_FILE; //$NON-NLS-1$
	// protected static final String CONFIG_FILE = "config_woresources.xml";
	// //$NON-NLS-1$

	private ConfigUpdateMgr mgr = new ConfigUpdateMgr();

	public BaseTest(String name) {
		super(name);
	}

	public BaseTest(String name, boolean useNoOpConfig) {
		super(name);
		if (!useNoOpConfig) {
			Properties sysProps = System.getProperties();
			System.setProperties(sysProps);
		}
	}

	protected String getPath() {
		return mgr.getPath();
	}

	protected void printMsg(String msg) {
		if (printMessages) {
			System.out.println(msg);
		}
	}

	protected static void createSystemProperties(String fileName) throws Exception{
		ConfigUpdateMgr.createSystemProperties(fileName);
	}


	// the following methods are used in conjunction when wanting to do
	// configuration transactions.
	public void initTransactions(Properties props) throws Exception {
		mgr.initTransactions(props);

	}

	public BasicConfigurationObjectEditor getEditor() {
		return mgr.getEditor();
	}

	public ConfigurationModelContainer getConfigModel() throws Exception {
		return mgr.getConfigModel();

	}

	public void commit() throws Exception {
		mgr.commit();
	}

	protected XMLConfigurationConnector getWriter() {
		return mgr.getWriter();
	}

}
