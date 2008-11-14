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

package com.metamatrix.platform.config;

import java.io.File;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.bootstrap.SystemCurrentConfigBootstrap;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.connection.ManagedConnection;
import com.metamatrix.common.messaging.MessageBusConstants;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.platform.config.persistence.api.PersistentConnectionFactory;
import com.metamatrix.platform.config.persistence.impl.file.FilePersistentConnection;
import com.metamatrix.platform.config.spi.xml.XMLConfigurationConnector;
import com.metamatrix.platform.config.spi.xml.XMLConfigurationConnectorFactory;
import com.metamatrix.platform.config.transaction.ConfigTransactionFactory;

public abstract class BaseTest extends TestCase {

	private final static String path = UnitTestUtil.getTestScratchPath()
			+ File.separator + "config"; //$NON-NLS-1$
	// private static final String DEFAULT_PATH =".";
	protected boolean printMessages = false;

	// the resources in this config file are not set to JDBC
	protected static final String CONFIG_FILE = "config.xml"; //$NON-NLS-1$
	// protected static final String CONFIG_FILE = "config_woresources.xml";
	// //$NON-NLS-1$

	private static BasicConfigurationObjectEditor editor = new BasicConfigurationObjectEditor(
			true);
	private static XMLConfigurationConnectorFactory factory = new XMLConfigurationConnectorFactory();
	private XMLConfigurationConnector writer = null;
	private ManagedConnection conn = null;

	public BaseTest(String name) {
		super(name);
		initData();
		// must remove the system property that is being set for every junit
		// test
		// the indicates to use no configuration
		Properties sysProps = System.getProperties();
		sysProps.remove(SystemCurrentConfigBootstrap.NO_CONFIGURATION);
		sysProps.put(MessageBusConstants.MESSAGE_BUS_TYPE,
				MessageBusConstants.TYPE_NOOP);

		System.setProperties(sysProps);

	}

	public BaseTest(String name, boolean useNoOpConfig) {
		super(name);
		initData();
		if (!useNoOpConfig) {
			Properties sysProps = System.getProperties();
			sysProps.remove(SystemCurrentConfigBootstrap.NO_CONFIGURATION);
			System.setProperties(sysProps);
		}
	}

	protected String getPath() {
		return path;
	}

	protected void printMsg(String msg) {
		if (printMessages) {
			System.out.println(msg);
		}
	}

	protected static Properties createSystemProperties(String fileName) {
		Properties cfg_props = createProperties(fileName);

		// these system props need to be set for the CurrentConfiguration call

		Properties sysProps = System.getProperties();
		sysProps.putAll(cfg_props);
		System.setProperties(sysProps);

		return cfg_props;
	}

	public static Properties createProperties(String fileName) {

		Properties props = new Properties();

		if (fileName != null) {
			props.setProperty(
					FilePersistentConnection.CONFIG_NS_FILE_NAME_PROPERTY,
					fileName);
			props.setProperty(
					PersistentConnectionFactory.PERSISTENT_FACTORY_NAME,
					PersistentConnectionFactory.FILE_FACTORY_NAME);
			props.setProperty(
					ConfigTransactionFactory.SINGLE_VM_TRANSACTION_LOCK_OPTION,
					"true"); //$NON-NLS-1$

		}

		if (path != null) {
			props.setProperty(
					FilePersistentConnection.CONFIG_FILE_PATH_PROPERTY, path);
		}

		return props;
	}

	// the following methods are used in conjunction when wanting to do
	// configuration transactions.
	public void initTransactions(Properties props) throws Exception {

		conn = factory.createConnection(props, getName());
		writer = (XMLConfigurationConnector) factory.createTransaction(conn,
				false);

	}

	public BasicConfigurationObjectEditor getEditor() {
		return editor;
	}

	public ConfigurationModelContainer getConfigModel() throws Exception {
		ConfigurationModelContainer config = CurrentConfiguration
				.getConfigurationModel();
		return config;

	}

	public void commit() throws Exception {

		try {
			writer.executeActions(editor.getDestination().popActions(),
					getName());
			writer.commit();
		} catch (Exception e) {
			writer.rollback();
			throw e;
		}

		writer = (XMLConfigurationConnector) factory.createTransaction(conn,
				false);

	}

	protected XMLConfigurationConnector getWriter() {
		return this.writer;
	}
	
	private void initData() {
		File scratch = new File(path);
		if (scratch.exists()) {
			FileUtils.removeDirectoryAndChildren(scratch);
		}
		scratch.mkdir();
		try {
			FileUtils.copyDirectoryContentsRecursively(UnitTestUtil
					.getTestDataFile("config"), scratch); //$NON-NLS-1$
		} catch (Exception e) {
			throw new RuntimeException(e);
		} 
	}

}
