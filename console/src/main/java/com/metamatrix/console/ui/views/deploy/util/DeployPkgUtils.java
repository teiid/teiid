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

//#############################################################################
package com.metamatrix.console.ui.views.deploy.util;

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.Icon;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.border.EmptyBorder;

import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConfigurationManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.ui.util.property.GuiComponentFactory;
import com.metamatrix.console.ui.util.property.PropertyProvider;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;

/**
 * @version 1.0
 * @author Dan Florian
 */
public final class DeployPkgUtils implements PropertyConstants {

	// /////////////////////////////////////////////////////////////////////////
	// CONSTANTS
	// /////////////////////////////////////////////////////////////////////////

	public static final String PROPS = "com/metamatrix/console/ui/views/deploy/data/ui";

	public static final EmptyBorder EMPTY_BORDER;

	public static final String[] DEPLOY_HDRS;
	public static final int HOST_COL = 0;
	public static final int PROCESS_COL = 1;
//	public static final int DEPLOY_SVC_COL = 2;

	public static final String[] SERV_DEF_HDRS;
//	public static final int VM_COL = 0;
	public static final int SERV_COL = 0;
	public static final int CONNECTOR_COL= 1;
	public static final int ENABLED_COL = 2;
	public static final int ESSENTIAL_COL = 3;

	/** Key for UserPreferences file for last import/export directory. */
	// public static final String LAST_DIR =
	// "metamatrix.console.lastconfigdirectory";
	// /////////////////////////////////////////////////////////////////////////
	// FIELDS
	// /////////////////////////////////////////////////////////////////////////
	private static PropertyProvider propProvider;

	// /////////////////////////////////////////////////////////////////////////
	// INITIALIZER
	// /////////////////////////////////////////////////////////////////////////

	static {
		// first setup property provider
		ArrayList propFiles = new ArrayList();
		propFiles.add(PROPS);
		propFiles.add(PropertyProvider.COMMON_PROP);
		propFiles.add(GuiComponentFactory.TYPE_DEFS_PROP);
		propProvider = new PropertyProvider(propFiles);

		int inset = getInt("emptyinsets", 10);
		EMPTY_BORDER = new EmptyBorder(inset, inset, inset, inset);

		DEPLOY_HDRS = new String[2];
		DEPLOY_HDRS[HOST_COL] = getString("dpu.deployedhost.hdr");
		DEPLOY_HDRS[PROCESS_COL] = getString("dpu.deployedprocess.hdr");
		// DEPLOY_HDRS[DEPLOY_SVC_COL] = getString("dpu.deployedsvc.hdr");

		SERV_DEF_HDRS = new String[4];
//		SERV_DEF_HDRS[VM_COL] = getString("dpu.vm.hdr");
		SERV_DEF_HDRS[SERV_COL] = getString("dpu.service.hdr");
		SERV_DEF_HDRS[CONNECTOR_COL] = getString("dpu.connector.hdr");
		SERV_DEF_HDRS[ENABLED_COL] = getString("dpu.enabled.hdr");
		SERV_DEF_HDRS[ESSENTIAL_COL] = getString("dpu.essential.hdr");

	}

	// /////////////////////////////////////////////////////////////////////////
	// CONSTRUCTORS
	// /////////////////////////////////////////////////////////////////////////

	/** Don't allow no arg construction. */
	private DeployPkgUtils() {
	}

	// /////////////////////////////////////////////////////////////////////////
	// METHODS
	// /////////////////////////////////////////////////////////////////////////

	public static LabelWidget createLabel(String theStringId) {
		String value = getString(theStringId);
		return new LabelWidget(value);
	}

	public static TextFieldWidget createTextField(String theType) {
		return GuiComponentFactory.createTextField(theType);
	}

	public static boolean getBoolean(String theKey) {
		return propProvider.getBoolean(theKey);
	}

	public static Icon getIcon(String theKey) {
		return propProvider.getIcon(theKey);
	}

	public static int getInt(String theKey, int theDefault) {

		return propProvider.getInt(theKey, theDefault);
	}

	public static int getMnemonic(String theKey) {
		String key = propProvider.getString(theKey, true);
		return (key == null) ? 0 : (int) key.charAt(0);
	}

	public static Object getObject(String theKey) {
		return propProvider.getObject(theKey);
	}

	public static String getString(String theKey) {
		return propProvider.getString(theKey);
	}

	public static String getString(String theKey, boolean theReturnNullFlag) {

		return propProvider.getString(theKey, theReturnNullFlag);
	}

	public static String getString(String theKey, Object[] theArgs) {

		return propProvider.getString(theKey, theArgs);
	}

	/**
	 * Compares to strings with <code>null</code> and the empty string being
	 * equal.
	 * 
	 * @param theOne
	 *            the first string being compared
	 * @param theOther
	 *            the second string being compared
	 * @return <code>true</code> if both strings are equivalent
	 */
	public static boolean equivalent(String theOne, String theOther) {

		boolean result = true;
		if ((theOne == null) || (theOne.length() == 0)) {
			if ((theOther != null) && (theOther.length() > 0)) {
				result = false;
			}
		} else {
			if ((theOther == null) || (theOther.length() == 0)) {
				result = false;
			} else {
				result = theOne.equals(theOther);
			}
		}
		return result;
	}

	public static void loadProcesses(Configuration theConfiguration,
			DefaultTableModel theModel, ConnectionInfo connection)
			throws ExternalException {

		// populate deployments table
		theModel.setNumRows(0);
		ConfigurationManager configMgr = ModelManager
				.getConfigurationManager(connection);
		Collection hosts = theConfiguration.getHosts();
		// configMgr.getHosts((ConfigurationID) theConfiguration.getID());
		if (hosts != null) {
			Iterator hostItr = hosts.iterator();
			while (hostItr.hasNext()) {
				Host host = (Host) hostItr.next();
				Collection procs = theConfiguration.getVMsForHost((HostID) host
						.getID());
				if (procs != null) {
					Iterator procItr = procs.iterator();
					while (procItr.hasNext()) {
						VMComponentDefn process = (VMComponentDefn) procItr
								.next();
						Vector row = new Vector(DEPLOY_HDRS.length);
						row.setSize(DEPLOY_HDRS.length);
						row.setElementAt(host, HOST_COL);
						row.setElementAt(process, PROCESS_COL);
						// row.setElementAt(product, DEPLOY_PROD_COL);
						theModel.addRow(row);

					}
				}
			}
		}
	}

	public static void loadServiceDefintions(Configuration theConfiguration,
			DefaultTableModel theModel, ConnectionInfo connection)
			throws ExternalException {

		theModel.setNumRows(0);
		// ConfigurationManager configMgr =
		// ModelManager.getConfigurationManager(connection);

		Collection depcomps = theConfiguration.getDeployedComponents();
		if (depcomps != null) {
			Iterator svcItr = depcomps.iterator();
			while (svcItr.hasNext()) {
				DeployedComponent dsvc = (DeployedComponent) svcItr.next();
				if (dsvc.isDeployedConnector() || dsvc.isDeployedService()) {
					Host host = theConfiguration.getHost(dsvc.getHostID()
							.getName());
					VMComponentDefn process = theConfiguration
							.getVMComponentDefn(dsvc.getVMComponentDefnID());

					Vector row = new Vector(SERV_DEF_HDRS.length);
					row.setSize(SERV_DEF_HDRS.length);
					row.setElementAt(dsvc, SERV_COL);

					ServiceComponentDefnID svcID = (ServiceComponentDefnID) dsvc.getServiceComponentDefnID();
					ServiceComponentDefn svc = null;
					
					svc = (ServiceComponentDefn) theConfiguration.getComponentDefn(svcID);

					if (dsvc.isDeployedConnector()) {
						row.setElementAt(svc.getComponentTypeID().getName(), CONNECTOR_COL);
					}
					row.setElementAt(new Boolean(dsvc.isEnabled()), ENABLED_COL);
					row.setElementAt(new Boolean(svc.isEssential()),
							ESSENTIAL_COL);
					theModel.addRow(row);

				}
			}
		}

	}

	public static DefaultTableModel setup(TableWidget theTable,
			String[] theHeaders, int theVisibleRows,
			final int[] theEditableColumns) {

		DefaultTableModel model = (DefaultTableModel) theTable.getModel();
		model.setColumnIdentifiers(theHeaders);
		theTable.setEditable(false);
		if (theEditableColumns != null) {
			for (int i = 0; i < theEditableColumns.length; theTable
					.setColumnEditable(theEditableColumns[i++], true)) {

			}
		}
		theTable.setPreferredScrollableViewportSize(new Dimension(theTable
				.getPreferredSize().width, theVisibleRows
				* theTable.getRowHeight()));
		theTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		theTable.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
		theTable.setSortable(true);
		return model;
	}

}
