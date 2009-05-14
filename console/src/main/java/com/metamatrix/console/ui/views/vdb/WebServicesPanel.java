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

package com.metamatrix.console.ui.views.vdb;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextArea;

import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConfigurationManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.models.VdbManager;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.platform.admin.api.ConfigurationAdminAPI;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;

/**
 * @since 5.5.3
 */
public class WebServicesPanel extends JPanel implements
                                            VdbDisplayer {

    private final static String PUBLISH_BTN_LABEL = ConsolePlugin.Util.getString("WebServicesPanel.publish"); //$NON-NLS-1$
    private final static String UNPUBLISH_BTN_LABEL = ConsolePlugin.Util.getString("WebServicesPanel.unpublish"); //$NON-NLS-1$
    private final static String NO_WSDL_TEXT = ConsolePlugin.Util.getString("WebServicesPanel.noModelsMsg"); //$NON-NLS-1$
    private final static String ERROR_RETRIEVING_DATABASES = ConsolePlugin.Util
                                                                               .getString("WebServicesPanel.errorRetrievingVirtualDatabases"); //$NON-NLS-1$
    private final static String ERROR_RETRIEVING_CURRENT_CONFIGURATION = ConsolePlugin.Util
                                                                                           .getString("WebServicesPanel.errorRetrievingCurrentConfiguration"); //$NON-NLS-1$ 
    private final static String NOT_ACTIVE_HDR = ConsolePlugin.Util.getString("WebServicesPanel.notActiveHdr"); //$NON-NLS-1$
    private final static String NOT_ACTIVE_MSG = ConsolePlugin.Util.getString("WebServicesPanel.notActiveMsg"); //$NON-NLS-1$                                                                                

    private VirtualDatabase vdb;
    private JButton publishButton;
    private JButton unpublishButton;
    private JPanel buttonsPanel;
    private ConnectionInfo connection;

    /**
     * Constructor
     * @param connection
     */
    public WebServicesPanel(ConnectionInfo connection) {
        super();
        this.connection = connection;
        createButtons();
    }

    private VdbManager getVdbManager() {
        return ModelManager.getVdbManager(connection);
    }

    private void createButtons() {
        publishButton = new ButtonWidget(PUBLISH_BTN_LABEL);
        unpublishButton = new ButtonWidget(UNPUBLISH_BTN_LABEL);
        publishButton.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent ev) {
                publishPressed();
            }
        });
        unpublishButton.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent ev) {
                unpublishPressed();
            }
        });
        buttonsPanel = new JPanel(new GridLayout(1, 2, 5, 0));
        buttonsPanel.add(publishButton);
        buttonsPanel.add(unpublishButton);
    }

    private void publishPressed() {
        if (checkIfActive()) {
            launchWizard(true);
        } else {
            displayNotActiveDialog();
        }
    }

    private void unpublishPressed() {
        if (checkIfActive()) {
            launchWizard(false);
        } else {
            displayNotActiveDialog();
        }
    }

    private boolean checkIfActive() {
        boolean continuing = true;
        Boolean active = null;
        boolean isActive = false;
        try {
            String vdbName = vdb.getName();
            VirtualDatabaseID id = (VirtualDatabaseID)vdb.getID();
            int version = (new Integer(id.getVersion())).intValue();
            active = getVdbManager().isVDBActive(vdbName, version);
        } catch (Exception ex) {
            LogManager.logError(LogContexts.VIRTUAL_DATABASE, ex, ERROR_RETRIEVING_DATABASES);
            ExceptionUtility.showMessage(ERROR_RETRIEVING_DATABASES, ex);
            isActive = false;
            continuing = false;
        }
        if (continuing) {
            isActive = active.booleanValue();
        }
        return isActive;
    }

    private void displayNotActiveDialog() {
        StaticUtilities.displayModalDialogWithOK(NOT_ACTIVE_HDR, NOT_ACTIVE_MSG, JOptionPane.WARNING_MESSAGE);
    }

    private void launchWizard(boolean publishing) {
        ConfigurationManager configManager = ModelManager.getConfigurationManager(connection);
        ConfigurationAdminAPI configAPI = ModelManager.getConfigurationAPI(connection);
        Configuration currentConfig = null;
        try {
            currentConfig = configAPI.getCurrentConfiguration();
        } catch (Exception ex) {
            LogManager.logError(LogContexts.VIRTUAL_DATABASE, ex, ERROR_RETRIEVING_CURRENT_CONFIGURATION);
            ExceptionUtility.showMessage(ERROR_RETRIEVING_CURRENT_CONFIGURATION, ex);
        }
        if (currentConfig != null) {
            VirtualDatabaseID id = (VirtualDatabaseID)vdb.getID();
            String version = id.getVersion();
            WSDLWizardRunner runner = new WSDLWizardRunner(vdb.getName(), version, currentConfig, configManager, publishing);
            runner.go();
        }
    }

    public void setVirtualDatabase(VirtualDatabase vdb) {
        if (this.vdb != vdb) {
            this.vdb = vdb;

            this.removeAll();
            if (vdb != null) {
                GridBagLayout layout = new GridBagLayout();
                setLayout(layout);
                WSDLOperationsDescription desc = getVdbManager().getWSDLOperationsDescription(vdb);
                if (desc == null) {
                    JTextArea noWSDLTextArea = new JTextArea(NO_WSDL_TEXT);
                    noWSDLTextArea.setEditable(false);
                    noWSDLTextArea.setLineWrap(true);
                    noWSDLTextArea.setWrapStyleWord(true);
                    noWSDLTextArea.setBackground(this.getBackground());
                    this.add(noWSDLTextArea);
                    layout.setConstraints(noWSDLTextArea, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER,
                                                                                 GridBagConstraints.BOTH, new Insets(10, 10, 10,
                                                                                                                     10), 0, 0));
                } else {
                    JPanel opsPanel = addOperationsDescription(desc);
                    add(opsPanel);
                    layout
                          .setConstraints(opsPanel, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER,
                                                                           GridBagConstraints.BOTH, new Insets(4, 4, 4, 4), 0, 0));
                    add(buttonsPanel);
                    layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                                                                               GridBagConstraints.NONE, new Insets(4, 4, 4, 4),
                                                                               0, 0));
                }
            }

        }
    }

    private JPanel addOperationsDescription(WSDLOperationsDescription desc) {
        // TODO
        return new JPanel();
    }
}
