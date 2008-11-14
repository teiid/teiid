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

//#############################################################################
package com.metamatrix.console.ui.views.deploy;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.Collection;

import javax.swing.JPanel;

import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConfigurationManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.ChooserPanel;
import com.metamatrix.console.ui.util.ConsoleConstants;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.ui.util.WizardInterfaceImpl;
import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.toolbox.ui.widget.DialogWindow;
import com.metamatrix.toolbox.ui.widget.DirectoryChooserPanel;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;

public final class ConfigurationImportWizard {

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    private static final String[] FILE_EXTENSIONS =
        (String[])DeployPkgUtils.getObject("dmp.importexport.extensions"); //$NON-NLS-1$
        
    private static final String FILE_VIEW_DESC = DeployPkgUtils.getString("dmp.importexport.description"); //$NON-NLS-1$
        
    private static final String CHOOSER_DESC = "Select configuration file to import to the Next Startup configuration."; //$NON-NLS-1$

    ///////////////////////////////////////////////////////////////////////////
    // CONTROLS
    ///////////////////////////////////////////////////////////////////////////

    private ChooserPanel pnlChooser;
    private ConfirmationPage pnlConfirmation;
    private WizardImpl wizard;
    private DialogWindow dlg;

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private Collection configObjs;
    private String fileName;
    private ConnectionInfo connectionInfo;
    //private ConfigurationManager configManager;

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public ConfigurationImportWizard(ConnectionInfo connectionInfo) {
        super();
        //configManager = cMgr;
        this.connectionInfo = connectionInfo;
        wizard = new WizardImpl();
        pnlChooser = new ChooserPanel(wizard, 1, DirectoryChooserPanel.TYPE_OPEN, 
                    CHOOSER_DESC, FILE_EXTENSIONS, FILE_VIEW_DESC, ConsoleConstants.CONSOLE_DIRECTORY_LOCATION_KEY);
                    
        pnlChooser.enableForwardButton(false);
        pnlChooser.init();
        pnlConfirmation = new ConfirmationPage(wizard);
        wizard.addPage(pnlChooser);
        wizard.addPage(pnlConfirmation);
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    public ConfigurationManager getConfigurationManager() {
        return ModelManager.getConfigurationManager(connectionInfo);
    }
    
    public Collection getImportedObjects() {
        return configObjs;
    }

    private void nextPage() {
        fileName = pnlChooser.getSelectedFullFileName();
        try {
            configObjs = getConfigurationManager().importObjects(fileName);
            
            pnlChooser.saveCurrentDirLocation();
        } catch (Exception theException) {
                Object[] args = {fileName};
                ExceptionUtility.showMessage(
                    DeployPkgUtils.getString("dmp.msg.importerror"), //$NON-NLS-1$
                    DeployPkgUtils.getString(
                        "dmp.msg.importerrordetail", args), //$NON-NLS-1$
                    theException);
                LogManager.logError(
                    LogContexts.PSCDEPLOY,
                    theException,
                    DeployPkgUtils.getString("dmp.msg.importerror", args)); //$NON-NLS-1$
                pnlChooser.enableForwardButton(false);
                return;
        }

           pnlConfirmation.setConfiguration(Configuration.NEXT_STARTUP);
            pnlConfirmation.setConflicts(false);
            //lConflicts.hasConflicts());
            pnlConfirmation.setFileName(fileName);
            wizard.showPage(pnlConfirmation);
 //     }
    }

    /**
     * @return <code>true</code> if the wizard finished; <code>false</code>
     * otherwise.
     */
    public boolean run() {
        dlg = new DialogWindow(ConsoleMainFrame.getInstance(),
                               DeployPkgUtils.getString("dmp.import.title"), //$NON-NLS-1$
                               wizard);
        dlg.setLocationRelativeTo(ConsoleMainFrame.getInstance());
        dlg.show();
        boolean finished =
            ((wizard.getSelectedButton() != null) &&
             (wizard.getSelectedButton() == wizard.getFinishButton()));
        if (finished) {
            pnlChooser.saveCurrentDirLocation();            
        }
        return finished;
    }

    ///////////////////////////////////////////////////////////////////////////
    // ConfirmationPage INNER CLASSES
    ///////////////////////////////////////////////////////////////////////////

    // third page
    private class ConfirmationPage
        extends BasicWizardSubpanelContainer {

        private TextFieldWidget txfConfig;
        private TextFieldWidget txfFile;

        public ConfirmationPage(WizardInterface wizardInterface) {
            super(wizardInterface);
            setStepText(2, "Confirm import to Next Startup configuration."); //$NON-NLS-1$

            JPanel pnl = new JPanel(new GridBagLayout());
            setMainContent(pnl);
                       

            LabelWidget lblFile =
                new LabelWidget(DeployPkgUtils.getString("dmp.import.lblFile")); //$NON-NLS-1$
            GridBagConstraints gbc = new GridBagConstraints();
            gbc.gridx = 0;
            gbc.gridy = 0;
            gbc.insets = new Insets(3, 3, 3, 3);            
            gbc.anchor = GridBagConstraints.EAST;
             pnl.add(lblFile, gbc);

            txfFile = DeployPkgUtils.createTextField("dirpath"); //$NON-NLS-1$            
            txfFile.setEditable(false);
            gbc.gridx++;            
            gbc.anchor = GridBagConstraints.WEST;
            pnl.add(txfFile, gbc);

            LabelWidget lblConfig =
                new LabelWidget(DeployPkgUtils.getString("dmp.import.lblConfig")); //$NON-NLS-1$
            gbc.gridx = 0;
            gbc.gridy++;
            gbc.anchor = GridBagConstraints.EAST;
            pnl.add(lblConfig, gbc);

            txfConfig = DeployPkgUtils.createTextField("configname"); //$NON-NLS-1$
            txfConfig.setEditable(false);
            gbc.gridx++;
            gbc.anchor = GridBagConstraints.WEST;
            pnl.add(txfConfig, gbc);

            LabelWidget lblMsg =
                new LabelWidget(
                    DeployPkgUtils.getString("dmp.import.msg.confirmationpage")); //$NON-NLS-1$
            gbc.gridx = 0;
            gbc.gridy++;
            gbc.anchor = GridBagConstraints.CENTER;
            gbc.insets = new Insets(20, 3, 3, 3);
            gbc.gridwidth = GridBagConstraints.REMAINDER;
            pnl.add(lblMsg, gbc);
        }

        public void setConfiguration(String theConfigName) {
            txfConfig.setText(theConfigName);
        }

        public void setConflicts(boolean theConflictsFlag) {
//            chkConflicts.setSelected(theConflictsFlag);
        }

        public void setFileName(String theFileName) {
            txfFile.setText(theFileName);
        }

    }

    ///////////////////////////////////////////////////////////////////////////
    // WizardImpl INNER CLASSES
    ///////////////////////////////////////////////////////////////////////////

    // wizard controller
    private class WizardImpl extends WizardInterfaceImpl {

        public WizardImpl() {
            super();
        }

        public void showNextPage() {
            nextPage();
        }

    }
    
        
}
