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

package com.metamatrix.console.ui.views.vdb;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.IOException;

import javax.swing.AbstractButton;
import javax.swing.JDialog;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.text.JTextComponent;

import com.metamatrix.admin.api.exception.AdminException;
import com.metamatrix.admin.api.server.ServerAdmin;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.tree.directory.FileSystemEntry;
import com.metamatrix.common.tree.directory.FileSystemFilter;
import com.metamatrix.common.tree.directory.FileSystemView;
import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.ConsoleConstants;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.ui.util.WizardInterfaceImpl;
import com.metamatrix.console.util.DialogUtility;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.core.util.FileUtil;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.toolbox.preference.UserPreferences;
import com.metamatrix.toolbox.ui.widget.DirectoryChooserPanel;

/**
 * VDBRolesExporter - the VDB Roles Exporter.
 * 
 */
public class VDBRolesExporter {
    

	final static String VDB_ROLES_DEFAULT_EXT = "xml";
    final static String[] VDB_ROLES_EXPORT_EXTENSIONS = new String[] {
    	VDB_ROLES_DEFAULT_EXT
    };

    final static String VDB_ROLES_TYPE_FILE_DESC = "VDB Roles Export Types (*.xml)";

    private ConnectionInfo connection;
    private VirtualDatabase vdb;
    private String vdbName;
    private String vdbVersionStr;
    private int vdbVersion;
    private RolesTargetSelectorPanel selectorPanel = null;
    // private boolean cancelled = false;
    private boolean finished = false;
    private RolesExporterWizardPanelDialog dialog = null;

    /**
     * Constructor.
     * @param vdb the VDB to export Roles.
     * @param connection the connection object.
     * 
     */
    public VDBRolesExporter(VirtualDatabase vdb,
                       ConnectionInfo connection) {
        super();
        this.vdb = vdb;
        this.connection = connection;
        vdbName = this.vdb.getName();
        VirtualDatabaseID id = (VirtualDatabaseID)this.vdb.getID();
        vdbVersionStr = id.getVersion();
        vdbVersion = (new Integer(vdbVersionStr)).intValue();
    }

    /**
     * Go method that does the panels logic and performs the export.
     * 
     */
    public boolean go() {
        // 1-panel wizard plus possible warning dialog. Simply display file
        // selector widget, enabling "Finish" when a selection has been entered.
        // Then, if file exists display warning dialog that it will be overwritten.
        // If proceeding, attempt to write module to indicated file. If unsuccessful,
        // display message.
        boolean exported = false;
        RolesExporterWizardPanel wizardPanel = new RolesExporterWizardPanel(this);
        String initialDirectory = (String)UserPreferences.getInstance().getValue(ConsoleConstants.CONSOLE_DIRECTORY_LOCATION_KEY);
        selectorPanel = new RolesTargetSelectorPanel(vdbName, vdbVersion, wizardPanel, initialDirectory);
        wizardPanel.addPage(selectorPanel);
        wizardPanel.getFinishButton().setEnabled(true);
        wizardPanel.getCancelButton().addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent ev) {
                cancelPressed();
            }
        });
        wizardPanel.getFinishButton().addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent ev) {
                finishPressed();
            }
        });
        dialog = new RolesExporterWizardPanelDialog(this, wizardPanel);
        dialog.show();
        if (finished) {
            String fullFileName = selectorPanel.getSelectedFileFullName();
            File target = new File(fullFileName);
            boolean proceeding = true;
            if (target.exists()) {
                String hdr = "File already exists";
                String msg = "File "
                             + fullFileName
                             + " already exists.  Exporting "
                             + "will overwrite its current contents.  Proceed and "
                             + "overwrite contents of file?";
                int response = DialogUtility.displayYesNoDialog(ConsoleMainFrame.getInstance(), hdr, msg);
                proceeding = (response == DialogUtility.YES);
            } else {
                try {
                    target.createNewFile();
                } catch (IOException ex) {
                    StaticUtilities.displayModalDialogWithOK("Cannot create target file",
                                                             "Unable to create target file "
                                                                             + fullFileName
                                                                             + ".  Must select another file name or cancel.");
                    proceeding = false;
                }
            }
            if (proceeding) {
                if (!target.canWrite()) {
                    StaticUtilities.displayModalDialogWithOK("Cannot write to target file",
                                                             "Unable to write to target file "
                                                                             + fullFileName
                                                                             + ".  Must select another file name or cancel.");
                    proceeding = false;
                }
                if (proceeding) {
                    try {
                        // Export the Roles
                        exportVdbRoles(this.vdbName,this.vdbVersionStr,fullFileName);

                        StaticUtilities.displayModalDialogWithOK("Export Roles successful", "Roles for VDB "
                                                                                      + vdbName
                                                                                      + " version "
                                                                                      + vdbVersion
                                                                                      + " successfully exported to file "
                                                                                      + fullFileName
                                                                                      + ".");
                        exported = true;
                    } catch (Exception ex) {
                        LogManager.logError(LogContexts.VIRTUAL_DATABASE, ex, "Error exporting VDB Roles.");
                        ExceptionUtility.showMessage("Error exporting VDB Roles", ex);
                    }
                    if (exported) {
                        String directoryForModule = selectorPanel.getDirectoryName();
                        UserPreferences.getInstance().setValue(ConsoleConstants.CONSOLE_DIRECTORY_LOCATION_KEY,
                                                               directoryForModule);
                        UserPreferences.getInstance().saveChanges();
                    }
                }
            }
        }
        selectorPanel = null;
        wizardPanel = null;
        return exported;
    }

    public void dialogWindowClosing() {
        // cancelled = true;
    }

    public boolean showNextPage() {
        boolean continuing = true;
        // Is there anything to do here?
        return continuing;
    }

    public void showPreviousPage() {
        // Is there anything to do here?
    }

    private void cancelPressed() {
        dialog.cancelPressed();
        // cancelled = true;
    }

    private void finishPressed() {
        dialog.finishPressed();
        finished = true;
    }
    
    /**
     * Export the Roles for the supplied VDB name/version to the supplied fileName.
     * @param vdbName the VDB name.
     * @param vdbVersion the VDB version.
     * @param fillFileName the fullName of the export file.
     * @throws CommunicationException 
     * 
     */
    private void exportVdbRoles(String vdbName, String vdbVersion, String fullFileName) throws LogonException, AdminException, CommunicationException {
        ServerAdmin admin = null;
        try {
            admin = this.connection.getServerAdmin();
            char[] chars = admin.exportDataRoles(vdbName,vdbVersion);
            if (chars == null) {
            	// If nothing was exported
            }
            FileUtil util = new FileUtil(fullFileName);
            util.write(new String(chars));
        } finally {
            if (admin != null) {
                admin.close();
            }
        }
    }

}// end VDBRolesExporter


/**
 * RolesExporterWizardPanel - controls page display.
 * 
 */
class RolesExporterWizardPanel extends WizardInterfaceImpl {

    private VDBRolesExporter controller;

    public RolesExporterWizardPanel(VDBRolesExporter cntrlr) {
        super();
        controller = cntrlr;
    }

    public void showNextPage() {
        boolean continuing = controller.showNextPage();
        if (continuing) {
            super.showNextPage();
        }
    }

    public void showPreviousPage() {
        controller.showPreviousPage();
        super.showPreviousPage();
    }
}// end RolesExporterWizardPanel


/**
 * RolesTargetSelectorPanel - Panel for Selection of the export file.
 * 
 */
class RolesTargetSelectorPanel extends BasicWizardSubpanelContainer {
    private String vdbName;
    private int vdbVersion;
    private DirectoryChooserPanel chooser;
    private FileSystemView fileSystemView;

    /**
     * Constructor.
     * @param name the name of the VDB
     * @param version the version of the VDB
     * @param wizardInterface the wizard interface
     * @param initialDirectory the initial directory for the file selector
     * 
     */
    public RolesTargetSelectorPanel(String name, int version,
                               WizardInterface wizardInterface,
                               String initialDirectory) {
        super(wizardInterface);
        vdbName = name;
        vdbVersion = version;
        fileSystemView = new FileSystemView();
        if ((initialDirectory != null) && (initialDirectory.length() > 0)) {
            try {
                fileSystemView.setHome(fileSystemView.lookup(initialDirectory));
            } catch (Exception ex) {
                // Any exception that may occur in setting the initial view is
                // inconsequential. This is merely a convenience to the user.
            }
        }
        chooser = new DirectoryChooserPanel(fileSystemView, DirectoryChooserPanel.TYPE_SAVE, getFileFilters(fileSystemView));
        chooser.setShowAcceptButton(false);
        chooser.setShowCancelButton(false);
        chooser.setShowDetailsButton(false);
        chooser.setShowFilterComboBox(false);
        chooser.setShowNewFolderButton(false);
        chooser.setShowPassThruFilter(false);
        chooser.setInitialFilename(vdbName + "_V" + vdbVersion + "_Roles." + VDBRolesExporter.VDB_ROLES_DEFAULT_EXT);
        chooser.addChangeListener(new ChangeListener() {

            public void stateChanged(ChangeEvent ev) {
                JTextComponent textField = (JTextComponent)ev.getSource();
                String textEntered = textField.getText().trim();
                chooserStateChanged(textEntered);
            }
        });
        super.setStepText(1, ConsolePlugin.Util.getString("RolesTargetSelectorPanel.stepText")); //$NON-NLS-1$
        super.setMainContent(chooser);
    }

    private void chooserStateChanged(String textEntered) {
        boolean enabling = (textEntered.length() > 0);
        AbstractButton forwardButton = getWizardInterface().getForwardButton();
        forwardButton.setEnabled(enabling);
    }

    /**
     * Get the selected file fullName.
     * @return the selected file fullName
     */
    public String getSelectedFileFullName() {
        String name = null;
        FileSystemEntry fse = (FileSystemEntry)chooser.getSelectedTreeNode();
        if (fse != null) {
            name = fse.getFullName();
        } else {
            String directory = chooser.getParentDirectoryEntry().toString();
            if (!directory.endsWith(File.separator)) {
                directory = directory + File.separator;
            }
            String fileNameEntered = chooser.getNameFieldText().trim();
            name = directory + fileNameEntered;
        }
        return name;
    }

    /**
     * Get the selected files directory
     * @return the selected file directory
     */
    public String getDirectoryName() {
        String directoryName = null;
        String fullName = getSelectedFileFullName();
        if (fullName != null) {
            int index = fullName.lastIndexOf(File.separatorChar);
            directoryName = fullName.substring(0, index);
        }
        return directoryName;
    }

    /**
     * Get the file filters for valid vdb roles file extensions
     * @return the file filters.
     */
    private FileSystemFilter[] getFileFilters(FileSystemView fsv) {
        FileSystemFilter[] filters = null;
        FileSystemFilter filter = new FileSystemFilter(fsv, VDBRolesExporter.VDB_ROLES_EXPORT_EXTENSIONS, VDBRolesExporter.VDB_ROLES_TYPE_FILE_DESC);

        filters = new FileSystemFilter[] {
            filter
        };

        return filters;
    }
}// end RolesTargetSelectorPanel


/**
 * RolesExporterWizardPanelDialog - the wizard dialog.
 * 
 */
class RolesExporterWizardPanelDialog extends JDialog {

    private VDBRolesExporter caller;
    private RolesExporterWizardPanel wizardPanel;

    /**
     * Constructor.
     * @param cllr the VDBRolesExporter
     * @param wizPnl the wizard panel
     */
    public RolesExporterWizardPanelDialog(VDBRolesExporter cllr,
                                     RolesExporterWizardPanel wizPnl) {
        super(ConsoleMainFrame.getInstance(), ConsolePlugin.Util.getString("RolesExporterWizardPanelDialog.title")); //$NON-NLS-1$
        caller = cllr;
        wizardPanel = wizPnl;
        this.setModal(true);
        this.addWindowListener(new WindowAdapter() {

            public void windowClosing(WindowEvent ev) {
                caller.dialogWindowClosing();
            }
        });
        init();
        this.pack();
        this.setLocation(StaticUtilities.centerFrame(this.getSize()));
    }

    /**
     * Initialization.
     */
    private void init() {
        GridBagLayout layout = new GridBagLayout();
        this.getContentPane().setLayout(layout);
        this.getContentPane().add(wizardPanel);
        layout.setConstraints(wizardPanel, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER,
                                                                  GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    }

    public void cancelPressed() {
        this.dispose();
    }

    public void finishPressed() {
        this.dispose();
    }
}// end RolesExporterWizardPanelDialog
