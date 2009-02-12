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
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.IOException;

import javax.swing.AbstractButton;
import javax.swing.JDialog;
import javax.swing.JOptionPane;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.text.JTextComponent;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.tree.directory.FileSystemEntry;
import com.metamatrix.common.tree.directory.FileSystemFilter;
import com.metamatrix.common.tree.directory.FileSystemView;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.models.VdbManager;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.ConsoleConstants;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.ui.util.WizardInterfaceImpl;
import com.metamatrix.console.util.DialogUtility;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.core.CoreConstants;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.toolbox.preference.UserPreferences;
import com.metamatrix.toolbox.ui.widget.DirectoryChooserPanel;

public class VDBExporter {
    
    
    final static String[] VDB_EXPORT_EXTENSIONS = new String[] {
        "vdb"
    };

    final static String VDB_TYPE_FILE_DESC = "VDB Export Types (*.vdb)";

    private ConnectionInfo connection;
    private VirtualDatabase vdb;
    private String vdbName;
    private int vdbVersion;
    private TargetSelectorPanel selectorPanel = null;
    // private boolean cancelled = false;
    private boolean finished = false;
    private ExporterWizardPanelDialog dialog = null;

    public VDBExporter(VirtualDatabase vdb,
                       ConnectionInfo connection) {
        super();
        this.vdb = vdb;
        this.connection = connection;
        vdbName = this.vdb.getName();
        VirtualDatabaseID id = (VirtualDatabaseID)this.vdb.getID();
        String versionStr = id.getVersion();
        vdbVersion = (new Integer(versionStr)).intValue();
    }

    private VdbManager getVdbManager() {
        return ModelManager.getVdbManager(connection);
    }

    public boolean go() {
        // 1-panel wizard plus possible warning dialog. Simply display file
        // selector widget, enabling "Finish" when a selection has been entered.
        // Then, if file exists display warning dialog that it will be overwritten.
        // If proceeding, attempt to write module to indicated file. If unsuccessful,
        // display message.
        boolean exported = false;
        ExporterWizardPanel wizardPanel = new ExporterWizardPanel(this);
        String initialDirectory = (String)UserPreferences.getInstance().getValue(ConsoleConstants.CONSOLE_DIRECTORY_LOCATION_KEY);
        selectorPanel = new TargetSelectorPanel(vdbName, wizardPanel, initialDirectory);
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
        dialog = new ExporterWizardPanelDialog(this, wizardPanel);
        dialog.show();
        if (finished) {
            String fullFileName = selectorPanel.getSelectedFileName();
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
                        String fileName = StaticUtilities.getFileName(fullFileName);
                        String directoryName = StaticUtilities.getDirectoryName(fullFileName);
                        getVdbManager().exportVDB(vdb, fileName, directoryName);
                        StaticUtilities.displayModalDialogWithOK("Export successful", "VDB "
                                                                                      + vdbName
                                                                                      + " version "
                                                                                      + vdbVersion
                                                                                      + " successfully exported to file "
                                                                                      + fullFileName
                                                                                      + ".");
                        exported = true;
                    } catch (Exception ex) {
                        MetaMatrixException mex = ExceptionUtility.containsExceptionHavingCode(ex, "ERR.018.001.0021");
                        if (mex != null) {
                            String msg = "Export unsuccessful.  One or more " + "model files for the VDB were not found.";
                            StaticUtilities.displayModalDialogWithOK("Model File not Found", msg, JOptionPane.ERROR_MESSAGE);
                        } else {
                            LogManager.logError(LogContexts.VIRTUAL_DATABASE, ex, "Error exporting VDB.");
                            ExceptionUtility.showMessage("Error exporting to file", ex);
                        }
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
}// end VDBExporter

class ExporterWizardPanel extends WizardInterfaceImpl {

    private VDBExporter controller;

    public ExporterWizardPanel(VDBExporter cntrlr) {
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
}

class TargetSelectorPanel extends BasicWizardSubpanelContainer {
    private String vdbName;
    private DirectoryChooserPanel chooser;
    private FileSystemView fileSystemView;

    public TargetSelectorPanel(String name,
                               WizardInterface wizardInterface,
                               String initialDirectory) {
        super(wizardInterface);
        vdbName = name;
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
        chooser.setInitialFilename(vdbName + CoreConstants.VDB_ARCHIVE_EXTENSION);
        chooser.addChangeListener(new ChangeListener() {

            public void stateChanged(ChangeEvent ev) {
                JTextComponent textField = (JTextComponent)ev.getSource();
                String textEntered = textField.getText().trim();
                chooserStateChanged(textEntered);
            }
        });
        super.setStepText(1, "Select destination file for VDB.");
        super.setMainContent(chooser);
    }

    private void chooserStateChanged(String textEntered) {
        boolean enabling = (textEntered.length() > 0);
        AbstractButton forwardButton = getWizardInterface().getForwardButton();
        forwardButton.setEnabled(enabling);
    }

    public String getSelectedFileName() {
        String name = null;
        String fileNameEntered = chooser.getNameFieldText().trim();
        
        if (fileNameEntered != null && fileNameEntered.length() > 0) {
            String directory = chooser.getParentDirectoryEntry().toString();
            if (!directory.endsWith(File.separator)) {
                directory = directory + File.separator;
            }
            
            name = directory + fileNameEntered;
        } else {
            
            FileSystemEntry fse = (FileSystemEntry) chooser.getSelectedTreeNode();
            if (fse != null) {
                name = fse.getFullName();
            } 
            
        }
        return name;
    }

    public String getDirectoryName() {
        String directoryName = null;
        String fullName = getSelectedFileName();
        if (fullName != null) {
            int index = fullName.lastIndexOf(File.separatorChar);
            directoryName = fullName.substring(0, index);
        }
        return directoryName;
    }

    private FileSystemFilter[] getFileFilters(FileSystemView fsv) {
        FileSystemFilter[] filters = null;
        FileSystemFilter filter = new FileSystemFilter(fsv, VDBExporter.VDB_EXPORT_EXTENSIONS, VDBExporter.VDB_TYPE_FILE_DESC);

        filters = new FileSystemFilter[] {
            filter
        };

        return filters;
    }
}// end TargetSelectorPanel

class ExporterWizardPanelDialog extends JDialog {

    private VDBExporter caller;
    private ExporterWizardPanel wizardPanel;

    public ExporterWizardPanelDialog(VDBExporter cllr,
                                     ExporterWizardPanel wizPnl) {
        super(ConsoleMainFrame.getInstance(), "Export VDB");
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
}// end ExporterWizardPanelDialog
