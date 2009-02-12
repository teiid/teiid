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

import java.awt.Component;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;

import javax.swing.AbstractButton;
import javax.swing.JPanel;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.tree.directory.DirectoryEntry;
import com.metamatrix.common.tree.directory.FileSystemEntry;
import com.metamatrix.common.tree.directory.FileSystemFilter;
import com.metamatrix.common.tree.directory.FileSystemView;
import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.ConsoleConstants;
import com.metamatrix.console.ui.util.MDCPOpenStateListener;
import com.metamatrix.console.ui.util.ModifiedDirectoryChooserPanel;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.ui.util.WizardInterfaceImpl;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.core.util.FileUtil;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.platform.admin.api.EntitlementMigrationReport;
import com.metamatrix.toolbox.preference.UserPreferences;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.DirectoryChooserPanel;

/**
 * ImportVdbRolesPanel - panels necessary for import of a VDB Roles file.
 * 
 */
public class ImportVdbRolesPanel extends WizardInterfaceImpl {

    final static String[] VDB_IMPORT_ROLES_EXTENSIONS = new String[] {"xml"};        //$NON-NLS-1$ 
    
    final static String VDB_ROLES_TYPE_FILE_DESC = "VDB Roles Import Types (*.xml)"; //$NON-NLS-1$

    ImportRolesFileSelectionPanel selectorPanel;
    VdbRoleOptionsPanel roleOptionsPanel;
    VdbWizardEntitlementsPanel roleReportPanel;

    private VirtualDatabase vdb;
    
    private CreateVDBPanelParent dlgParent = null;
    private ConnectionInfo connection = null;
    private EntitlementMigrationReport migrationReport = null;
    

    /**
     * Constructor
     * @param dlgParent the dialogs parent container
     * @param vdbSourceVdb the vdb to import roles
     * @param connection the connection object
     */
    public ImportVdbRolesPanel(CreateVDBPanelParent dlgParent,
                                VirtualDatabase vdbSourceVdb,
                                ConnectionInfo connection) {
        super();
        this.dlgParent = dlgParent;
        this.vdb = vdbSourceVdb;
        this.connection = connection;
        init();
    }

    /**
     * initialize the panels.
     * 
     */
    private void init() {
        // clear out the wizard panel
        int iPages = getPageCount();
        for (int x = 0; x < iPages; x++) {
            removePage(x);
        }

        String initialDirectory = (String)UserPreferences.getInstance()
                                                         .getValue(ConsoleConstants.CONSOLE_DIRECTORY_LOCATION_KEY);

        selectorPanel = new ImportRolesFileSelectionPanel(this, initialDirectory, 1);
        addPage(selectorPanel);

        roleOptionsPanel = new VdbRoleOptionsPanel(this.vdb, this, 2);
        addPage(roleOptionsPanel);

        roleReportPanel = new VdbWizardEntitlementsPanel(this.vdb, this, this.connection, 3);
        addPage(roleReportPanel);

        renumberPages();
        setListeners();

        this.getNextButton().setEnabled(false);
    }

    /**
     * initialize the listeners.
     * 
     */
    private void setListeners() {
        getCancelButton().addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent event) {
                processCancelButton();
            }
        });

        getFinishButton().addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent event) {
                processFinishButton();
            }
        });
    }

    /**
     * Helper method to set the page numbers for the wizard.
     * 
     */
    private void renumberPages() {
        Component[] thePages = getPages();
        for (int i = 0; i < thePages.length; i++) {
            if (thePages[i] instanceof BasicWizardSubpanelContainer) {
                BasicWizardSubpanelContainer bwsc = (BasicWizardSubpanelContainer)thePages[i];
                bwsc.replaceStepNum(i + 1);
            }
        }
    }

    /**
     * Cancel button processing for button pressed.
     * 
     */
    private void processCancelButton() {
        // let dialog do its thing
        dlgParent.processCancelButton();
    }

    /**
     * Finish button processing for button pressed.
     * 
     */
    private void processFinishButton() {
        // let dialog do its thing
        dlgParent.processFinishButton();
    }

    public AbstractButton getWizardNextButton() {
        return getNextButton();
    }

    public void enableNextButton(boolean b) {
        ButtonWidget btnNext = null;

        btnNext = getNextButton();

        if (btnNext != null) {
            btnNext.setEnabled(b);
        } else {
            btnNext = getFinishButton();
            if (btnNext != null) {
                btnNext.setEnabled(b);
            }
        }
    }

    /**
     * Get the current virtual database object.
     * @return the VirtualDatabase object.
     */
    public VirtualDatabase getVdb() {
        return this.vdb;
    }

    /** 
     * @see com.metamatrix.toolbox.ui.widget.WizardPanel#showNextPage()
     * @since 4.3
     */
    public void showNextPage() {
        try {
            StaticUtilities.startWait(dlgParent.getContentPane());
            JPanel pnlCurrPage = (JPanel)getCurrentPage();
            boolean proceeding = true;
            
            
            if (pnlCurrPage.equals(selectorPanel)) {
                proceeding = navigateFromSelectorPanel();
                
            } else if (pnlCurrPage.equals(roleOptionsPanel)) {
                proceeding = navigateFromRoleOptionsPanel();
                
            }
            
            if (! proceeding) {
                return;
            }
            
        } catch (Exception e) {
            ExceptionUtility.showMessage("ERROR!  failed navigating the wizard", e); //$NON-NLS-1$
        } finally {
            StaticUtilities.endWait(dlgParent.getContentPane());
        }
        super.showNextPage();
    }

    
    
    /**
     * Called when navigating from selectorPanel to the next panel 
     * @since 4.3
     */
    private boolean navigateFromSelectorPanel() throws Exception {
        boolean result = checkSelectorPanelResults();
        if(!result) return false;
        boolean proceeding = true;
        
        if (!proceeding) {
            processCancelButton();
        }
        return true;
    }
    
    /**
     * Called when navigating from pnlConfirm to the next panel 
     */
    private boolean navigateFromRoleOptionsPanel() {         
        try {
        	// Get fullname for the selected file.
            String fileName = selectorPanel.getSelectedFileFullName();
            
            // Check user option for overwrite of existing roles.
            boolean overwriteExisting = this.roleOptionsPanel.isSelectedOverwriteExistingRoles();
            
            char[] dataRoleContents = readFileToCharArray(fileName);
            
            // Do the roles import.
        	this.migrationReport = ModelManager.getVdbManager(this.connection).importEntitlements(vdb,dataRoleContents,overwriteExisting);
            
        } catch (Exception e) {
            LogManager.logError(LogContexts.VIRTUAL_DATABASE, e, "Error importing VDB Roles.");
            ExceptionUtility.showMessage("Error importing VDB Roles", e);
        }
        
    	roleReportPanel.setNewVdb(this.vdb);
    	roleReportPanel.setEntitlementMigrationReport(this.migrationReport);

        getBackButton().setVisible(false);
        getCancelButton().setVisible(false);
        return true;
    }
    
	/**
	 * Helper method converting the string filename to char array.
	 * @param fileName the name of the roles file.
	 * @return the char array version of the filename.
	 */
	private static char[] readFileToCharArray(String fileName) {
	    FileUtil util = new FileUtil(fileName);
	    return util.read().toCharArray();
	}
            
    /** 
     * @see com.metamatrix.toolbox.ui.widget.WizardPanel#showPreviousPage()
     * @since 4.3
     */
    public void showPreviousPage() {
        super.showPreviousPage();
        enableNextButton(true);
    }

    /**
     * This method does some validation checks on the selector Panel, then determines if
     * it is ok to proceed to the next page.
     * @return 'true' if selector panel is OK, 'false' if not.  
     */
    public boolean checkSelectorPanelResults() {
        boolean goingToNextPage = true;
        String fileName = selectorPanel.getSelectedFileFullName();
        
        if (fileName==null || fileName.trim().length()==0) return false;
        
        File file = new File(fileName);

        // ------------------------------------------------
        // Determine if the selected file exists
        // ------------------------------------------------
        if (!file.exists()) {
            StaticUtilities
                           .displayModalDialogWithOK("Unable to open file", //$NON-NLS-1$
                                                     "Unable to open file " + fileName + ".  Must select " + "a different file or cancel."); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            goingToNextPage = false;
        // ------------------------------------------------
        // Determine if the selected file can be read
        // ------------------------------------------------
        } else if (!file.canRead()) {
            StaticUtilities
                           .displayModalDialogWithOK("Unable to read file", //$NON-NLS-1$
                                                     "Unable to read file " + fileName + ".  Must select " + "a different file or cancel."); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            goingToNextPage = false;
        // ---------------------------------------------------------
        // File exists and is readable.  Save directory preference
        // ---------------------------------------------------------
        } else {
            String vdbDirectoryName = selectorPanel.getDirectoryName();

            UserPreferences.getInstance().setValue(ConsoleConstants.CONSOLE_DIRECTORY_LOCATION_KEY, vdbDirectoryName);
            UserPreferences.getInstance().saveChanges();
        }
        return goingToNextPage;

    }

}// end ImportVdbRolesPanel

/**
 * Panel for selection of Roles File to import.
 */
class ImportRolesFileSelectionPanel extends BasicWizardSubpanelContainer implements
                                                                   MDCPOpenStateListener {

    private ModifiedDirectoryChooserPanel chooser;

    /**
     * Constructor
     */
    public ImportRolesFileSelectionPanel(WizardInterface wizardInterface,
                                          String initialDirectory,
                                          int stepNum) {
        super(wizardInterface);

        FileSystemView view = new FileSystemView();
        if ((initialDirectory != null) && (initialDirectory.length() > 0)) {
            try {
                DirectoryEntry dirEntry = view.lookup(initialDirectory);
                view.setHome(dirEntry);
            } catch (Exception ex) {
                // Any exception that may occur on setting the initial view is
                // inconsequential. This is merely a convenience to the user.
            }
        }
        chooser = new ModifiedDirectoryChooserPanel(view, DirectoryChooserPanel.TYPE_OPEN, getFileFilters(view), this);
        chooser.setShowAcceptButton(false);
        chooser.setShowCancelButton(false);
        chooser.setShowDetailsButton(false);
        chooser.setShowFilterComboBox(false);
        chooser.setShowNewFolderButton(false);
        chooser.setShowPassThruFilter(false);
        
        String stepText = ConsolePlugin.Util.getString("ImportRolesFileSelectionPanel.stepText"); //$NON-NLS-1$
        super.setStepText(stepNum, stepText); 
        super.setMainContent(chooser);
    }

    /**
     * Sets forward button enabled state, based on valid file selection.
     */
    public void fileSelectionIsValid(boolean flag) {
        AbstractButton forwardButton = getWizardInterface().getForwardButton();
        forwardButton.setEnabled(flag);
    }

    /**
     * Get the full name of the selected file.
     * @return the file fullname.
     */
    public String getSelectedFileFullName() {
        String name = null;
        FileSystemEntry fse = (FileSystemEntry)chooser.getSelectedTreeNode();
        if (fse != null) {
            name = fse.getFullName();
        }
        return name;
    }

    /**
     * Get the directory portion of the selected file.
     * @return the selected file directory.
     */
    public String getDirectoryName() {
        String fullName = getSelectedFileFullName();
        String directoryName = StaticUtilities.getDirectoryName(fullName);
        return directoryName;
    }

    /**
     * Get the filename portion of the selected file.
     * @return the selected file name.
     */
    public String getSelectedFileName() {
        String fullName = getSelectedFileFullName();
        String fileName = StaticUtilities.getFileName(fullName);
        return fileName;
    }

    /**
     * Get the file extension filters.
     * @return the selected file extension filters.
     */
    private FileSystemFilter[] getFileFilters(FileSystemView fsv) {
        FileSystemFilter[] filters = null;
        FileSystemFilter filter = new FileSystemFilter(fsv, ImportVdbRolesPanel.VDB_IMPORT_ROLES_EXTENSIONS,
                                                       ImportVdbRolesPanel.VDB_ROLES_TYPE_FILE_DESC);

        filters = new FileSystemFilter[] {
            filter
        };

        return filters;
    }

}// end ImportRolesFileSelectionPanel
