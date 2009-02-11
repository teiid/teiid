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

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeSet;

import javax.swing.JPanel;

import com.metamatrix.admin.api.objects.AdminOptions;
import com.metamatrix.admin.api.objects.ScriptsContainer;
import com.metamatrix.admin.api.server.ServerAdmin;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.util.ConsoleConstants;
import com.metamatrix.console.ui.util.WizardInterfaceImpl;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.preference.UserPreferences;

/**
 * Wizard for exporting Materialization scripts for a VDB.
 * 
 * @since 4.3
 */
public class MaterializationWizard extends WizardInterfaceImpl {

    
    private final static int NUM_MATERIALIZATION_FILES = 5;


    private VdbWizardUserAndPasswordPanel userPasswordPanel = null;
    private VdbWizardSaveMaterializationFilesPanel savePanel = null;
    private VdbWizardWrittenMaterializationFilesPanel writtenFilesPanel = null;
    private String vdbName;
    private String vdbVersion;
    private ScriptsContainer scripts = null;


    MaterializationWizardDialog parentDialog = null;

    private ConnectionInfo connection = null;


    public MaterializationWizard(MaterializationWizardDialog parentDialog,
                                 String vdbName,
                                 String vdbVersion,
                                 ConnectionInfo connection) {
        super();
        this.parentDialog = parentDialog;
        this.vdbName = vdbName;
        this.vdbVersion = vdbVersion;
        this.connection = connection;
        init();
    }


    private void init() {
        
        userPasswordPanel = new VdbWizardUserAndPasswordPanel(this, 1);

        String initialDirectory = 
            (String) UserPreferences.getInstance().getValue(ConsoleConstants.SAVE_MATERIALIZATION_DIRECTORY_LOCATION_KEY);
        savePanel = new VdbWizardSaveMaterializationFilesPanel(this, 2, initialDirectory);
        
        writtenFilesPanel = new VdbWizardWrittenMaterializationFilesPanel(this, 3, false);
        
        addPage(userPasswordPanel);
        addPage(savePanel);
        addPage(writtenFilesPanel);
        getForwardButton().setEnabled(false);
        
        setListeners();
    }

    

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

      

    private void processCancelButton() {
        parentDialog.processCancelButton();
    }

    private void processFinishButton() {
        boolean proceed = saveScripts();
                
        if (proceed) {
            parentDialog.processFinishButton();
        }
    }

   
    
    /** 
     * @see com.metamatrix.toolbox.ui.widget.WizardPanel#showNextPage()
     * @since 4.3
     */
    public void showNextPage() {
        boolean proceed = true;
        try {
            StaticUtilities.startWait(parentDialog.getContentPane());
            JPanel currentPanel = (JPanel)getCurrentPage();

            if (currentPanel.equals(savePanel)) {
                proceed = generateScripts(); 
            }
        } finally {
            StaticUtilities.endWait(parentDialog.getContentPane());
        }
        
        if (proceed) {
            super.showNextPage();
        }
    }
    
    
    /**
     * Create the scripts, but don't save them to the filesystem yet. 
     * @return whether this operation succeeded.
     * @since 4.3
     */
    private boolean generateScripts() {        
        
        ServerAdmin admin = null;
        String directory = null;
        try {
            admin = connection.getServerAdmin();
            
            //create the scripts
            String loginUserName = userPasswordPanel.getLoginUserName();
            String loginPassword = userPasswordPanel.getLoginPassword();
            String dataBaseUserName = userPasswordPanel.getDataBaseUserName();
            String dataBasePassword = userPasswordPanel.getDataBasePassword();
            
            scripts = admin.generateMaterializationScripts(vdbName, vdbVersion,
                                                 loginUserName, loginPassword, 
                                                 dataBaseUserName, dataBasePassword);
            
            directory = savePanel.getDirectoryName();

        } catch (Exception ex) {
            String msg = ConsolePlugin.Util.getString("MaterializationWizard.errorGenerating"); //$NON-NLS-1$
            LogManager.logError(LogContexts.VIRTUAL_DATABASE, ex, msg);
            ExceptionUtility.showMessage(msg, ex.getMessage(), ex);
            return false;
        } 
        
        Object materializationFileCreationResult = null;
        SingleMaterializationFileDisplayInfo[] files = new SingleMaterializationFileDisplayInfo[NUM_MATERIALIZATION_FILES];
        
        Collection fileNames = new TreeSet(scripts.getFileNames());
        Iterator iter = fileNames.iterator();
        for (int i=0; i<NUM_MATERIALIZATION_FILES; i++) {
            files[i] = new SingleMaterializationFileDisplayInfo(null, (String) iter.next());
        }
        AllMaterializationFilesDisplayInfo allFiles = new AllMaterializationFilesDisplayInfo(directory, files);
        materializationFileCreationResult = allFiles;
        writtenFilesPanel.setResults(materializationFileCreationResult);
        
        return true;
    }
    
  
    /**
     * Save the scripts to the filesystem. 
     * @return whether this operation succeeded.
     * @since 4.3
     */
    private boolean saveScripts() {
        try {
            //write the scripts
            String directory = savePanel.getDirectoryName();
            boolean overwrite = writtenFilesPanel.getOverwrite();
            
            AdminOptions options;
            if (overwrite) {
                options = new AdminOptions(AdminOptions.OnConflict.OVERWRITE);
            } else {
                options = new AdminOptions(AdminOptions.OnConflict.EXCEPTION);
            }
            scripts.saveAllToDirectory(directory, options);
            
            
            //save directory to preferences
            UserPreferences.getInstance().setValue(ConsoleConstants.SAVE_MATERIALIZATION_DIRECTORY_LOCATION_KEY, directory);
            UserPreferences.getInstance().saveChanges();
            
            return true;
        } catch (Exception ex) {
            String msg = ConsolePlugin.Util.getString("MaterializationWizard.errorSaving"); //$NON-NLS-1$
            LogManager.logError(LogContexts.VIRTUAL_DATABASE, ex, msg);
            ExceptionUtility.showMessage(msg, ex.getMessage(), ex);
            
            return false;
        }
    }
   
}

