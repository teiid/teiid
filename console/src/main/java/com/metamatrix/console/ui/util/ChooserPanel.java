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

package com.metamatrix.console.ui.util;

import java.io.File;

import javax.swing.AbstractButton;
import javax.swing.JComponent;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.text.JTextComponent;

import com.metamatrix.common.tree.directory.DirectoryEntry;
import com.metamatrix.common.tree.directory.FileSystemFilter;
import com.metamatrix.common.tree.directory.FileSystemView;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.preference.UserPreferences;

/**
 */
public class ChooserPanel extends BasicWizardSubpanelContainer 
        implements MDCPOpenStateListener {

    private ModifiedDirectoryChooserPanel dirPnlChooser;
    private String userPrefKey = ConsoleConstants.CONSOLE_DIRECTORY_LOCATION_KEY;
//    private int openOrSaveType;
    private String path=null;
    
    public ChooserPanel(WizardInterface wizardInterface, int step, int openOrSave, String stepDesc, String[] fileExtensions, String fileViewDescription,
            String userPreferenceKey) {
        this(wizardInterface, step, openOrSave, stepDesc, fileExtensions, fileViewDescription);
        this.userPrefKey = userPreferenceKey;      
     }

    public ChooserPanel(WizardInterface wizardInterface, int step, int openOrSave, String stepDesc, String[] fileExtensions, String fileViewDescription) {
        super(wizardInterface);

        setStepText(step, stepDesc);
//        this.openOrSaveType = openOrSave;

        FileSystemView view = new FileSystemView();
        String dirTxt =
            (String)UserPreferences.getInstance()
                                   .getValue(userPrefKey);
        if (dirTxt != null) {
            try {
                view.setHome(view.lookup(dirTxt));
            } catch (Exception ex) {
                //Any exception that may occur on setting the initial view
                //is inconsequential.  This is merely a convenience to the
                //user.
            }
        }
        FileSystemFilter filter =
            new FileSystemFilter(view, fileExtensions, fileViewDescription);
        FileSystemFilter[] filters = {filter};

        dirPnlChooser = new ModifiedDirectoryChooserPanel(view, 
                    openOrSave, filters, this) {
            protected JComponent createNavigationBar() {
                return null;
            }
            protected void populateTopButtonsPanel() {
            }
        };
        
        dirPnlChooser.setAllowFolderCreation(false);
        dirPnlChooser.setShowNewFolderButton(false);        
        
    }
    private void chooserStateChanged(String textEntered) {
       
        boolean enabling = (textEntered.length() > 0);
        AbstractButton forwardButton = getWizardInterface().getForwardButton();
        forwardButton.setEnabled(enabling);
    }    
    /*
     * Use this method to set the chooser how you wish
     * prior to calling the init method.
     * At that point, the chooser is created.
     */
    
    public ModifiedDirectoryChooserPanel getChooserPanel() {
        return dirPnlChooser;
    }
    
         
    public void init() {
        
        dirPnlChooser.addChangeListener(new ChangeListener() {
            public void stateChanged(ChangeEvent ev) {
                JTextComponent textField = (JTextComponent)ev.getSource();
                String textEntered = textField.getText().trim();
                chooserStateChanged(textEntered);
            }
        });       
        setMainContent(dirPnlChooser);      
        
    }

    public void fileSelectionIsValid(boolean flag) {
        AbstractButton forwardButton = getWizardInterface()
                .getForwardButton();
        forwardButton.setEnabled(flag);
    }

    public boolean canOpen() {
        return dirPnlChooser.getAcceptButton().isEnabled();
    }

    public String getEnteredFileName() {       
        String name = null;
        String directory = dirPnlChooser.getParentDirectoryEntry().toString();
        
        DirectoryEntry fse = (DirectoryEntry)dirPnlChooser.getSelectedTreeNode();
        if (fse != null) {
            name = fse.getFullName();
        } else {
            if (!directory.endsWith(File.separator)) {
                directory = directory + File.separator;
            }
            String fileNameEntered = dirPnlChooser.getNameFieldText().trim();
            name = directory + fileNameEntered;
        }
        path = directory;
        
        return name;    
    }
    public String getSelectedFullFileName() {
        DirectoryEntry result =
            (DirectoryEntry)dirPnlChooser.getSelectedTreeNode();
        if (result != null) {
            path = dirPnlChooser.getParentDirectoryEntry().toString();
            
            return result.getNamespace();
        } 
        
        return null;

    }
    
    public String getSelectedFileName() {
        String fullName = getSelectedFullFileName();
        String fileName = StaticUtilities.getFileName(fullName);
        return fileName;
    }    
    
    public String getDirectoryName() {
        String directoryName = null;
        String fullName = getSelectedFullFileName();
        if (fullName != null) {
            int index = fullName.lastIndexOf(File.separatorChar);
            directoryName = fullName.substring(0, index);
        }
        return directoryName;
    }    
        
    public void saveCurrentDirLocation() {
        
        path = dirPnlChooser.getParentDirectoryEntry().toString();
        
        UserPreferences.getInstance().setValue(userPrefKey, 
                path);
        UserPreferences.getInstance().saveChanges();        
        
    }
        
}


