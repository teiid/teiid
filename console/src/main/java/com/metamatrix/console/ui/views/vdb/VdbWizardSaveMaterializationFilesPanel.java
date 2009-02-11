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

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.border.TitledBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.text.JTextComponent;

import com.metamatrix.common.tree.directory.FileSystemView;
import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.toolbox.ui.widget.DirectoryChooserPanel;
import com.metamatrix.toolbox.ui.widget.LabelWidget;


/** 
 * @since 4.2
 */
public class VdbWizardSaveMaterializationFilesPanel extends BasicWizardSubpanelContainer {
    private final static String TITLE = ConsolePlugin.Util.getString(
            "VdbWizardSaveMaterializationFilesPanel.title"); //$NON-NLS-1$
    private final static String FILES_DESC = ConsolePlugin.Util.getString(
            "VdbWizardSaveMaterializationFilesPanel.filesDesc"); //$NON-NLS-1$
    private final static String CONNECTION_PROPERTIES_FILE = 
            ConsolePlugin.Util.getString(
            "VdbWizardSaveMaterializationFilesPanel.connectionPropertiesFile"); //$NON-NLS-1$
    private final static String CREATE_SCRIPT_FILE = ConsolePlugin.Util.getString(
            "VdbWizardSaveMaterializationFilesPanel.createScriptFile"); //$NON-NLS-1$
    private final static String LOAD_SCRIPT_FILE = ConsolePlugin.Util.getString(
            "VdbWizardSaveMaterializationFilesPanel.loadScriptFile"); //$NON-NLS-1$
    private final static String SWAP_SCRIPT_FILE = ConsolePlugin.Util.getString(
            "VdbWizardSaveMaterializationFilesPanel.swapScriptFile"); //$NON-NLS-1$
    private final static String TRUNCATE_SCRIPT_FILE = ConsolePlugin.Util.getString(
            "VdbWizardSaveMaterializationFilesPanel.truncateScriptFile"); //$NON-NLS-1$
    private final static int FILE_TYPES_INSET = 40;
    private final static int FILE_NAMES_INSET = 20;
        
    private DirectoryChooserPanel chooser;
    private JLabel connectionPropsLabel;
    private JLabel createScriptLabel;
    private JLabel loadScriptLabel;
    private JLabel swapScriptLabel;
    private JLabel truncateScriptLabel;
    private JLabel connectionPropsFile;
    private JLabel createScriptFile;
    private JLabel loadScriptFile;
    private JLabel swapScriptFile;
    private JLabel truncateScriptFile;
    
    public VdbWizardSaveMaterializationFilesPanel(WizardInterface wizardInterface, int stepNum,
                String initialDirectory) {
        super(wizardInterface);
        super.setStepText(stepNum, TITLE);
        JPanel thePanel = createPanel(initialDirectory);
        super.setMainContent(thePanel);
    }
    
    private JPanel createPanel(String initialDirectory) {
        JPanel panel = new JPanel();
        GridBagLayout layout = new GridBagLayout();
        panel.setLayout(layout);
        JPanel filesPanel = new JPanel();
        filesPanel.setBorder(new TitledBorder(FILES_DESC));
        panel.add(filesPanel);
        layout.setConstraints(filesPanel, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, 
                new Insets(10, 10, 10, 10), 0, 0));
        GridBagLayout filesLayout = new GridBagLayout();
        filesPanel.setLayout(filesLayout);
        connectionPropsLabel = new LabelWidget(CONNECTION_PROPERTIES_FILE);
        filesPanel.add(connectionPropsLabel);
        filesLayout.setConstraints(connectionPropsLabel, new GridBagConstraints(0, 0, 1, 1, 
                0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(0, FILE_TYPES_INSET, 0, 0), 0, 0));
        connectionPropsFile = new LabelWidget();
        filesPanel.add(connectionPropsFile);
        filesLayout.setConstraints(connectionPropsFile, new GridBagConstraints(1, 0, 1, 1,
                1.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
                new Insets(0, FILE_NAMES_INSET, 0, 0), 0, 0));
        createScriptLabel = new LabelWidget(CREATE_SCRIPT_FILE);
        filesPanel.add(createScriptLabel);
        filesLayout.setConstraints(createScriptLabel, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(0, FILE_TYPES_INSET, 0, 0), 0, 0));
        createScriptFile = new LabelWidget();
        filesPanel.add(createScriptFile);
        filesLayout.setConstraints(createScriptFile, new GridBagConstraints(1, 1, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE, 
                new Insets(0, FILE_NAMES_INSET, 0, 0), 0, 0));
        loadScriptLabel = new LabelWidget(LOAD_SCRIPT_FILE);
        filesPanel.add(loadScriptLabel);
        filesLayout.setConstraints(loadScriptLabel, new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(0, FILE_TYPES_INSET, 0, 0), 0, 0));
        loadScriptFile = new LabelWidget();
        filesPanel.add(loadScriptFile);
        filesLayout.setConstraints(loadScriptFile, new GridBagConstraints(1, 2, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(0, FILE_NAMES_INSET, 0, 0), 0, 0));
        swapScriptLabel = new LabelWidget(SWAP_SCRIPT_FILE);
        filesPanel.add(swapScriptLabel);
        filesLayout.setConstraints(swapScriptLabel, new GridBagConstraints(0, 3, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(0, FILE_TYPES_INSET, 0, 0), 0, 0));
        swapScriptFile = new LabelWidget();
        filesPanel.add(swapScriptFile);
        filesLayout.setConstraints(swapScriptFile, new GridBagConstraints(1, 3, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(0, FILE_NAMES_INSET, 0, 0), 0, 0));
        truncateScriptLabel = new LabelWidget(TRUNCATE_SCRIPT_FILE);
        filesPanel.add(truncateScriptLabel);
        filesLayout.setConstraints(truncateScriptLabel, new GridBagConstraints(0, 4, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(0, FILE_TYPES_INSET, 4, 0), 0, 0));
        truncateScriptFile = new LabelWidget();
        filesPanel.add(truncateScriptFile);
        filesLayout.setConstraints(truncateScriptFile, new GridBagConstraints(1, 4, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(0, FILE_NAMES_INSET, 4, 0), 0, 0));
                
        FileSystemView fileSystemView = new FileSystemView();
        if ((initialDirectory != null) && (initialDirectory.length() > 0)) {
            try {
                fileSystemView.setHome(fileSystemView.lookup(initialDirectory));
            } catch (Exception ex) {
                //Any exception that may occur in setting the initial view is
                //inconsequential.  This is merely a convenience to the user.
            }
        }
        chooser = new DirectoryChooserPanel(fileSystemView,
                DirectoryChooserPanel.TYPE_SAVE);
        chooser.setShowAcceptButton(false);
        chooser.setShowCancelButton(false);
        chooser.setShowDetailsButton(false);
        chooser.setShowFilterComboBox(false);
        chooser.setShowNewFolderButton(true);
        chooser.setFileNameFieldVisible(false);
        chooser.setShowPassThruFilter(false);
        chooser.setFilenameSelectionAllowed(false);
        chooser.addChangeListener(new ChangeListener() {
            public void stateChanged(ChangeEvent ev) {
                JTextComponent textField = (JTextComponent)ev.getSource();
                String textEntered = textField.getText().trim();
                chooserStateChanged(textEntered);
            }
        });
        panel.add(chooser);
        layout.setConstraints(chooser, new GridBagConstraints(0, 1, 1, 1, 1.0, 1.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(10, 10, 10, 10), 0, 0));
        return panel;
    }

    public void setConnectionPropsFileName(String name) {
        connectionPropsFile.setText(name);
        connectionPropsLabel.setText(connectionPropsLabel.getText() + ':');
    }
    
    public void setCreateScriptFileName(String name) {
        createScriptFile.setText(name);
        createScriptLabel.setText(createScriptLabel.getText() + ':');
    }
    
    public void setLoadScriptFileName(String name) {
        loadScriptFile.setText(name);
        loadScriptLabel.setText(loadScriptLabel.getText() + ':');
    }
    
    public void setSwapScriptFileName(String name) {
        swapScriptFile.setText(name);
        swapScriptLabel.setText(swapScriptLabel.getText() + ':');
    }
    
    public void setTruncateScriptFileName(String name) {
        truncateScriptFile.setText(name);
        truncateScriptLabel.setText(truncateScriptLabel.getText() + ':');
    }
    
    public String getDirectoryName() {
        String directoryName = chooser.getParentDirectoryEntry().toString();
        return directoryName;
    }
    
    private void chooserStateChanged(String textEntered) {
        String dirName = getDirectoryName();
        boolean enabling = ((dirName != null) && (dirName.length() > 0));
        enableForwardButton(enabling);
    }
}
