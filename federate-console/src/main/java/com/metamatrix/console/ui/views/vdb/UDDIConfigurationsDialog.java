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
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.ArrayList;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.DefaultTableModel;

import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.util.SavedUDDIRegistryInfo;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;


/** 
 * @since 4.2
 */
public class UDDIConfigurationsDialog extends JDialog {
    private final static String TITLE = ConsolePlugin.Util.getString(
            "UDDIConfigurationsDialog.title"); //$NON-NLS-1$
    private final static String SELECT_A_CONFIGURATION = ConsolePlugin.Util.getString(
            "UDDIConfigurationsDialog.selectAConfiguration"); //$NON-NLS-1$                                                                                      
    private final static String CONFIGURATIONS_COL_HDR = ConsolePlugin.Util.getString(
            "UDDIConfigurationsDialog.configurationsColHdr"); //$NON-NLS-1$
    private final static String ADD = ConsolePlugin.Util.getString(
            "General.AddWithEllipsis"); //$NON-NLS-1$
    private final static String REMOVE = ConsolePlugin.Util.getString(
            "General.Remove"); //$NON-NLS-1$
    private final static String CONFIG_NAME = ConsolePlugin.Util.getString(
            "UDDIConfigurationsDialog.configName") + ':'; //$NON-NLS-1$
    private final static String HOST = ConsolePlugin.Util.getString(
            "UDDIConfigurationsDialog.host") + ':'; //$NON-NLS-1$
    private final static String PORT = ConsolePlugin.Util.getString(
            "UDDIConfigurationsDialog.port") + ':'; //$NON-NLS-1$
    private final static String USER_NAME = ConsolePlugin.Util.getString(
            "UDDIConfigurationsDialog.userName") + ':'; //$NON-NLS-1$
    private final static String NOTE_LINE_1 = ConsolePlugin.Util.getString(
            "UDDIConfigurationsDialog.noteLine1"); //$NON-NLS-1$
    private final static String NOTE_LINE_2 = ConsolePlugin.Util.getString(
            "UDDIConfigurationsDialog.noteLine2"); //$NON-NLS-1$
    private final static String OK = ConsolePlugin.Util.getString("General.OK"); //$NON-NLS-1$
    private final static String CANCEL = ConsolePlugin.Util.getString("General.Cancel"); //$NON-NLS-1$
    
    private final static int NO_STATE = 1;
    private final static int EDITING = 2;
    private final static int ADDING = 3;
    private final static int REMOVING = 4;
    
    private final static int LEFT_INSET = 4;
    private final static int RIGHT_INSET = 4;
    
    private UDDIConfigurationsHandler handler;
    private SavedUDDIRegistryInfo[] items;
    private JTable configsTable;
    private JButton addButton;
    private JButton removeButton;
    private JButton okButton;
    private JButton cancelButton;
    private JTextField nameField;
    private JTextField hostField;
    private JTextField portField;
    private JTextField userField;
    private SavedUDDIRegistryInfo removedItem = null;
    private int curState = NO_STATE;
    
    public UDDIConfigurationsDialog(JFrame parentFrame, UDDIConfigurationsHandler handler,
            SavedUDDIRegistryInfo[] items) {
        super(parentFrame, TITLE, true);
        this.handler = handler;
        this.items = items;
        createComponent();
        insertItemsIntoTable();
    }
    
    private void createComponent() {
        this.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent ev) {
                cancelPressed();
            }
        });
        GridBagLayout layout = new GridBagLayout();
        this.getContentPane().setLayout(layout);
        JLabel selectLabel = new LabelWidget(SELECT_A_CONFIGURATION);
        this.getContentPane().add(selectLabel);
        layout.setConstraints(selectLabel, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE, 
                new Insets(10, LEFT_INSET, 0, RIGHT_INSET), 0, 0));                                                                 
        java.util.List colHdrs = new ArrayList(1);
        colHdrs.add(CONFIGURATIONS_COL_HDR);
        configsTable = new TableWidget(colHdrs);
        configsTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        configsTable.getSelectionModel().addListSelectionListener(new ListSelectionListener() {
            public void valueChanged(ListSelectionEvent ev) {
                if (!ev.getValueIsAdjusting()) {
                    selectionChanged();
                }
            }
        });
        JPanel configsPanel = new JPanel();
        this.getContentPane().add(configsPanel);
        layout.setConstraints(configsPanel, new GridBagConstraints(0, 1, 1, 1, 1.0, 1.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, 
                new Insets(0, LEFT_INSET, 0, RIGHT_INSET), 0, 0));
        GridBagLayout configsLayout = new GridBagLayout();
        configsPanel.setLayout(configsLayout);
        JScrollPane listScrollPane = new JScrollPane(configsTable);
        configsPanel.add(listScrollPane);
        configsLayout.setConstraints(listScrollPane, new GridBagConstraints(0, 0, 1, 1, 
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, 
                new Insets(0, 0, 0, 0), 0, 0));
        JPanel buttonsColumn = new JPanel(new GridLayout(2, 1, 0, 4));
        configsPanel.add(buttonsColumn);
        configsLayout.setConstraints(buttonsColumn, new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(2, 4, 2, 2), 0, 0));
        addButton = new ButtonWidget(ADD);
        addButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                addPressed();
            }
        });
        buttonsColumn.add(addButton);
        removeButton = new ButtonWidget(REMOVE);
        removeButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                removePressed();
            }
        });
        removeButton.setEnabled(false);
        
        JPanel textFieldsPanel = new JPanel();
        this.getContentPane().add(textFieldsPanel);
        layout.setConstraints(textFieldsPanel, new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0, 
                GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, 
                new Insets(30, 20, 10, 20), 0, 0));
        GridBagLayout textFieldsLayout = new GridBagLayout();
        textFieldsPanel.setLayout(textFieldsLayout);
        JLabel nameLabel = new LabelWidget(CONFIG_NAME);
        textFieldsPanel.add(nameLabel);
        textFieldsLayout.setConstraints(nameLabel, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        JLabel hostLabel = new LabelWidget(HOST);
        textFieldsPanel.add(hostLabel);
        textFieldsLayout.setConstraints(hostLabel, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        JLabel portLabel = new LabelWidget(PORT);
        textFieldsPanel.add(portLabel);
        textFieldsLayout.setConstraints(portLabel, new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        JLabel userLabel = new LabelWidget(USER_NAME);
        textFieldsPanel.add(userLabel);
        textFieldsLayout.setConstraints(userLabel, new GridBagConstraints(0, 3, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        DocumentListener docListener = new DocumentListener() {
            public void changedUpdate(DocumentEvent ev) {
                textFieldChanged();
            }
            public void removeUpdate(DocumentEvent ev) {
                textFieldChanged();
            }
            public void insertUpdate(DocumentEvent ev) {
                textFieldChanged();
            }
        };
        nameField = new TextFieldWidget();
        nameField.getDocument().addDocumentListener(docListener);
        nameField.setEnabled(false);
        textFieldsPanel.add(nameField);
        textFieldsLayout.setConstraints(nameField, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 2, 0, 0), 0, 0));
        hostField = new TextFieldWidget();
        hostField.getDocument().addDocumentListener(docListener);
        hostField.setEnabled(false);
        textFieldsPanel.add(hostField);
        textFieldsLayout.setConstraints(hostField, new GridBagConstraints(1, 1, 1, 1, 1.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 2, 0, 0), 0, 0));
        portField = new TextFieldWidget();
        portField.getDocument().addDocumentListener(docListener);
        portField.setEnabled(false);
        textFieldsPanel.add(portField);
        textFieldsLayout.setConstraints(portField, new GridBagConstraints(1, 2, 1, 1, 1.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 2, 0, 0), 0, 0));
        userField = new TextFieldWidget();
        userField.getDocument().addDocumentListener(docListener);
        userField.setEnabled(false);
        textFieldsPanel.add(userField);
        textFieldsLayout.setConstraints(userField, new GridBagConstraints(1, 3, 1, 1, 1.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 2, 0, 0), 0, 0));
        
        JPanel notePanel = new JPanel();
        this.getContentPane().add(notePanel);
        layout.setConstraints(notePanel, new GridBagConstraints(0, 3, 1, 1, 1.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2),
                0, 0));
        notePanel.setBorder(new TitledBorder(""));
        GridBagLayout noteLayout = new GridBagLayout();
        notePanel.setLayout(noteLayout);
        JLabel note1Label = new LabelWidget(NOTE_LINE_1);
        notePanel.add(note1Label);
        noteLayout.setConstraints(note1Label, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(2, 2, 0, 2), 0, 0));
        JLabel note2Label = new LabelWidget(NOTE_LINE_2);
        notePanel.add(note2Label);
        layout.setConstraints(note2Label, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(0, 2, 2, 2), 0, 0));
        
        JPanel buttonsPanel = new JPanel(new GridLayout(1, 2, 10, 0));
        this.getContentPane().add(buttonsPanel);
        layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 4, 1, 1, 0.0, 0.0,
                GridBagConstraints.EAST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
        okButton = new ButtonWidget(OK);
        okButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                okPressed();
            }
        });
        okButton.setEnabled(false);
        buttonsPanel.add(okButton);
        cancelButton = new ButtonWidget(CANCEL);
        cancelButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                cancelPressed();
            }
        });
        buttonsPanel.add(cancelButton);
        pack();
        this.setLocation(StaticUtilities.centerFrame(this.getSize()));
    }
    
    private void insertItemsIntoTable() {
        DefaultTableModel model = (DefaultTableModel)configsTable.getModel();
        for (int i = 0; i < items.length; i++) {
            String curName = items[i].getName();
            Object[] rowVals = new Object[] {curName};
            model.addRow(rowVals);
        }
    }
    
    private void selectionChanged() {
        removedItem = null;
        SavedUDDIRegistryInfo item = getSelectedItem();
        displayItem(item);
        if (item == null) {
            curState = NO_STATE;
            removeButton.setEnabled(false);
            okButton.setEnabled(false);
            disableText();
        } else {
            curState = EDITING;
            removeButton.setEnabled(true);
            okButton.setEnabled(true);
            enableText(false);
        }
    }
    
    private void textFieldChanged() {
        boolean newConfig = (curState == ADDING);
        String configName = nameField.getText().trim();
        String host = hostField.getText().trim();
        String port = portField.getText().trim();
        String user = userField.getText().trim();
        boolean entriesValid = isValidConfig(configName, host, port, user, newConfig);
        okButton.setEnabled(entriesValid);
    }
    
    private void addPressed() {
        removedItem = null;
        clearText();
        enableText(true);
        addButton.setEnabled(false);
        removeButton.setEnabled(false);
        okButton.setEnabled(false);
        clearSelection();
        curState = ADDING;
    }
    
    private void removePressed() {
        curState = REMOVING;
        removeButton.setEnabled(false);
        addButton.setEnabled(false);
        okButton.setEnabled(true);
        removedItem = getSelectedItem();
        clearText();
        disableText();
    }
    
    private void cancelPressed() {
        this.dispose();
    }
    
    private void okPressed() {
        this.dispose();
        switch (curState) {
            case EDITING:
                SavedUDDIRegistryInfo item = getSelectedItem();
                String configName = item.getName();
                String host = hostField.getText().trim();
                String port = portField.getText().trim();
                String user = userField.getText().trim();
                SavedUDDIRegistryInfo editedItem = new SavedUDDIRegistryInfo(configName, user, host, port);
                if (item.equals(editedItem)) {
                    handler.unchangedConfiguration(item);
                } else {
                    handler.editedConfiguration(editedItem);
                }
                break;
            case ADDING:
                configName = nameField.getText().trim();
                host = hostField.getText().trim();
                port = portField.getText().trim();
                user = userField.getText().trim();
                SavedUDDIRegistryInfo newItem = new SavedUDDIRegistryInfo(configName, user, host, port);
                handler.addedConfiguration(newItem);
                break;
            case REMOVING:
                handler.removedConfiguration(removedItem);
                break;
        }
    }
    
    private boolean isExistingConfigName(String configName) {
        boolean found = false;
        int i = 0;
        while ((!found) && (i < items.length)) {
            String curName = items[i].getName();
            if (configName.equals(curName)) {
                found = true;
            } else {
                i++;
            }
        }
        return found;
    }
    
    private boolean isValidConfig(String configName, String host, String port, String userName,
            boolean newConfig) {
        boolean valid = false;
        if ((configName.length() > 0) && (host.length() > 0) && (port.length() > 0) && (userName.length() >
                0)) {
            if (newConfig) {
                if (!isExistingConfigName(configName)) {
                    valid = true;
                }
            } else {
                valid = true;
            }
        }
        return valid;
    }
    
    private void clearText() {
        String empty = StringUtil.Constants.EMPTY_STRING;
        nameField.setText(empty);
        hostField.setText(empty);
        portField.setText(empty);
        userField.setText(empty);
    }
    
    private void enableText(boolean includingName) {
        hostField.setEnabled(true);
        portField.setEnabled(true);
        userField.setEnabled(true);
        if (includingName) {
            nameField.setEnabled(true);
        }
    }
    
    private void disableText() {
        nameField.setEnabled(false);
        hostField.setEnabled(false);
        portField.setEnabled(false);
        userField.setEnabled(false);
    }
    
    private SavedUDDIRegistryInfo getSelectedItem() {
        SavedUDDIRegistryInfo selectedItem = null;
        int selectedIndex = configsTable.getSelectedRow();
        if (selectedIndex >= 0) {
            selectedItem = items[selectedIndex];
        }
        return selectedItem;
    }
    
    private void displayItem(SavedUDDIRegistryInfo item) {
        if (item == null) {
            clearText();
        } else {
            nameField.setText(item.getName());
            hostField.setText(item.getHost());
            portField.setText(item.getPort());
            userField.setText(item.getUserName());
        }
    }
    
    private void clearSelection() {
        configsTable.clearSelection();
    }
}
