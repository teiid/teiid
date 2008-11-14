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
import javax.swing.JPasswordField;
import javax.swing.JTextField;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.NoMinTextFieldWidget;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.toolbox.ui.widget.LabelWidget;

/** 
 * @since 4.2
 */
public class VdbWizardUserAndPasswordPanel extends BasicWizardSubpanelContainer {
    private final static String TITLE = ConsolePlugin.Util.getString(
            "VdbWizardUserAndPasswordPanel.title"); //$NON-NLS-1$
    private final static String LOGIN_DESC = ConsolePlugin.Util.getString(
            "VdbWizardUserAndPasswordPanel.loginDesc"); //$NON-NLS-1$
    private final static String TABLE_DESC = ConsolePlugin.Util.getString(
            "VdbWizardUserAndPasswordPanel.tableDesc"); //$NON-NLS-1$
    public final static String USER_NAME = ConsolePlugin.Util.getString(
            "VdbWizardUserAndPasswordPanel.userName"); //$NON-NLS-1$
    public final static String PASSWORD = ConsolePlugin.Util.getString(
            "VdbWizardUserAndPasswordPanel.password"); //$NON-NLS-1$            
    private final static int EDGE_INSETS = 4;
    
    private UserPasswordPanel loginPanel;
    private UserPasswordPanel dataBasePanel;
        
    public VdbWizardUserAndPasswordPanel(WizardInterface wizardInterface, int stepNum) {
        super(wizardInterface);
        super.setStepText(stepNum, TITLE);
        JPanel thePanel = createPanel();
        super.setMainContent(thePanel);
    }
    
    private JPanel createPanel() {
        JPanel panel = new JPanel();
        GridBagLayout layout = new GridBagLayout();
        panel.setLayout(layout);
        JLabel loginDescLabel = new LabelWidget(LOGIN_DESC);
        panel.add(loginDescLabel);
        layout.setConstraints(loginDescLabel, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(10, EDGE_INSETS, 4, EDGE_INSETS), 0, 0));
        loginPanel = new UserPasswordPanel(this);
        panel.add(loginPanel);
        layout.setConstraints(loginPanel, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE,
                new Insets(4, EDGE_INSETS, 40, EDGE_INSETS), 0, 0));
        JLabel tableDescLabel = new LabelWidget(TABLE_DESC);
        panel.add(tableDescLabel);
        layout.setConstraints(tableDescLabel, new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(10, EDGE_INSETS, 4, EDGE_INSETS), 0, 0));
        dataBasePanel = new UserPasswordPanel(this);
        panel.add(dataBasePanel);
        layout.setConstraints(dataBasePanel, new GridBagConstraints(0, 3, 1, 1, 0.0, 1.0,
                GridBagConstraints.NORTH, GridBagConstraints.NONE,
                new Insets(4, EDGE_INSETS, 4, EDGE_INSETS), 0, 0));
        return panel;
    }

    public void fieldsChanged() {
        boolean enabling = ((getLoginUserName().length() > 0) && 
                (getLoginPassword().length() > 0) && (getDataBaseUserName().length() > 0) 
                && (getDataBasePassword().length() > 0));
        enableForwardButton(enabling);
    }
    
    public String getLoginUserName() {
        return loginPanel.getUserName();
    }
    
    public String getLoginPassword() {
        return loginPanel.getPassword();
    }
    
    public String getDataBaseUserName() {
        return dataBasePanel.getUserName();
    }
    
    public String getDataBasePassword() {
        return dataBasePanel.getPassword();
    }
    
    public void resolveForwardButton() {
        if ((loginPanel != null) && (dataBasePanel != null)) {
            fieldsChanged();
        }
    }
}//end VdbWizardUserAndPasswordPanel




class UserPasswordPanel extends JPanel {
    private VdbWizardUserAndPasswordPanel caller;
    private JTextField userField;
    private JPasswordField passwordField;
    
    public UserPasswordPanel(VdbWizardUserAndPasswordPanel caller) {
        super();
        this.caller = caller;
        init();
    }
    
    private void init() {
        GridBagLayout layout = new GridBagLayout();
        this.setLayout(layout);
        JLabel userLabel = new LabelWidget(VdbWizardUserAndPasswordPanel.USER_NAME);
        this.add(userLabel);
        layout.setConstraints(userLabel, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(0, 0, 2, 2),
                0, 0));
        userField = new NoMinTextFieldWidget(50);
        this.add(userField);
        layout.setConstraints(userField, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(0, 2, 2, 0),
                0, 0));
        userField.getDocument().addDocumentListener(new DocumentListener() {
            public void changedUpdate(DocumentEvent ev) {
                caller.fieldsChanged();
            }
            public void insertUpdate(DocumentEvent ev) {
                caller.fieldsChanged();
            }
            public void removeUpdate(DocumentEvent ev) {
                caller.fieldsChanged();
            }
        });
        JLabel passwordLabel = new LabelWidget(VdbWizardUserAndPasswordPanel.PASSWORD);
        this.add(passwordLabel);
        layout.setConstraints(passwordLabel, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(2, 0, 0, 2),
                0, 0));
        passwordField = new JPasswordField(20);
        this.add(passwordField);
        layout.setConstraints(passwordField, new GridBagConstraints(1, 1, 1, 1, 1.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(2, 2, 0, 0),
                0, 0));
        passwordField.getDocument().addDocumentListener(new DocumentListener() {
            public void changedUpdate(DocumentEvent ev) {
                caller.fieldsChanged();
            }
            public void insertUpdate(DocumentEvent ev) {
                caller.fieldsChanged();
            }
            public void removeUpdate(DocumentEvent ev) {
                caller.fieldsChanged();
            }
        });
    }
    
    public String getUserName() {
        return userField.getText().trim();
    }
    
    public String getPassword() {
        return (new String(passwordField.getPassword())).trim();
    }
}//end UserPasswordPanel
