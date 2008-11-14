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

package com.metamatrix.console.ui.dialog;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.Arrays;

import javax.swing.JPanel;
import javax.swing.JPasswordField;

import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;

public class ResetPasswordPanel extends JPanel{

    /**
     * Title suitable for Dialog or Window
     */
    public static final String TITLE = "Change password"; //$NON-NLS-1$

    /**
     * Message suitable for showing a confirmation that the password was
     *changed successfully
     */
    public static final String SUCCESS_MESSAGE = "Password changed."; //$NON-NLS-1$

    /**
     * Message suitable for indicating that the password change failed,
     *due to none being entered, or to both fields not containing the
     *same String
     */
    public static final String FAIL_MESSAGE = "Password was not changed.\nCheck that password was entered correctly twice."; //$NON-NLS-1$


	private static final String USERNAME_LABEL_TEXT = "Username:"; //$NON-NLS-1$
	private static final String PASSWORD_LABEL_TEXT = "Password:"; //$NON-NLS-1$
	private static final String CONFIRM_PASSWORD_LABEL_TEXT = "Confirm Password:"; //$NON-NLS-1$

    private LabelWidget usernameLabel, passwordLabel, confirmPasswordLabel;
    private TextFieldWidget usernameField;
    private JPasswordField passwordField;
    private JPasswordField confirmPasswordField;
    private GridBagLayout l;
    private GridBagConstraints c;

    public ResetPasswordPanel(){
        super();
    }

    public char[] getPassword(){
        if (this.validatePasswords()){
            return this.passwordField.getPassword();
        }
        return null;
    }

    private boolean validatePasswords(){
        return Arrays.equals(this.passwordField.getPassword(), this.confirmPasswordField.getPassword());
    }

    public void setUsername(String username){
        this.usernameField.setText(username);
    }

    public String getUsername(){
        return this.usernameField.getText().trim();
    }

    public void init(){
        usernameLabel = new LabelWidget(USERNAME_LABEL_TEXT);
        passwordLabel = new LabelWidget(PASSWORD_LABEL_TEXT);
        confirmPasswordLabel = new LabelWidget(CONFIRM_PASSWORD_LABEL_TEXT);

        usernameField = new TextFieldWidget();
        usernameField.setText(""); //$NON-NLS-1$
        usernameField.setEditable(false);

        passwordField = new JPasswordField();
        passwordField.setText(""); //$NON-NLS-1$

        confirmPasswordField = new JPasswordField();
        confirmPasswordField.setText(""); //$NON-NLS-1$

        l = new GridBagLayout();
        this.setLayout(l);
        c = new GridBagConstraints();

        layoutStuff();

        this.setVisible(true);
    }

    private void layoutStuff(){
        /*
        Insets fiveAtTop = new Insets(5, 0, 0, 0);
        Insets fiveAtTopAndRight = new Insets(5, 0, 0, 5);
        Insets fiveTopTwentyRight = new Insets(5, 0, 0, 20);
        Insets fiveRight = new Insets(0, 0, 0, 5);
        Insets twentyRight = new Insets(0, 0, 0, 20);
        */
        Insets insets = new Insets(5,5,5,5);

        c.ipadx = 0;
        c.ipady = 0;
        c.gridx = 0;
        c.gridy = 0;
        c.fill = GridBagConstraints.NONE;
        c.anchor = GridBagConstraints.NORTHEAST;
        c.weightx = 0.1;
        c.weighty = 0.1;
        c.gridwidth = 1;
        c.gridheight = 1;
        c.insets = insets;
        l.setConstraints(usernameLabel, c);
        this.add(usernameLabel);

        c.ipadx = 0;
        c.ipady = 0;
        c.gridx = 1;
        c.gridy = 0;
        c.gridwidth = 1;
        c.gridheight = 1;
        c.weightx = 1.0;
        c.weighty = 0.1;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.anchor = GridBagConstraints.NORTH;
        c.insets = insets;
        l.setConstraints(usernameField, c);
        this.add(usernameField);

        c.ipadx = 0;
        c.ipady = 0;
        c.gridx = 0;
        c.gridy = 1;
        c.gridwidth = 1;
        c.gridheight = 1;
        c.weightx = 0.1;
        c.weighty = 0.1;
        c.fill = GridBagConstraints.NONE;
        c.anchor = GridBagConstraints.NORTHEAST;
        c.insets = insets;
        l.setConstraints(passwordLabel, c);
        this.add(passwordLabel);

        c.ipadx = 0;
        c.ipady = 0;
        c.gridx = 1;
        c.gridy = 1;
        c.gridwidth = 1;
        c.gridheight = 1;
        c.weightx = 1.0;
        c.weighty = 0.1;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.anchor = GridBagConstraints.NORTH;
        c.insets = insets;
        l.setConstraints(passwordField, c);
        this.add(passwordField);

        c.ipadx = 0;
        c.ipady = 0;
        c.gridx = 0;
        c.gridy = 2;
        c.gridheight = 1;
        c.gridwidth = 1;
        c.weightx = 0.1;
        c.weighty = 1.0;
        c.fill = GridBagConstraints.NONE;
        c.anchor = GridBagConstraints.NORTHEAST;
        c.insets = insets;
        l.setConstraints(confirmPasswordLabel, c);
        this.add(confirmPasswordLabel);

        c.ipadx = 0;
        c.ipady = 0;
        c.gridx = 1;
        c.gridy = 2;
        c.gridwidth = 1;
        c.gridheight = 1;
        c.weightx = 1.0;
        c.weighty = 1.0;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.anchor = GridBagConstraints.NORTH;
        c.insets = insets;
        l.setConstraints(confirmPasswordField, c);
        this.add(confirmPasswordField);

    }
}


