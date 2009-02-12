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

package com.metamatrix.console.ui.views.connectorbinding;

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.event.*;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConnectorManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.util.*;
import com.metamatrix.toolbox.ui.widget.*;

public class ConnectorBindingRenameDialog extends JDialog {

    private ConnectionInfo connection;
    private boolean cancelled = false;
    private TextFieldWidget nameField;
    private JButton okButton;
    private String title;

    public ConnectorBindingRenameDialog(ConnectionInfo connection,
                                        String curName,
                                        String title) {
        super(ConsoleMainFrame.getInstance(), title, true); //$NON-NLS-1$
        this.connection = connection;
        this.title = title;
        init(curName);
    }

    private ConnectorManager getConnectorManager() {
        return ModelManager.getConnectorManager(connection);
    }

    private void init(String curName) {
        GridBagLayout layout = new GridBagLayout();
        this.getContentPane().setLayout(layout);
        GridBagLayout cl = new GridBagLayout();

        JPanel clonePanel = new JPanel();
        clonePanel.setLayout(cl);
        JLabel topRowText = new LabelWidget(title + ":");//$NON-NLS-1$
        // curName);
        TextFieldWidget cloneField = new TextFieldWidget(50);
        cloneField.setEditable(false);

        cloneField.setText(curName);
        clonePanel.add(topRowText);
        clonePanel.add(cloneField);

        cl.setConstraints(topRowText, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.EAST,
                                                             GridBagConstraints.NONE, new Insets(0, 0, 0, 4), 0, 0));
        cl.setConstraints(cloneField, new GridBagConstraints(1, 0, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER,
                                                             GridBagConstraints.HORIZONTAL, new Insets(0, 4, 0, 0), 0, 0));

        GridBagLayout tl = new GridBagLayout();

        nameField = new TextFieldWidget(50);
        JPanel namePanel = new JPanel();
        namePanel.setLayout(tl);
        JLabel nameLabel = new LabelWidget("New Connector Binding Name:"); //$NON-NLS-1$
        namePanel.add(nameLabel);
        namePanel.add(nameField);
        tl.setConstraints(nameLabel, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.EAST,
                                                            GridBagConstraints.NONE, new Insets(0, 0, 0, 4), 0, 0));
        tl.setConstraints(nameField, new GridBagConstraints(1, 0, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER,
                                                            GridBagConstraints.HORIZONTAL, new Insets(0, 4, 0, 0), 0, 0));
        JPanel buttonsPanel = new JPanel(new GridLayout(1, 2, 10, 0));
        okButton = new ButtonWidget("OK"); //$NON-NLS-1$
        buttonsPanel.add(okButton);
        JButton cancelButton = new ButtonWidget("Cancel"); //$NON-NLS-1$
        buttonsPanel.add(cancelButton);

        this.getContentPane().add(clonePanel);
        this.getContentPane().add(namePanel);
        this.getContentPane().add(buttonsPanel);
        // layout.setConstraints(clonePanel, new GridBagConstraints(0, 0, 1, 1,
        // 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
        // new Insets(4, 4, 10, 4), 0, 0));
        layout.setConstraints(clonePanel, new GridBagConstraints(0, 1, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER,
                                                                 GridBagConstraints.BOTH, new Insets(10, 10, 10, 10), 0, 0));

        layout.setConstraints(namePanel, new GridBagConstraints(0, 2, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER,
                                                                GridBagConstraints.BOTH, new Insets(10, 10, 10, 10), 0, 0));
        layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 3, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                                                                   GridBagConstraints.NONE, new Insets(10, 10, 4, 10), 0, 0));

        this.pack();
        Dimension size = this.getSize();
        Point centeringLoc = StaticUtilities.centerFrame(size);
        this.setLocation(centeringLoc);

        okButton.setEnabled(false);
        okButton.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent ev) {
                okPressed();
            }
        });
        cancelButton.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent ev) {
                cancelPressed();
            }
        });
        this.addWindowListener(new WindowAdapter() {

            public void windowClosing(WindowEvent ev) {
                cancelPressed();
            }
        });
        nameField.getDocument().addDocumentListener(new DocumentListener() {

            public void changedUpdate(DocumentEvent ev) {
                nameTextChanged();
            }

            public void insertUpdate(DocumentEvent ev) {
                nameTextChanged();
            }

            public void removeUpdate(DocumentEvent ev) {
                nameTextChanged();
            }
        });

        nameField.requestFocus();

    }

    private void okPressed() {
        String newName = nameField.getText().trim();
        boolean alreadyExists = false;
        boolean continuing = true;
        try {
            alreadyExists = getConnectorManager().connectorBindingNameAlreadyExists(newName);
        } catch (Exception ex) {
            LogManager.logError(LogContexts.CONNECTOR_BINDINGS, ex, "Error checking for connector binding name uniqueness"); //$NON-NLS-1$
            ExceptionUtility.showMessage("Error checking for connector binding " + //$NON-NLS-1$
                                         "name uniqueness.  Unable to make connector binding name " + //$NON-NLS-1$
                                         "change.", ex); //$NON-NLS-1$
            cancelPressed();
            continuing = false;
        }
        if (continuing) {
            if (alreadyExists) {
                StaticUtilities.displayModalDialogWithOK("Connector Binding Already Exists", //$NON-NLS-1$
                                                         "Connector Binding " + newName + " already exists.  " + //$NON-NLS-1$ //$NON-NLS-2$
                                                                         "Must enter a unique name."); //$NON-NLS-1$
            } else {
                this.dispose();
            }
        }
    }

    private void cancelPressed() {
        cancelled = true;
        this.dispose();
    }

    private void nameTextChanged() {
        int textLen = nameField.getText().trim().length();
        okButton.setEnabled((textLen > 0));
    }

    public String getNewName() {
        String newName = null;
        if (!cancelled) {
            newName = nameField.getText().trim();
        }
        return newName;
    }
}
