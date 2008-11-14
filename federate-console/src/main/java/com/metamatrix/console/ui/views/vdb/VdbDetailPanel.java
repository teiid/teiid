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

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.text.SimpleDateFormat;

import javax.swing.*;
import javax.swing.border.Border;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.models.VdbManager;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.toolbox.ui.widget.*;
import com.metamatrix.toolbox.ui.widget.text.DefaultTextFieldModel;

public class VdbDetailPanel extends JPanel implements
                                          VdbDisplayer {

    Border border1;
    GridBagLayout gridBagLayout1 = new GridBagLayout();
    DefaultTextFieldModel dtfmTextModel = new DefaultTextFieldModel();
    JTextArea txaDescription = new JTextArea(dtfmTextModel);

    JLabel lblDescription = new LabelWidget();
    JTextField txfVersionedBy = new TextFieldWidget();
    JLabel lblVersionedBy = new LabelWidget();
    JTextField txfVersioned = new TextFieldWidget();
    JLabel lblVersioned = new LabelWidget();
    JTextField txfCreatedBy = new TextFieldWidget();
    JLabel lblCreatedBy = new LabelWidget();
    JTextField txfCreated = new TextFieldWidget();
    JLabel lblCreated = new LabelWidget();
    JTextField txfUpdatedBy = new TextFieldWidget();
    JLabel lblUpdatedBy = new LabelWidget();
    JTextField txfUpdated = new TextFieldWidget();
    JLabel lblUpdated = new LabelWidget();
    JTextField txfFileName = new TextFieldWidget();
    JLabel lblFileName = new LabelWidget();
    JTextField txfVersion = new TextFieldWidget();
    JLabel lblVersion = new LabelWidget();
    JTextField txfName = new TextFieldWidget();
    JLabel lblName = new LabelWidget();
    Border border2;
    JLabel lblStatus = new LabelWidget();
    Border border3;
    JScrollPane scpnDescription = new JScrollPane();
    ButtonWidget btnApply = new ButtonWidget("Apply ");
    // JButton btnApply = new JButton("Apply ");
    ButtonWidget btnReset = new ButtonWidget("Reset");
    private int TEXTAREA_MAXLENGTH = 255;

    private boolean bCanModify;
    VirtualDatabase vdbCurrent = null;
    private ConnectionInfo connection = null;

    public VdbDetailPanel(ConnectionInfo connection,
                          boolean canModify) {
        super();
        this.connection = connection;
        this.bCanModify = canModify;
        try {
            jbInit();
            addListeners();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private VdbManager getVdbManager() {
        return ModelManager.getVdbManager(connection);
    }

    public void setVirtualDatabase(VirtualDatabase vdb) {
        if (vdbCurrent != vdb) {
            vdbCurrent = vdb;
            refresh();
        }
    }

    public VirtualDatabase getVirtualDatabase() {
        return vdbCurrent;
    }

    public void refresh() {
        SimpleDateFormat formatter = StaticUtilities.getDefaultDateFormat();
        if (getVirtualDatabase() == null) {
            clear();
            return;
        }

        txfName.setText(getVirtualDatabase().getName());

        short siStatus = getVirtualDatabase().getStatus();
        String sStatus = getVdbManager().getVdbStatusAsString(siStatus);
        lblStatus.setText(sStatus);

        txfVersionedBy.setText(getVirtualDatabase().getVersionBy());
        txfVersioned.setText(formatter.format(getVirtualDatabase().getVersionDate()));
        txfCreatedBy.setText(getVirtualDatabase().getCreatedBy());
        txfCreated.setText(formatter.format(getVirtualDatabase().getCreationDate()));
        txfUpdatedBy.setText(getVirtualDatabase().getUpdatedBy());
        txfUpdated.setText(formatter.format(getVirtualDatabase().getUpdateDate()));

        txfFileName.setText(getVirtualDatabase().getFileName());

        VirtualDatabaseID vdbid = (VirtualDatabaseID)getVirtualDatabase().getID();

        txfVersion.setText(vdbid.getVersion());
        txaDescription.setText(getVirtualDatabase().getDescription());

        if (bCanModify) {
            txaDescription.setEnabled(true);
        } else {
            txaDescription.setEnabled(false);
        }
    }

    public void clear() {
        txfName.setText("");
        lblStatus.setText("");
        txfVersionedBy.setText("");
        txfVersioned.setText("");
        txfCreatedBy.setText("");
        txfCreated.setText("");
        txfUpdatedBy.setText("");
        txfUpdated.setText("");
        txfFileName.setText("");
        txaDescription.setText("");
        txaDescription.setEnabled(false);
        txfVersion.setText("");
    }

    private void addListeners() {
        btnApply.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent event) {
                processApplyButton();
            }
        });
        btnReset.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent event) {
                processResetButton();
            }
        });
        txaDescription.getDocument().addDocumentListener(new DocumentListener() {

            public void changedUpdate(DocumentEvent de) {
                enableButtons();
            }

            public void insertUpdate(DocumentEvent de) {
                enableButtons();
            }

            public void removeUpdate(DocumentEvent de) {
                enableButtons();
            }
        });
    }

    private void enableButtons() {
        // determine if a change has been made by comparing the current value
        // with the original value???
        // If a change has been made, enable the apply and reset buttons
        // If a change has not been made, disable both.

        if (bCanModify) {
            if ((getVirtualDatabase() != null)) {
                if (txaDescription.getText().equals(getVirtualDatabase().getDescription())) {
                    btnApply.setEnabled(false);
                    btnReset.setEnabled(false);
                } else {
                    btnApply.setEnabled(true);
                    btnReset.setEnabled(true);
                }
            } else {
                btnApply.setEnabled(false);
                btnReset.setEnabled(false);
            }
        } else {
            btnApply.setEnabled(false);
            btnReset.setEnabled(false);
        }
    }

    private void initButtonState() {
        btnApply.setEnabled(false);
        btnReset.setEnabled(false);
    }

    private void processApplyButton() {
        // place the contents of the VDB description into the VDB
        VirtualDatabase vdb = getVirtualDatabase();
        vdb.update(VirtualDatabase.ModifiableAttributes.DESCRIPTION, txaDescription.getText());

        // update the VDB
        try {
            getVdbManager().updateVirtualDatabase(vdb);
        } catch (Exception ex) {
            ExceptionUtility.showMessage("Failed trying to update a VDB", ex);
        }
        initButtonState();
    }

    private void processResetButton() {
        refresh();
    }

    private void jbInit() throws Exception {

        dtfmTextModel.setMaximumLength(TEXTAREA_MAXLENGTH);

        border2 = BorderFactory.createEmptyBorder(5, 11, 11, 11);
        txaDescription.setColumns(30);
        txaDescription.setRows(4);
        txaDescription.setPreferredSize(new Dimension(150, 68));
        txaDescription.setLineWrap(true);
        txaDescription.setWrapStyleWord(true);

        txaDescription.setText("");
        scpnDescription.setViewportView(txaDescription);

        this.setLayout(gridBagLayout1);
        lblDescription.setText("Description:");

        txfVersionedBy.setEditable(false);
        txfVersionedBy.setColumns(30);
        txfVersionedBy.setMinimumSize(new Dimension(180, 21));
        lblVersionedBy.setText("By: ");

        txfVersioned.setText("");
        txfVersioned.setColumns(30);
        txfVersioned.setMinimumSize(new Dimension(180, 21));
        txfVersioned.setEditable(false);
        lblVersioned.setText("Versioned: ");

        txfCreatedBy.setEditable(false);
        txfCreatedBy.setColumns(30);
        txfCreatedBy.setMinimumSize(new Dimension(180, 21));
        lblCreatedBy.setText("By: ");

        txfCreated.setText("");
        txfCreated.setColumns(30);
        txfCreated.setMinimumSize(new Dimension(180, 21));
        txfCreated.setEditable(false);
        lblCreated.setText("Created: ");

        txfUpdatedBy.setEditable(false);
        txfUpdatedBy.setColumns(30);
        txfUpdatedBy.setMinimumSize(new Dimension(180, 21));
        lblUpdatedBy.setText("By: ");

        txfUpdated.setText("");
        txfUpdated.setColumns(30);
        txfUpdated.setMinimumSize(new Dimension(180, 21));
        txfUpdated.setEditable(false);
        lblUpdated.setText("Updated: ");

        txfFileName.setText("");
        txfFileName.setColumns(50);
        txfFileName.setMinimumSize(new Dimension(300, 210));
        txfFileName.setEditable(false);
        lblFileName.setText("VDB FileName: ");

        txfVersion.setText("");
        txfVersion.setColumns(10);
        txfVersion.setPreferredSize(new Dimension(50, 21));
        txfVersion.setMinimumSize(new Dimension(50, 21));
        txfVersion.setEditable(false);
        lblVersion.setText("Version: ");

        txfName.setText("");
        txfName.setColumns(30);
        txfName.setMinimumSize(new Dimension(180, 21));
        txfName.setEditable(false);
        lblName.setText("VDB Name: ");

        this.setBorder(border2);
        lblStatus.setFont(new java.awt.Font("Dialog", 1, 14));
        lblStatus.setBorder(border3);
        lblStatus.setText("ACTIVE");

        this.add(lblName, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                                                 new Insets(2, 0, 2, 0), 0, 0));

        this.add(txfName, new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                                                 new Insets(2, 0, 2, 5), 0, 0));

        this.add(lblVersion, new GridBagConstraints(2, 0, 1, 1, 0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                                                    new Insets(2, 0, 2, 0), 0, 0));

        this.add(txfVersion, new GridBagConstraints(3, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                                                    new Insets(2, 0, 2, 0), 0, 0));

        this.add(lblStatus, new GridBagConstraints(4, 0, 1, 2, 0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                                                   new Insets(5, 5, 5, 5), 0, 0));

        this.add(lblVersioned, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                                                      new Insets(2, 0, 2, 0), 0, 0));

        this.add(txfVersioned, new GridBagConstraints(1, 1, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                                                      new Insets(2, 0, 2, 0), 0, 0));

        this.add(lblVersionedBy, new GridBagConstraints(2, 1, 1, 1, 0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                                                        new Insets(2, 0, 2, 0), 0, 0));

        this.add(txfVersionedBy, new GridBagConstraints(3, 1, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                                                        new Insets(2, 0, 2, 0), 0, 0));

        this.add(lblCreated, new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                                                    new Insets(2, 0, 2, 0), 0, 0));

        this.add(txfCreated, new GridBagConstraints(1, 2, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                                                    new Insets(2, 0, 2, 0), 0, 0));

        this.add(lblCreatedBy, new GridBagConstraints(2, 2, 1, 1, 0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                                                      new Insets(2, 0, 2, 0), 0, 0));

        this.add(txfCreatedBy, new GridBagConstraints(3, 2, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                                                      new Insets(2, 0, 2, 0), 0, 0));

        this.add(lblUpdated, new GridBagConstraints(0, 3, 1, 1, 0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                                                    new Insets(2, 0, 2, 0), 0, 0));

        this.add(txfUpdated, new GridBagConstraints(1, 3, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                                                    new Insets(2, 0, 2, 0), 0, 0));

        this.add(lblUpdatedBy, new GridBagConstraints(2, 3, 1, 1, 0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                                                      new Insets(2, 0, 2, 0), 0, 0));

        this.add(txfUpdatedBy, new GridBagConstraints(3, 3, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                                                      new Insets(2, 0, 2, 0), 0, 0));

        JPanel configPanel = new JPanel();
        GridBagLayout cl = new GridBagLayout();
        configPanel.setLayout(cl);
        configPanel.add(lblFileName);
        cl.setConstraints(lblFileName, new GridBagConstraints(0, 4, 1, 1, 0.0, 0.0, GridBagConstraints.EAST,
                                                              GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        configPanel.add(txfFileName);
        cl.setConstraints(txfFileName, new GridBagConstraints(1, 4, 1, 1, 0.0, 0.0, GridBagConstraints.WEST,
                                                              GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));

        this.add(configPanel, new GridBagConstraints(0, 4, 4, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                                                     new Insets(2, 0, 2, 0), 0, 0));

        this.add(lblDescription, new GridBagConstraints(0, 5, 1, 1, 0.0, 0.0, GridBagConstraints.NORTH, GridBagConstraints.NONE,
                                                        new Insets(0, 0, 0, 0), 0, 0));

        this.add(scpnDescription, new GridBagConstraints(1, 5, 3, 2, 1.0, 0.5, GridBagConstraints.NORTH, GridBagConstraints.BOTH,
                                                         new Insets(2, 0, 2, 0), 0, 0));

        this.add(btnApply, new GridBagConstraints(4, 5, 1, 1, 0.0, 0.0, GridBagConstraints.NORTHEAST, GridBagConstraints.NONE,
                                                  new Insets(5, 5, 5, 5), 0, 0));

        this.add(btnReset, new GridBagConstraints(4, 6, 1, 1, 0.0, 0.0, GridBagConstraints.NORTHEAST, GridBagConstraints.NONE,
                                                  new Insets(5, 5, 0, 5), 0, 0));

        refresh();
        initButtonState();
    }
}
