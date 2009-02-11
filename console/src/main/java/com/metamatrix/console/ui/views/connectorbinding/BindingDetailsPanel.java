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

package com.metamatrix.console.ui.views.connectorbinding;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.text.SimpleDateFormat;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.layout.BasePanel;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;

/**
 *
 */
public class BindingDetailsPanel extends BasePanel {

    // private BindingDataInterface dataSource;
    private TextFieldWidget createdField;
    private TextFieldWidget createdByField;
    private TextFieldWidget modifiedField;
    private TextFieldWidget modifiedByField;
    private JTextArea descriptionField;
    private ServiceComponentDefn scdConnectorBinding;
    private ConnectionInfo connection;

    public BindingDetailsPanel(ConnectionInfo connection) {
        super();
        this.connection = connection;
        init();
    }

    private void init() {
        GridBagLayout layout = new GridBagLayout();
        setLayout(layout);
        createdField = new TextFieldWidget();
        createdField.setEditable(false);
        createdByField = new TextFieldWidget();
        createdByField.setEditable(false);
        modifiedField = new TextFieldWidget();
        modifiedField.setEditable(false);
        modifiedByField = new TextFieldWidget();
        modifiedByField.setEditable(false);
        descriptionField = new JTextArea();
        descriptionField.setEditable(false);
        descriptionField.setWrapStyleWord(true);
        LabelWidget createdLabel = new LabelWidget("Created:"); //$NON-NLS-1$
        LabelWidget createdByLabel = new LabelWidget("By:"); //$NON-NLS-1$
        LabelWidget modifiedLabel = new LabelWidget("Modified:"); //$NON-NLS-1$
        LabelWidget modifiedByLabel = new LabelWidget("By:"); //$NON-NLS-1$
        LabelWidget descriptionLabel = new LabelWidget("Description:"); //$NON-NLS-1$
        add(createdField);
        add(createdByField);
        add(modifiedField);
        add(modifiedByField);
        add(createdLabel);
        add(createdByLabel);
        add(modifiedLabel);
        add(modifiedByLabel);
        JPanel descPanel = new JPanel();
        // Description is being removed. Is apparently not available. BWP 08/27/01
        // add(descPanel);
        GridBagLayout dl = new GridBagLayout();
        descPanel.setLayout(dl);
        JScrollPane descFieldSP = new JScrollPane(descriptionField);
        descPanel.add(descriptionLabel);
        descPanel.add(descFieldSP);
        layout.setConstraints(createdLabel, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.EAST,
                                                                   GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
        layout.setConstraints(createdField, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER,
                                                                   GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 10), 0, 0));
        layout.setConstraints(createdByLabel, new GridBagConstraints(2, 0, 1, 1, 0.0, 0.0, GridBagConstraints.EAST,
                                                                     GridBagConstraints.NONE, new Insets(5, 10, 5, 5), 0, 0));
        layout
              .setConstraints(createdByField, new GridBagConstraints(3, 0, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER,
                                                                     GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
        layout.setConstraints(modifiedLabel, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.EAST,
                                                                    GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
        layout
              .setConstraints(modifiedField, new GridBagConstraints(1, 1, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER,
                                                                    GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 10), 0, 0));
        layout.setConstraints(modifiedByLabel, new GridBagConstraints(2, 1, 1, 1, 0.0, 0.0, GridBagConstraints.EAST,
                                                                      GridBagConstraints.NONE, new Insets(5, 10, 5, 5), 0, 0));
        layout.setConstraints(modifiedByField,
                              new GridBagConstraints(3, 1, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER,
                                                     GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
        layout.setConstraints(descPanel, new GridBagConstraints(0, 2, 4, 1, 1.0, 1.0, GridBagConstraints.CENTER,
                                                                GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
        // Description being removed, is apparently not available. BWP 08/27/01
        // dl.setConstraints(descriptionLabel, new GridBagConstraints(0, 0, 1, 1,
        // 0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
        // new Insets(5, 5, 5, 5), 0, 0));
        // dl.setConstraints(descFieldSP, new GridBagConstraints(1, 0, 1, 1,
        // 1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
        // new Insets(15, 15, 15, 15), 0, 0));
    }

    public void populate() {
        SimpleDateFormat formatter = StaticUtilities.getDefaultDateFormat();
        if (scdConnectorBinding == null) {
            createdField.setText(""); //$NON-NLS-1$
            createdByField.setText(""); //$NON-NLS-1$
            modifiedField.setText(""); //$NON-NLS-1$
            modifiedByField.setText(""); //$NON-NLS-1$
            descriptionField.setText(""); //$NON-NLS-1$
        } else {
            // BindingDetails details = null;
            // boolean continuing = true;
            // try {
            // details = connectorManager.getBindingDetails(scdConnectorBinding);
            // } catch (Exception ex) {
            // ExceptionUtility.showMessage("Retrieve details for connector binding", ex);
            // LogManager.logError(LogContexts.CONNECTOR_BINDINGS, ex,
            // "Error retrieving details of connector binding.");
            // continuing = false;
            // }
            //
            // if ( details == null ) {
            // continuing = false;
            // }

            // if (continuing) {
            createdField.setText(formatter.format(scdConnectorBinding.getCreatedDate()));
            // details.getCreated()));
            createdByField.setText(scdConnectorBinding.getCreatedBy());
            // details.getCreatedBy());
            modifiedField.setText(formatter.format(scdConnectorBinding.getLastChangedDate()));
            // details.getModified()));
            modifiedByField.setText(scdConnectorBinding.getLastChangedBy());

            // details.getModifiedBy());
            // Removing descriptionField as description is apparently unavailable. BWP 08/27/01
            descriptionField.setText(scdConnectorBinding.getDescription());
            // }
        }
    }

    public void setConnectorBinding(ServiceComponentDefn scdConnectorBinding) {
        this.scdConnectorBinding = scdConnectorBinding;
        populate();
    }

    public ServiceComponentDefn getConnectorBinding() {
        return scdConnectorBinding;
    }
    
    public ConnectionInfo getConnectionInfo() {
        return connection;
    }
}
