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

package com.metamatrix.console.ui.views.authorization;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.text.SimpleDateFormat;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.console.ui.layout.BasePanel;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;

/**
 * Panel for display of AuthenticationProvider Details
 */
public class AuthenticationProviderDetailsPanel extends BasePanel {

    private TextFieldWidget createdField;
    private TextFieldWidget createdByField;
    private TextFieldWidget modifiedField;
    private TextFieldWidget modifiedByField;
    private JTextArea descriptionField;
    private ComponentDefn providerDefn;

    /**
     * Constructor
     */
    public AuthenticationProviderDetailsPanel( ) {
        super();
        init();
    }

    /**
     * Initialization
     */
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
    }

    /**
     * Populate the panel
     */
    public void populate() {
        SimpleDateFormat formatter = StaticUtilities.getDefaultDateFormat();
        if (providerDefn == null) {
            createdField.setText(""); //$NON-NLS-1$
            createdByField.setText(""); //$NON-NLS-1$
            modifiedField.setText(""); //$NON-NLS-1$
            modifiedByField.setText(""); //$NON-NLS-1$
            descriptionField.setText(""); //$NON-NLS-1$
        } else {
            createdField.setText(formatter.format(providerDefn.getCreatedDate()));
            createdByField.setText(providerDefn.getCreatedBy());
            modifiedField.setText(formatter.format(providerDefn.getLastChangedDate()));
            modifiedByField.setText(providerDefn.getLastChangedBy());

            descriptionField.setText(providerDefn.getDescription());
        }
    }

    /**
     * Set the displayed provider
     * @param defn the provider ComponentDefn to display
     */
    public void setProvider(ComponentDefn defn) {
        this.providerDefn = defn;
        populate();
    }

    /**
     * Get the currently displayed provider ComponentDefn
     * @return the current provider
     */
    public ComponentDefn getProvider() {
		return providerDefn;
	}
    
}
