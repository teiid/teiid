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

package com.metamatrix.console.ui.views.entitlements;

import java.awt.*;

import javax.swing.*;

import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.toolbox.ui.widget.*;
import com.metamatrix.toolbox.ui.widget.text.DefaultTextFieldModel;

public class NewEntitlementConfirmationPanel extends BasicWizardSubpanelContainer {
    public final static String NONE = "(none)";

    private TextFieldWidget nameValue;
    private JTextArea descValue;
    private TextFieldWidget vdbNameValue;
    private TextFieldWidget dataNodesValue;
    private TextFieldWidget principalsValue;
    public final static int MAX_DESCRIPTION_LENGTH = 250;
    private JScrollPane scpnDescription = new JScrollPane();
    
    public NewEntitlementConfirmationPanel(WizardInterface wizardInterface) {
        super(wizardInterface);
        JPanel panel = init();
        this.setMainContent(panel);
        this.setStepText(4, "Check and confirm the assigned values.");
    }

    public void setPrincipalsTexts(String principalsEnt, String principalsVDB, int principalsVDBVersion){
         if ((principalsEnt == null) || (principalsEnt.length() == 0) ||
                principalsEnt.equals(NONE)) {
            principalsValue.setText(NONE);
         }else{
            principalsValue.setText(principalsEnt + ",  VDB: " + principalsVDB +
                    "  Vers. " + principalsVDBVersion);
         }
    }

    public void clear(){
    
    }

     public void setTexts(String entitlementName, String description,
            String vdbName, String vdbVersion, String dataNodesEnt,
            String dataNodesVDB, int dataNodesVDBVersion
            ) {
        nameValue.setText(entitlementName);
        descValue.setText(description);
        vdbNameValue.setText(vdbName + "      Vers. " + vdbVersion);
        if ((dataNodesEnt == null) || (dataNodesEnt.length() == 0) ||
                dataNodesEnt.equals(NONE)) {
            dataNodesValue.setText(NONE);
        } else {
            dataNodesValue.setText(dataNodesEnt + ",  VDB: " + dataNodesVDB +
                    "  Vers. " + dataNodesVDBVersion);
        }
    }

    private JPanel init() {
        JPanel panel = new JPanel();
        GridBagLayout layout = new GridBagLayout();
        panel.setLayout(layout);
        LabelWidget pressFinishLabel = new LabelWidget(
            "Press 'Finish' to create role:");
        pressFinishLabel.setFont(pressFinishLabel.getFont().deriveFont(Font.BOLD));
        panel.add(pressFinishLabel);
        layout.setConstraints(pressFinishLabel, new GridBagConstraints(0, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
                new Insets(10, 5, 20, 10), 0, 0));
        JPanel infoPanel = new JPanel();
        infoPanel.setBorder(new TitledBorder(""));
        GridBagLayout il = new GridBagLayout();
        infoPanel.setLayout(il);
        LabelWidget nameLabel = new LabelWidget("Role name:");
        infoPanel.add(nameLabel);
        il.setConstraints(nameLabel, new GridBagConstraints(0, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        LabelWidget descLabel = new LabelWidget("Role description:");
        infoPanel.add(descLabel);
        il.setConstraints(descLabel, new GridBagConstraints(0, 1, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        LabelWidget vdbNameLabel = new LabelWidget("VDB name:");
        infoPanel.add(vdbNameLabel);
        il.setConstraints(vdbNameLabel, new GridBagConstraints(0, 2, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        LabelWidget dataNodesLabel = new LabelWidget("Authorizations set from role:");
        infoPanel.add(dataNodesLabel);
        il.setConstraints(dataNodesLabel, new GridBagConstraints(0, 4, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        LabelWidget principalsLabel = new LabelWidget("Groups set from role:");
        infoPanel.add(principalsLabel);
        il.setConstraints(principalsLabel, new GridBagConstraints(0, 5, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        nameValue = new TextFieldWidget();
        nameValue.setEditable(false);
        infoPanel.add(nameValue);
        il.setConstraints(nameValue, new GridBagConstraints(1, 0, 1, 1,
                0.1, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 5), 0, 0));
        DefaultTextFieldModel document = new DefaultTextFieldModel();
        document.setMaximumLength(MAX_DESCRIPTION_LENGTH);
        descValue = new JTextArea(document);
        descValue.setColumns(30);
        descValue.setRows(6);
        descValue.setPreferredSize(new Dimension(150, 68));
        descValue.setLineWrap(true);
        descValue.setWrapStyleWord(true);

        descValue.setText("");
        scpnDescription.setViewportView(descValue);
        descValue.setEditable(false);
        infoPanel.add(descValue);
        il.setConstraints(descValue, new GridBagConstraints(1, 1, 1, 1,
                0.9, 1.0, GridBagConstraints.WEST, GridBagConstraints.BOTH,
                new Insets(5, 5, 5, 5), 0, 0));
        vdbNameValue = new TextFieldWidget();
        vdbNameValue.setEditable(false);
        infoPanel.add(vdbNameValue);
        il.setConstraints(vdbNameValue, new GridBagConstraints(1, 2, 1, 1,
                0.1, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 5), 0, 0));
        dataNodesValue = new TextFieldWidget();
        dataNodesValue.setEditable(false);
        infoPanel.add(dataNodesValue);
        il.setConstraints(dataNodesValue, new GridBagConstraints(1, 4, 1, 1,
                0.1, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 5), 0, 0));
        principalsValue = new TextFieldWidget();
        principalsValue.setEditable(false);
        infoPanel.add(principalsValue);
        il.setConstraints(principalsValue, new GridBagConstraints(1, 5, 1, 1,
                0.1, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 5), 0, 0));
        panel.add(infoPanel);
        layout.setConstraints(infoPanel, new GridBagConstraints(0, 1, 1, 1,
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(20, 10, 20, 10), 0, 0));
        return panel;
    }
}
