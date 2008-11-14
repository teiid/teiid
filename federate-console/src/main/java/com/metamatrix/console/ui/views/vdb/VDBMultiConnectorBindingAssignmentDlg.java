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

import java.awt.Dimension;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JPanel;

import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.views.connectorbinding.NewBindingWizardController;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.console.util.StringComparator;
import com.metamatrix.toolbox.ui.widget.AccumulatorPanel;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;

/**
 * Dialog showing accumulator panel to assign or deassign multiple connector bindings to a model.
 * 
 * @since 4.2
 */
public class VDBMultiConnectorBindingAssignmentDlg extends JDialog {

    private final static String TITLE = ConsolePlugin.Util.getString("VDBMultiConnectorBindingAssignmentDlg.title"); //$NON-NLS-1$
    private final static String OK = ConsolePlugin.Util.getString("General.OK"); //$NON-NLS-1$
    private final static String CANCEL = ConsolePlugin.Util.getString("General.Cancel"); //$NON-NLS-1$
    private final static String NEW = ConsolePlugin.Util.getString("General.New..."); //$NON-NLS-1$                                                                   

    private ConnectionInfo connection = null;
    private Map /* <String binding name to String UUID> */bindingNameToUUIDMap;

    private boolean canceled = false;
    private ConnectorBindingNameAndUUID[] selectedBindings = new ConnectorBindingNameAndUUID[0];
    private AccumulatorPanel accumulator;

    public VDBMultiConnectorBindingAssignmentDlg(Frame parent,
                                                 String modelName,
                                                 List /* <String> */availableBindings,
                                                 List /* <String> */assignedBindings,
                                                 Map /* <String binding name to String UUID> */bindingNameToUUIDMap,
                                                 ConnectionInfo connection) {
        super(parent);
        this.bindingNameToUUIDMap = bindingNameToUUIDMap;
        this.connection = connection;
        this.setTitle(TITLE + ' ' + modelName);
        this.setModal(true);
        createComponent(availableBindings, assignedBindings);
        this.addWindowListener(new WindowAdapter() {

            public void windowClosing(WindowEvent ev) {
                cancelPressed();
            }
        });
    }

    private void createComponent(List /* <String> */availableBindings,
                                 List /* <String> */assignedBindings) {
        GridBagLayout layout = new GridBagLayout();
        this.getContentPane().setLayout(layout);

        JButton newButton = new ButtonWidget(NEW);
        newButton.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent ev) {
                newButtonPressed();
            }
        });
        JButton[] buttonArray = new JButton[] {
            newButton
        };

        accumulator = new AccumulatorPanel(availableBindings, assignedBindings, buttonArray, new StringComparator(true));
        accumulator.getAcceptButton().setVisible(false);
        accumulator.getResetButton().setVisible(false);
        accumulator.getCancelButton().setVisible(false);
        this.getContentPane().add(accumulator);
        layout.setConstraints(accumulator, new GridBagConstraints(0, 1, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER,
                                                                  GridBagConstraints.BOTH, new Insets(2, 2, 2, 2), 0, 0));

        JPanel buttonsPanel = new JPanel();
        GridLayout buttonsPanelLayout = new GridLayout(1, 2, 10, 0);
        buttonsPanel.setLayout(buttonsPanelLayout);
        JButton okButton = new ButtonWidget(OK);
        okButton.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent ev) {
                okPressed();
            }
        });
        JButton cancelButton = new ButtonWidget(CANCEL);
        cancelButton.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent ev) {
                cancelPressed();
            }
        });
        buttonsPanel.add(okButton);
        buttonsPanel.add(cancelButton);
        this.getContentPane().add(buttonsPanel);
        layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                                                                   GridBagConstraints.NONE, new Insets(8, 8, 4, 8), 0, 0));

        this.pack();
        Dimension oldSize = this.getSize();
        Dimension newSize = new Dimension(oldSize.width, (int)(Math.min(oldSize.height, Toolkit.getDefaultToolkit()
                                                                                               .getScreenSize().height * 0.8)));
        this.setSize(newSize);
        this.setLocation(StaticUtilities.centerFrame(this.getSize()));
    }

    private void cancelPressed() {
        canceled = true;
        this.dispose();
    }

    private void okPressed() {
        setSelectedBindings();
        this.dispose();
    }

    private void newButtonPressed() {
        NewBindingWizardController controller = new NewBindingWizardController(connection);
        ConnectorBinding connectorBinding = (ConnectorBinding)controller.runWizard();
        if (connectorBinding != null) {
            accumulator.addAvailableValue(connectorBinding.getName());
            bindingNameToUUIDMap.put(connectorBinding.getName(), connectorBinding.getRoutingUUID());
        }
    }

    private void setSelectedBindings() {
        List /* <String> */values = accumulator.getValues();

        selectedBindings = new ConnectorBindingNameAndUUID[values.size()];
        Iterator it = values.iterator();
        for (int i = 0; it.hasNext(); i++) {
            String bindingName = (String)it.next();
            String bindingUUID = (String)bindingNameToUUIDMap.get(bindingName);
            selectedBindings[i] = new ConnectorBindingNameAndUUID(bindingName, bindingUUID);
        }
    }

    public ConnectorBindingNameAndUUID[] getSelectedBindings() {
        return selectedBindings;
    }

    public boolean wasCanceled() {
        return canceled;
    }
}
