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

import java.awt.Font;
import java.awt.Graphics;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import javax.swing.AbstractButton;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JComboBox;
import javax.swing.JPanel;
import javax.swing.SwingUtilities;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConnectorManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.ui.util.property.GuiComponentFactory;
import com.metamatrix.console.ui.util.property.TypeConstants;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;

public class NewBindingWizardConnectorSelectPanel extends BasicWizardSubpanelContainer implements
                                                                                      TypeConstants {

    private LabelWidget lblConnectorName = new LabelWidget("*Connector Type:"); //$NON-NLS-1$
    private TextFieldWidget txfBindingName = GuiComponentFactory.createTextField(CONNECTOR_BINDING_NAME);
    private LabelWidget lblBindingName = new LabelWidget("*Binding Name:"); //$NON-NLS-1$
    private JPanel pnlOuter = new JPanel();

    private HashMap hmNameConnectorXref = null;
    private ArrayList arylConnectors = null;

    private JComboBox cbxConnectorSelection;
    private DefaultComboBoxModel cbxmdlConnectorSelectionModel;
    private boolean hasBeenPainted = false;
    private ConnectionInfo connection = null;

    public NewBindingWizardConnectorSelectPanel(WizardInterface wizardInterface,
                                                ConnectionInfo connection) {
        super(wizardInterface);
        this.connection = connection;
        init();
    }

    private ConnectorManager getConnectorManager() {
        return ModelManager.getConnectorManager(connection);
    }

    private void init() {
        setBoldFont(lblBindingName);
        setBoldFont(lblConnectorName);
        LabelWidget requiredFieldLabel = new LabelWidget("*Required field"); //$NON-NLS-1$
        setBoldFont(requiredFieldLabel);
        GridBagLayout layout = new GridBagLayout();
        pnlOuter.setLayout(layout);
        pnlOuter.add(lblBindingName);
        pnlOuter.add(txfBindingName);
        pnlOuter.add(lblConnectorName);
        JComboBox connectorComboBox = getComboBox();
        pnlOuter.add(connectorComboBox);
        pnlOuter.add(requiredFieldLabel);
        layout.setConstraints(lblBindingName, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.EAST,
                                                                     GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
        layout
              .setConstraints(txfBindingName, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.WEST,
                                                                     GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
        layout.setConstraints(lblConnectorName, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.EAST,
                                                                       GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
        layout.setConstraints(connectorComboBox, new GridBagConstraints(1, 1, 1, 1, 1.0, 0.0, GridBagConstraints.WEST,
                                                                        GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0,
                                                                        0));
        layout.setConstraints(requiredFieldLabel, new GridBagConstraints(0, 2, 1, 1, 0.0, 1.0, GridBagConstraints.NORTHEAST,
                                                                         GridBagConstraints.NONE, new Insets(15, 5, 5, 5), 0, 0));

        setMainContent(pnlOuter);
        setStepText(1, "Specify a Name and Select a Connector Type for this Connector Binding."); //$NON-NLS-1$

        txfBindingName.getDocument().addDocumentListener(new DocumentListener() {

            public void changedUpdate(DocumentEvent ev) {
            }

            public void insertUpdate(DocumentEvent ev) {
                try {
                    String text = ev.getDocument().getText(0, ev.getDocument().getLength());
                    text = text.trim();
                    AbstractButton forwardButton = getWizardInterface().getForwardButton();
                    forwardButton.setEnabled((text.length() > 0));
                } catch (Exception ex) {
                }
            }

            public void removeUpdate(DocumentEvent ev) {
                try {
                    String text = ev.getDocument().getText(0, ev.getDocument().getLength());
                    text = text.trim();
                    AbstractButton forwardButton = getWizardInterface().getForwardButton();
                    forwardButton.setEnabled((text.length() > 0));
                } catch (Exception ex) {
                }
            }
        });
    }

    private JComboBox getComboBox() {
        if (cbxConnectorSelection == null) {
            cbxConnectorSelection = new JComboBox();
            cbxmdlConnectorSelectionModel = new DefaultComboBoxModel();
            cbxConnectorSelection.setModel(cbxmdlConnectorSelectionModel);
            hmNameConnectorXref = null;
            arylConnectors = getConnectorManager().getConnectors();
            Iterator itConnectors = arylConnectors.iterator();
            while (itConnectors.hasNext()) {
                ComponentType ctConnector = (ComponentType)itConnectors.next();
                if (ctConnector.isDeployable()) {
                    getConnectorXref().put(ctConnector.getName(), ctConnector);
                    cbxmdlConnectorSelectionModel.addElement(ctConnector.getName());
                }
            }
        }
        return cbxConnectorSelection;
    }

    public String getNewBindingName() {
        return txfBindingName.getText();
    }

    public ComponentType getSelectedConnector() {
        DefaultComboBoxModel mdl = (DefaultComboBoxModel)cbxConnectorSelection.getModel();
        String sSelectedConnector = (String)mdl.getSelectedItem();
        ComponentType ctConnector = (ComponentType)getConnectorXref().get(sSelectedConnector);
        return ctConnector;
    }

    private HashMap getConnectorXref() {
        if (hmNameConnectorXref == null) {
            hmNameConnectorXref = new HashMap();
        }
        return hmNameConnectorXref;
    }

    private void setBoldFont(LabelWidget label) {
        Font tempFont = label.getFont();
        Font newFont = new Font(tempFont.getName(), Font.BOLD, tempFont.getSize());
        label.setFont(newFont);
    }

    public void paint(Graphics g) {
        if (!hasBeenPainted) {
            AbstractButton forwardButton = getWizardInterface().getForwardButton();
            forwardButton.setEnabled(false);
            SwingUtilities.invokeLater(new Runnable() {

                public void run() {
                    txfBindingName.requestFocus();
                }
            });
            hasBeenPainted = true;
        }
        super.paint(g);
    }
}
