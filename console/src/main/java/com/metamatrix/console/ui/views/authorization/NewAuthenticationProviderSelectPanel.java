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

import java.awt.Font;
import java.awt.Graphics;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.swing.AbstractButton;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JComboBox;
import javax.swing.JPanel;
import javax.swing.SwingUtilities;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.AuthenticationProviderManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.ui.util.property.GuiComponentFactory;
import com.metamatrix.console.ui.util.property.TypeConstants;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;

/**
 * New AuthenticationProvider Selection Panel
 */
public class NewAuthenticationProviderSelectPanel extends BasicWizardSubpanelContainer implements
                                                                                      TypeConstants {

    private LabelWidget lblProviderTypeName = new LabelWidget("*Provider Type:"); //$NON-NLS-1$
    private TextFieldWidget txfProviderName = GuiComponentFactory.createTextField(CONNECTOR_BINDING_NAME);
    private LabelWidget lblProviderName = new LabelWidget("*Provider Name:"); //$NON-NLS-1$
    private JPanel pnlOuter = new JPanel();

    private HashMap hmNameProviderXref = null;
    private List arylProviders = null;

    private JComboBox cbxProviderSelection;
    private DefaultComboBoxModel cbxmdlProviderSelectionModel;
    private boolean hasBeenPainted = false;
    private ConnectionInfo connection = null;

    public NewAuthenticationProviderSelectPanel(WizardInterface wizardInterface,
                                                ConnectionInfo connection) {
        super(wizardInterface);
        this.connection = connection;
        init();
    }

    private AuthenticationProviderManager getAuthenticationProviderManager() {
        return ModelManager.getAuthenticationProviderManager(connection);
    }

    private void init() {
        setBoldFont(lblProviderTypeName);
        setBoldFont(lblProviderName);
        LabelWidget requiredFieldLabel = new LabelWidget("*Required field"); //$NON-NLS-1$
        setBoldFont(requiredFieldLabel);
        GridBagLayout layout = new GridBagLayout();
        pnlOuter.setLayout(layout);
        pnlOuter.add(lblProviderName);
        pnlOuter.add(txfProviderName);
        pnlOuter.add(lblProviderTypeName);
        JComboBox providerComboBox = getComboBox();
        pnlOuter.add(providerComboBox);
        pnlOuter.add(requiredFieldLabel);
        layout.setConstraints(lblProviderName, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.EAST,
                                                                     GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
        layout
              .setConstraints(txfProviderName, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.WEST,
                                                                     GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
        layout.setConstraints(lblProviderTypeName, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.EAST,
                                                                       GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
        layout.setConstraints(providerComboBox, new GridBagConstraints(1, 1, 1, 1, 1.0, 0.0, GridBagConstraints.WEST,
                                                                        GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0,
                                                                        0));
        layout.setConstraints(requiredFieldLabel, new GridBagConstraints(0, 2, 1, 1, 0.0, 1.0, GridBagConstraints.NORTHEAST,
                                                                         GridBagConstraints.NONE, new Insets(15, 5, 5, 5), 0, 0));

        setMainContent(pnlOuter);
        setStepText(1, "Specify a Name and Select a Type for this Membership Domain Provider."); //$NON-NLS-1$

        txfProviderName.getDocument().addDocumentListener(new DocumentListener() {

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
        if (cbxProviderSelection == null) {
        	cbxProviderSelection = new JComboBox();
        	cbxmdlProviderSelectionModel = new DefaultComboBoxModel();
            cbxProviderSelection.setModel(cbxmdlProviderSelectionModel);
            hmNameProviderXref = null;
            arylProviders = getAuthenticationProviderManager().getAllProviderTypes();
            Iterator itProviders = arylProviders.iterator();
            while (itProviders.hasNext()) {
                ComponentType ctProvider = (ComponentType)itProviders.next();
                getProviderXref().put(ctProvider.getName(), ctProvider);
                cbxmdlProviderSelectionModel.addElement(ctProvider.getName());
            }
        }
        return cbxProviderSelection;
    }

    public String getNewProviderName() {
        return txfProviderName.getText();
    }

    public ComponentType getSelectedAuthenticationProvider() {
        DefaultComboBoxModel mdl = (DefaultComboBoxModel)cbxProviderSelection.getModel();
        String sSelectedProvider = (String)mdl.getSelectedItem();
        ComponentType ctProvider = (ComponentType)getProviderXref().get(sSelectedProvider);
        return ctProvider;
    }

    private HashMap getProviderXref() {
        if (hmNameProviderXref == null) {
            hmNameProviderXref = new HashMap();
        }
        return hmNameProviderXref;
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
                	txfProviderName.requestFocus();
                }
            });
            hasBeenPainted = true;
        }
        super.paint(g);
    }
}
