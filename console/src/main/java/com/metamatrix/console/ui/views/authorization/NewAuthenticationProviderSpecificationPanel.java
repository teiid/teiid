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

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ComponentEvent;
import java.awt.event.ComponentListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.swing.AbstractButton;
import javax.swing.JPanel;

import com.metamatrix.common.actions.ModificationActionQueue;
import com.metamatrix.common.config.api.AuthenticationProvider;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.AuthenticationProviderManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.ExpertPropertiedObjectPanelHolder;
import com.metamatrix.console.ui.util.InitialAndCurrentValues;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.property.PropertiedObjectPanel;

/**
 * New AuthenticationProvider Specification Panel
 */
public class NewAuthenticationProviderSpecificationPanel extends BasicWizardSubpanelContainer
  		implements PropertyChangeListener, ComponentListener {

	private PropertiedObjectPanel popThePanel;

    private ComponentType ctProviderType = null;
    private String sProviderName = "Unknown"; //$NON-NLS-1$

    private TextFieldWidget txfProviderTypeName = new TextFieldWidget();
    private LabelWidget lblProviderTypeName = new LabelWidget();
    private TextFieldWidget txfProviderName = new TextFieldWidget();
    private LabelWidget lblProviderName = new LabelWidget();
    private JPanel pnlOuter = new JPanel();
    private JPanel pnlPOPShell = new JPanel(new GridLayout(1, 1));
    private ConfigurationObjectEditor coeEditor;
    private ModificationActionQueue maqActionQForProvider;
    private AuthenticationProvider scdNewAuthenticationProvider;
    private PropertiedObject poPropObject;
    private PropertiedObjectEditor poeEditor;
    private ExpertPropertiedObjectPanelHolder popHolder;
    
    private Map /* <PropertyDefinition> to <InitialAndCurrentValues> */
    valuesMap = new HashMap();
    private Map /* <String> name to <PropertyDefinition> */
    defsMap = new HashMap();

    private ConnectionInfo connection = null;

    /**
     * Constructor
     * @param wizardInterface the wizard interface
     * @param connection the ConnectionInfo
     */
    public NewAuthenticationProviderSpecificationPanel(WizardInterface wizardInterface,
                                                        ConnectionInfo connection) {
        super(wizardInterface);
        this.connection = connection;
        init();
    }

    private AuthenticationProviderManager getAuthenticationProviderManager() {
        return ModelManager.getAuthenticationProviderManager(connection);
    }

    private void init() {
        pnlPOPShell.setPreferredSize(new Dimension(200, 300));

        pnlOuter.setLayout(new GridBagLayout());

        lblProviderName.setText("Provider Name:"); //$NON-NLS-1$
        lblProviderTypeName.setText("Provider Type:"); //$NON-NLS-1$

        txfProviderName.setEditable(false);
        txfProviderTypeName.setEditable(false);

        pnlOuter.add(lblProviderName, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.EAST,
                                                            GridBagConstraints.NONE, new Insets(5, 0, 5, 0), 5, 4));
        pnlOuter.add(txfProviderName, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.WEST,
                                                            GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 1), 1, 0));

        pnlOuter.add(lblProviderTypeName, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.EAST,
                                                              GridBagConstraints.NONE, new Insets(5, 0, 5, 0), 5, 4));
        pnlOuter.add(txfProviderTypeName, new GridBagConstraints(1, 1, 1, 1, 1.0, 0.0, GridBagConstraints.WEST,
                                                              GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 1), 1, 0));

        pnlOuter.add(pnlPOPShell, new GridBagConstraints(0, 2, 2, 3, 1.0, 1.0, GridBagConstraints.CENTER,
                                                         GridBagConstraints.BOTH, new Insets(8, 0, 0, 1), 1, 0));

        setMainContent(pnlOuter);
        setStepText(2, "Specify Details for this Membership Domain Provider."); //$NON-NLS-1$
        setupListening();

    }

    public ConfigurationObjectEditor getConfigurationObjectEditor() {
        return coeEditor;
    }

    private void updateTextFields() {
        txfProviderTypeName.setText(ctProviderType.getName());
        txfProviderName.setText(sProviderName);
    }

    /**
     * Update the panel based on the specified Connector Type and Connector Binding Name.
     * 
     * @param ctConnector
     *            Connector Type to set.
     * @param sConnectorBindingName
     *            Connector Binding name to set.
     * @param initBinding
     *            If true, create a new ConnectorBinding with default properties. If false, copy the existing ConnectorBinding and
     *            its properties.
     */
    public void updatePropertiedObjectPanel(ComponentType cType,
                                            String sProviderName,
                                            boolean initProvider) {
        this.ctProviderType = cType;
        this.sProviderName = sProviderName;
        updateTextFields();
        boolean showingWaitCursor;
        try {
            showingWaitCursor = true;
            StaticUtilities.startWait(ConsoleMainFrame.getInstance());

            // 1. get ConfigurationObjectEditor to create the Binding
            coeEditor = getAuthenticationProviderManager().getAuthenticationProviderEditor();

            // 2. create/copy the provider
            if (initProvider) {
            	scdNewAuthenticationProvider = getAuthenticationProviderManager().
                                               getTentativeAuthenticationProvider(ctProviderType,coeEditor,sProviderName);
            } else {
            	scdNewAuthenticationProvider = getAuthenticationProviderManager().
                                               copyAuthenticationProvider(scdNewAuthenticationProvider,coeEditor,sProviderName);
            }

            // 3. Save the 'action' from this create of the provider
            maqActionQForProvider = coeEditor.getDestination();

            // 3.1 Force all property values into editor
            // ConfigurationAdminAPI cAPI = ModelManager.getConfigurationAPI(
            // connectorManager.getConnection());
            // putDefaultValuesInBinding(scdNewConnectorBinding, coeEditor, cAPI);

            // 3.2 create the PropertiedObject based on the binding
            poPropObject = getAuthenticationProviderManager().getPropertiedObject(scdNewAuthenticationProvider);

            // 4. create a PropertiedObjectEditor which contains the
            // initial 'create' action
            poeEditor = getAuthenticationProviderManager().getPropertiedObjectEditor(maqActionQForProvider);

            // 5. Create the PropertiedObjectPanel
            popThePanel = new PropertiedObjectPanel(poeEditor, getAuthenticationProviderManager().getEncryptor());
            poeEditor.setReadOnly(poPropObject, false);

            // 6. Hand the PropertiedObject to the POP panel
            popThePanel.setNameColumnHeaderWidth(0);
            popThePanel.setPropertiedObject(poPropObject);

            popThePanel.setShowRequiredProperties(true);
            popThePanel.setShowInvalidProperties(true);
            popThePanel.setShowHiddenProperties(false);
            popThePanel.setShowExpertProperties(false);

            popThePanel.refreshDisplay();
            popThePanel.createComponent();

            // 7. Place the POP in its panel
            pnlPOPShell.removeAll();
            ItemListener includeExpertListener = new ItemListener() {
                public void itemStateChanged(ItemEvent ev) {
                    includeExpertStateChanged();
                }
            };
            
            popHolder = new ExpertPropertiedObjectPanelHolder(popThePanel, includeExpertListener);
            popHolder.setIsIncludingExpertProperties(false);
            pnlPOPShell.add(popHolder);

            setInitialValues();
            popThePanel.addPropertyChangeListener(this);
            setButtons();
        } catch (Exception e) {
            StaticUtilities.endWait(ConsoleMainFrame.getInstance());
            showingWaitCursor = false;
            ExceptionUtility.showMessage("Failed while creating Membership Domain Provider Panel", e); //$NON-NLS-1$
        }
        if (showingWaitCursor) {
            StaticUtilities.endWait(ConsoleMainFrame.getInstance());
        }
    }

    private void includeExpertStateChanged() {
    	popThePanel.setShowExpertProperties(popHolder.isIncludingExpertProperties());
    	popThePanel.refreshDisplay();
    }
    
    public AuthenticationProvider getNewAuthenticationProvider() {
        return scdNewAuthenticationProvider;
    }

    public void propertyChange(PropertyChangeEvent evt) {
        setButtons();
    }
    
    public String getProviderName() {
    	return this.txfProviderName.getText().trim();
    }

    public void setButtons() {
        AbstractButton forwardButton = getWizardInterface().getForwardButton();
        forwardButton.setEnabled((!anyValueInvalid()));
        repaint();
    }

    private boolean anyValueInvalid() {
        int iInvalidCount = popThePanel.getInvalidDefinitions().size();
        // reportInvalidDefinitions();
        boolean anyInvalid = (iInvalidCount > 0);
        return anyInvalid;
    }

    private void setInitialValues() {
        PropertiedObject propObj = popThePanel.getPropertiedObject();
        java.util.List defs = poeEditor.getPropertyDefinitions(propObj);
        Iterator it = defs.iterator();
        while (it.hasNext()) {
            PropertyDefinition def = (PropertyDefinition)it.next();
            defsMap.put(def.getName(), def);
            Object value = poeEditor.getValue(propObj, def);
            InitialAndCurrentValues vals = new InitialAndCurrentValues(value, value);
            valuesMap.put(def, vals);
        }
    }

    public void setupListening() {
        addComponentListener(this);
    }

    // methods required by ComponentListener Interface

    public void componentMoved(ComponentEvent e) {
        // setInitialPostRealizeState();
    }

    public void componentResized(ComponentEvent e) {
        // This is John V's recommendation for disabling the Next button
        // on the first appearance of the first page:
        setInitialPostRealizeState();
        removeComponentListener(this);
    }

    public void componentShown(ComponentEvent e) {
        //setInitialPostRealizeState();
    }

    public void componentHidden(ComponentEvent e) {
        //setInitialPostRealizeState();
    }

    public void setInitialPostRealizeState() {
        setButtons();
    }
    
}
