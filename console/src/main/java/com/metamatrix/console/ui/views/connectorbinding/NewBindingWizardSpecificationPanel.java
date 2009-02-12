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
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConnectorManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.InitialAndCurrentValues;
import com.metamatrix.console.ui.util.PropertiedObjectPanelHolder;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.CheckBox;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.property.PropertiedObjectPanel;

public class NewBindingWizardSpecificationPanel extends BasicWizardSubpanelContainer
  		implements PropertyChangeListener, ComponentListener {

	private PropertiedObjectPanel popThePanel;

    private ComponentType ctConnector = null;
    private String sConnectorBindingName = "Unknown"; //$NON-NLS-1$

    private TextFieldWidget txfConnectorName = new TextFieldWidget();
    private LabelWidget lblConnectorName = new LabelWidget();
    private TextFieldWidget txfBindingName = new TextFieldWidget();
    private LabelWidget lblBindingName = new LabelWidget();
    private JPanel pnlOuter = new JPanel();
    private JPanel pnlPOPShell = new JPanel(new GridLayout(1, 1));
    private ConfigurationObjectEditor coeEditor;
    private ModificationActionQueue maqActionQForBinding;
    private ConnectorBinding scdNewConnectorBinding;
    private PropertiedObject poPropObject;
    private PropertiedObjectEditor poeEditor;

    private Map /* <PropertyDefinition> to <InitialAndCurrentValues> */
    valuesMap = new HashMap();
    private Map /* <String> name to <PropertyDefinition> */
    defsMap = new HashMap();

    private ConnectionInfo connection = null;

    public NewBindingWizardSpecificationPanel(WizardInterface wizardInterface,
                                              ConnectionInfo connection) {
        super(wizardInterface);
        this.connection = connection;
        init();
    }

    private ConnectorManager getConnectorManager() {
        return ModelManager.getConnectorManager(connection);
    }

    private void init() {
        pnlPOPShell.setPreferredSize(new Dimension(200, 300));

        pnlOuter.setLayout(new GridBagLayout());

        lblBindingName.setText("Binding Name:"); //$NON-NLS-1$
        lblConnectorName.setText("Connector Type:"); //$NON-NLS-1$

        txfBindingName.setEditable(false);
        txfConnectorName.setEditable(false);

        pnlOuter.add(lblBindingName, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.EAST,
                                                            GridBagConstraints.NONE, new Insets(5, 0, 5, 0), 5, 4));
        pnlOuter.add(txfBindingName, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.WEST,
                                                            GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 1), 1, 0));

        pnlOuter.add(lblConnectorName, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.EAST,
                                                              GridBagConstraints.NONE, new Insets(5, 0, 5, 0), 5, 4));
        pnlOuter.add(txfConnectorName, new GridBagConstraints(1, 1, 1, 1, 1.0, 0.0, GridBagConstraints.WEST,
                                                              GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 1), 1, 0));

        pnlOuter.add(pnlPOPShell, new GridBagConstraints(0, 2, 2, 3, 1.0, 1.0, GridBagConstraints.CENTER,
                                                         GridBagConstraints.BOTH, new Insets(8, 0, 0, 1), 1, 0));

        setMainContent(pnlOuter);
        setStepText(2, "Specify Details for this Connector Binding."); //$NON-NLS-1$
        setupListening();

    }

    public ConfigurationObjectEditor getConfigurationObjectEditor() {
        return coeEditor;
    }

    private void updateTextFields() {
        txfConnectorName.setText(ctConnector.getName());
        txfBindingName.setText(sConnectorBindingName);
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
    public void updatePropertiedObjectPanel(ComponentType ctConnector,
                                            String sConnectorBindingName,
                                            boolean initBinding) {
        this.ctConnector = ctConnector;
        this.sConnectorBindingName = sConnectorBindingName;
        updateTextFields();
        boolean showingWaitCursor;
        try {
            showingWaitCursor = true;
            StaticUtilities.startWait(ConsoleMainFrame.getInstance());

            // 1. get ConfigurationObjectEditor to create the Binding
            coeEditor = getConnectorManager().getConnectorBindingEditor();

            // 2. create/copy the binding
            if (initBinding) {
                scdNewConnectorBinding = getConnectorManager().getTentativeConnectorBinding(ctConnector,
                                                                                            coeEditor,
                                                                                            sConnectorBindingName);
            } else {
                scdNewConnectorBinding = getConnectorManager().copyConnectorBinding(scdNewConnectorBinding,
                                                                                    coeEditor,
                                                                                    sConnectorBindingName);
            }

            // 3. Save the 'action' from this create of the binding
            maqActionQForBinding = coeEditor.getDestination();

            // 3.1 Force all property values into editor
            // ConfigurationAdminAPI cAPI = ModelManager.getConfigurationAPI(
            // connectorManager.getConnection());
            // putDefaultValuesInBinding(scdNewConnectorBinding, coeEditor, cAPI);

            // 3.2 create the PropertiedObject based on the binding
            poPropObject = getConnectorManager().getPropertiedObject(scdNewConnectorBinding);

            // 4. create a PropertiedObjectEditor which contains the
            // initial 'create' action
            poeEditor = getConnectorManager().getPropertiedObjectEditor(maqActionQForBinding);

            // 5. Create the PropertiedObjectPanel
            popThePanel = new PropertiedObjectPanel(poeEditor, getConnectorManager().getEncryptor());
            poeEditor.setReadOnly(poPropObject, false);

            // 6. Hand the PropertiedObject to the POP panel
            popThePanel.setNameColumnHeaderWidth(0);
            popThePanel.setPropertiedObject(poPropObject);

            popThePanel.setShowRequiredProperties(true);
            popThePanel.setShowInvalidProperties(true);
            popThePanel.setShowHiddenProperties(false);
            popThePanel.setShowExpertProperties(true);
            popThePanel.setShowOptionalProperties(false);

            popThePanel.refreshDisplay();
            popThePanel.createComponent();

            // 7. Place the POP in its panel
            pnlPOPShell.removeAll();
            ItemListener popChangeListener = new ItemListener() {

                public void itemStateChanged(ItemEvent ev) {
                    includeOptionalsChanged(ev);
                }
            };
            pnlPOPShell.add(new PropertiedObjectPanelHolder(popThePanel, popChangeListener));

            setInitialValues();
            popThePanel.addPropertyChangeListener(this);
            setButtons();
        } catch (Exception e) {
            StaticUtilities.endWait(ConsoleMainFrame.getInstance());
            showingWaitCursor = false;
            ExceptionUtility.showMessage("Failed while creating Connector Binding Panel", e); //$NON-NLS-1$
        }
        if (showingWaitCursor) {
            StaticUtilities.endWait(ConsoleMainFrame.getInstance());
        }
    }

    private void includeOptionalsChanged(ItemEvent ev) {
        CheckBox cb = (CheckBox)ev.getSource();
        popThePanel.setShowOptionalProperties(cb.isSelected());
        popThePanel.refreshDisplay();
    }

    public ConnectorBinding getNewConnectorBinding() {
        return scdNewConnectorBinding;
    }

    public void propertyChange(PropertyChangeEvent evt) {
        setButtons();
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
    
//    private void putDefaultValuesInBinding(ServiceComponentDefn binding,
//            ConfigurationObjectEditor objEditor,
//            ConfigurationAdminAPI configAPI) throws Exception {
//        //we will use this to walk the hierarchy of types for this binding
//        //The highest super type will be top on the stack, on down to the
//        //specific type of the connector binding
//        Stack typeHierarchy = new Stack();
//
//        ComponentTypeID aTypeID = binding.getComponentTypeID();
//        typeHierarchy.push(aTypeID);
//        ComponentType aType = configAPI.getComponentType(aTypeID);
//        aTypeID = aType.getSuperComponentTypeID();
//		//iterate, stack all component type IDs
//        while (aTypeID != null){
//            typeHierarchy.push(aTypeID);
//            aType = configAPI.getComponentType(aTypeID);
//            aTypeID = aType.getSuperComponentTypeID();
//		}
//
//        ComponentTypeDefn aTypeDefn = null;
//        PropertyDefinition propDefn = null;
//        Iterator iter = null;
//        Properties props = new Properties();
//        while (!typeHierarchy.isEmpty()){
//            aTypeID = (ComponentTypeID)typeHierarchy.pop();
//            iter = configAPI.getComponentTypeDefinitions(aTypeID).iterator();
//            while (iter.hasNext()){
//                aTypeDefn = (ComponentTypeDefn)iter.next();
//				propDefn = aTypeDefn.getPropertyDefinition();
//				if (propDefn.hasDefaultValue()){
//                	String propName = propDefn.getName();
//                	String propValue = propDefn.getDefaultValue().toString();
//                    props.setProperty(propName, propValue);
//				}
//            }
//            iter = null;
//        }
//
//        Properties values = binding.getProperties();
//        props.putAll(values);
//
//		//this will internally add the actions to the ModificationActionQueue
//        objEditor.modifyProperties(binding, props, ObjectEditor.ADD);
//	}
}
