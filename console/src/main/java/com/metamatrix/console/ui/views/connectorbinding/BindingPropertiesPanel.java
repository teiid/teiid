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
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;

import javax.swing.JPanel;

import com.metamatrix.common.actions.ModificationActionQueue;
import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConnectorManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.util.*;
import com.metamatrix.console.util.*;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.property.PropertiedObjectPanel;



public class BindingPropertiesPanel extends JPanel
        implements POPWithButtonsController {
    private PropertiedObjectPanel pop;
    private ExpertPropertiedObjectPanelHolder popHolder;
    private POPWithButtons popWithButtons = null; 

    private TextFieldWidget txfConnectorName     = new TextFieldWidget();
    private LabelWidget lblConnectorName         = new LabelWidget();
    private TextFieldWidget txfBindingName       = new TextFieldWidget();
    private LabelWidget lblBindingName           = new LabelWidget();
    private JPanel pnlOuter         = new JPanel();
    private JPanel pnlPOPShell      = new JPanel(new GridLayout(1, 1));
    private PropertiedObject poPropObject;
    private PropertiedObjectEditor poe;
    private ServiceComponentDefn connectorBindingDefn;
    private boolean canModify;
    private ConnectionInfo connection;
    private ModificationActionQueue maq = null;
    private ConfigurationObjectEditor coe = null;
    

    public BindingPropertiesPanel(boolean modifiable, ConnectionInfo connection) {
        super();
        canModify = modifiable;
        this.connection = connection;
        init();
        try {
            coe = getConnectorManager().getConnectorBindingEditor();
            maq = coe.getDestination();
            poe = getConnectorManager().getPropertiedObjectEditor(maq);
        } catch (Exception ex) {
            LogManager.logError(LogContexts.CONNECTOR_BINDINGS, ex,
                    "Error creating Connector Binding Properties Panel."); //$NON-NLS-1$
            ExceptionUtility.showMessage(
                    "Error creating Connector Binding Properties Panel", ex); //$NON-NLS-1$
        }
    }
    
    private ConnectorManager getConnectorManager() {
        return ModelManager.getConnectorManager(connection);
    }

    private void init() {
        pnlPOPShell.setPreferredSize(new Dimension(200, 300));

        setLayout(new BorderLayout());

        pnlOuter.setLayout(new GridBagLayout());

        lblBindingName.setText("Binding Name:"); //$NON-NLS-1$
        lblConnectorName.setText("Connector Type:"); //$NON-NLS-1$

        txfBindingName.setEditable(false);
        txfConnectorName.setEditable(false);
        add(pnlPOPShell, BorderLayout.CENTER);
    }


    private PropertiedObjectEditor getPropertiedObjectEditor() {
        if (poe == null) {
            // 4. create a PropertiedObjectEditor
            try {
                poe = getConnectorManager().getPropertiedObjectEditor();
            } catch (Exception e) {
                ExceptionUtility.showMessage(
                        "Failed to get editor for Binding propd panel  ", e); //$NON-NLS-1$
            }
        }
        return poe;
    }

    private PropertiedObjectPanel getPropertiedObjectPanel() {
        if (pop == null) {
            try {
                StaticUtilities.startWait(ConsoleMainFrame.getInstance());

                // 4. create a PropertiedObjectEditor which contains the
                //    initial 'create' action
                poe = getPropertiedObjectEditor();

                // 5. Create the PropertiedObjectPanel
                pop = new PropertiedObjectPanel(poe, getConnectorManager().getEncryptor());
            } catch (RuntimeException ex) {
                StaticUtilities.endWait(ConsoleMainFrame.getInstance());
                throw ex;
            }
            StaticUtilities.endWait(ConsoleMainFrame.getInstance());
        }
        return pop;
    }

    private void populateTable() {
		try {
            StaticUtilities.startWait(ConsoleMainFrame.getInstance());
            if (connectorBindingDefn == null) {
                pnlPOPShell.removeAll();
            } else {
                updatePropertiedObjectPanel();
            }
        } catch (RuntimeException ex) {
            StaticUtilities.endWait(ConsoleMainFrame.getInstance());
            //throw ex;
            ExceptionUtility.showMessage("Failed in populateTable", ex); //$NON-NLS-1$
        }
        StaticUtilities.endWait(ConsoleMainFrame.getInstance());
    }

    public void setConnectorBinding(ServiceComponentDefn connectorBindingDefn) {
        this.connectorBindingDefn = connectorBindingDefn;
        populateTable();
    }

    public ServiceComponentDefn getConnectorBinding() {
        return connectorBindingDefn;
    }

    public void updatePropertiedObjectPanel() {
		ServiceComponentDefn connectorBindingDefn = getConnectorBinding();
        try {
            poPropObject = getConnectorManager().getPropertiedObject(connectorBindingDefn);
			getPropertiedObjectPanel().setNameColumnHeaderWidth(0);
            getPropertiedObjectPanel().setPropertiedObject(poPropObject, poe);
            poe.setReadOnly(poPropObject, false);

            getPropertiedObjectPanel().setShowRequiredProperties(true);
            getPropertiedObjectPanel().setShowInvalidProperties(true);
            getPropertiedObjectPanel().setShowHiddenProperties(false);
            getPropertiedObjectPanel().setShowExpertProperties(true);

            if (!canModify) {
                getPropertiedObjectPanel().setReadOnlyForced(true);
            }

            getPropertiedObjectPanel().createComponent();
            getPropertiedObjectPanel().refreshDisplay();

            boolean includeExpert = false;
            if (popHolder != null) {
            	includeExpert = popHolder.isIncludingExpertProperties();
            }

            pnlPOPShell.removeAll();
            ItemListener includeOptionalListener = new ItemListener() {
                public void itemStateChanged(ItemEvent ev) {
                    includeExpertStateChanged();
                }
            };
            popHolder = new ExpertPropertiedObjectPanelHolder(pop, includeOptionalListener);
            popWithButtons = new POPWithButtons(popHolder, poe, this);

            pnlPOPShell.add(popWithButtons);

            popHolder.setIsIncludingExpertProperties(includeExpert);

        } catch(Exception e) {
            ExceptionUtility.showMessage("Failed while creating Connector Binding Panel", //$NON-NLS-1$
                    e);
        }

    }

    private void includeExpertStateChanged() {
        getPropertiedObjectPanel().setShowExpertProperties(
                popHolder.isIncludingExpertProperties());
        getPropertiedObjectPanel().refreshDisplay();
    }

    public ServiceComponentDefn getNewConnectorBinding() {
        return connectorBindingDefn;
    }

	public boolean doApplyChanges(PropertiedObjectPanel pop) {
        boolean proceeding = true;
        try {
            StaticUtilities.displayModalDialogWithOK("Modify Connector Binding",  //$NON-NLS-1$
            		"Note: Change will not take effect until connector is " + //$NON-NLS-1$
            		"restarted in the System State panel."); //$NON-NLS-1$
            getConnectorManager().saveConnectorBinding(maq);
            
            
            ConnectorBinding cb = (ConnectorBinding) getConnectorManager().getConfigurationAdminAPI().getComponentDefn(Configuration.NEXT_STARTUP_ID, 
                        (ComponentDefnID) connectorBindingDefn.getID());
            setConnectorBinding(cb);  
            
        } catch (Exception ex) {
            LogManager.logError(LogContexts.CONNECTOR_BINDINGS, ex,
                    "Error saving connector binding changes."); //$NON-NLS-1$
            ExceptionUtility.showMessage("Error saving connector binding changes", //$NON-NLS-1$
                    ex);
        }
        return proceeding;
    }
    
    
    
    public void applyProperties() {
        if (popWithButtons != null) {
            popWithButtons.applyPressed();
        }        
    }
    public void resetProperties() {
        if (popWithButtons != null) {
            popWithButtons.resetPressed();
        }        
    }
    public boolean anyValueChanged() {
        if (popWithButtons == null) {
            return false;
        }
        return popWithButtons.anyValueChanged();
    }
}
