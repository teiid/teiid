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

package com.metamatrix.console.ui.views.deploy;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;

import javax.swing.JPanel;

import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConfigurationManager;
import com.metamatrix.console.models.ConfigurationPropertiedObjectEditor;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.util.ExpertPropertiedObjectPanelHolder;
import com.metamatrix.console.ui.util.POPWithButtons;
import com.metamatrix.console.ui.util.POPWithButtonsController;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.property.PropertiedObjectPanel;



public class ServiceDefinitionPropertiesPanel extends JPanel
        implements POPWithButtonsController {
    private PropertiedObjectPanel pop;
    private ExpertPropertiedObjectPanelHolder popHolder;
    private POPWithButtons popWithButtons = null; 

//    private TextFieldWidget txfServiceName     = new TextFieldWidget();
//    private LabelWidget lblServiceName         = new LabelWidget();
 //   private TextFieldWidget txfBindingName       = new TextFieldWidget();
//    private LabelWidget lblBindingName           = new LabelWidget();
    private JPanel pnlOuter         = new JPanel();
    private JPanel pnlPOPShell      = new JPanel(new GridLayout(1, 1));
    private PropertiedObject poPropObject;
    private ConfigurationPropertiedObjectEditor poe;
    private ServiceComponentDefn serviceDefn;
    private boolean canModify;
    private ConnectionInfo connection;
//    private ModificationActionQueue maq = null;
    private ConfigurationObjectEditor coe = null;
    

    public ServiceDefinitionPropertiesPanel(boolean modifiable, ConnectionInfo connection) {
        super();
        canModify = modifiable;
        this.connection = connection;
        init();
        try {
            coe = getConfigurationManager().getEditor();
 //            maq = coe.getDestination();
            poe = getPropertiedObjectEditor();
        } catch (Exception ex) {
            LogManager.logError(LogContexts.CONFIG, ex,
                    "Error creating Connector Binding Properties Panel."); //$NON-NLS-1$
            ExceptionUtility.showMessage(
                    "Error creating Connector Binding Properties Panel", ex); //$NON-NLS-1$
        }
    }
    
	private ConfigurationManager getConfigurationManager() {
        return ModelManager.getConfigurationManager(connection);
    }

    private void init() {
        pnlPOPShell.setPreferredSize(new Dimension(200, 300));

        setLayout(new BorderLayout());

        pnlOuter.setLayout(new GridBagLayout());

 //       lblBindingName.setText("Service Name:"); //$NON-NLS-1$
//        lblServiceName.setText("Servicie Name:"); //$NON-NLS-1$

//        txfBindingName.setEditable(false);
 //       txfServiceName.setEditable(false);
        add(pnlPOPShell, BorderLayout.CENTER);
    }


    private ConfigurationPropertiedObjectEditor getPropertiedObjectEditor() {
        if (poe == null) {
            // 4. create a PropertiedObjectEditor
            try {
                poe = getConfigurationManager().getPropertiedObjectEditor();
            } catch (Exception e) {
                ExceptionUtility.showMessage(
                        "Failed to get editor for Service Properties panel  ", e); //$NON-NLS-1$
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
                pop = new PropertiedObjectPanel(poe, getConfigurationManager().getEncryptor());
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
            if (serviceDefn == null) {
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

    public void setServiceComponentDefn(ServiceComponentDefn svcDefn) {
        this.serviceDefn = svcDefn;
        populateTable();
    }

    public ServiceComponentDefn getServiceComponentDefn() {
        return serviceDefn;
    }

    public void updatePropertiedObjectPanel() {
		ServiceComponentDefn svcdefn = getServiceComponentDefn();
        try {
            poPropObject = getConfigurationManager().getPropertiedObjectForComponentObject(svcdefn);
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
            ItemListener includeExpertListener = new ItemListener() {
                public void itemStateChanged(ItemEvent ev) {
                    includeOptionalStateChanged();
                }
            };
            popHolder = new ExpertPropertiedObjectPanelHolder(pop, includeExpertListener);
            popWithButtons = new POPWithButtons(popHolder, poe, this);

            pnlPOPShell.add(popWithButtons);

            popHolder.setIsIncludingExpertProperties(includeExpert);

        } catch(Exception e) {
            ExceptionUtility.showMessage("Failed while creating Service Definition Panel", //$NON-NLS-1$
                    e);
        }

    }

    private void includeOptionalStateChanged() {
        getPropertiedObjectPanel().setShowExpertProperties(
                popHolder.isIncludingExpertProperties());
        getPropertiedObjectPanel().refreshDisplay();
    }

    public ServiceComponentDefn getNewComponentDefn() {
        return serviceDefn;
    }

	public boolean doApplyChanges(PropertiedObjectPanel pop) {
        boolean proceeding = true;
        try {
            StaticUtilities.displayModalDialogWithOK("Modify Service Definition",  //$NON-NLS-1$
            		"Note: Change will not take effect until connector is " + //$NON-NLS-1$
            		"restarted in the System State panel."); //$NON-NLS-1$
 //           getConfigurationManager().saveConnectorBinding(maq);
            getConfigurationManager().modifyPropertiedObject(poe);
            
            
            ServiceComponentDefn svc = (ServiceComponentDefn) getConfigurationManager().getConfig(Configuration.NEXT_STARTUP_ID).getServiceComponentDefn( 
                        (ComponentDefnID) serviceDefn.getID());
            setServiceComponentDefn(svc);  
            
        } catch (Exception ex) {
            LogManager.logError(LogContexts.CONFIG, ex,
                    "Error saving service definition changes."); //$NON-NLS-1$
            ExceptionUtility.showMessage("Error saving service definition changes", //$NON-NLS-1$
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
