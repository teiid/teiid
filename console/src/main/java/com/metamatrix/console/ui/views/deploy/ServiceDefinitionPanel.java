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

//#############################################################################
package com.metamatrix.console.ui.views.deploy;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.swing.JPanel;
import javax.swing.border.CompoundBorder;

import com.metamatrix.admin.api.objects.PropertyDefinition.RestartType;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ProductServiceConfig;
import com.metamatrix.common.config.api.ProductType;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConfigurationPropertiedObjectEditor;
import com.metamatrix.console.security.UserCapabilities;
import com.metamatrix.console.ui.layout.MenuEntry;
import com.metamatrix.console.ui.util.AbstractPanelAction;
import com.metamatrix.console.ui.views.deploy.event.ConfigurationModifier;
import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;
import com.metamatrix.console.ui.views.deploy.util.PropertyConstants;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.CheckBox;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;
import com.metamatrix.toolbox.ui.widget.property.PropertiedObjectPanel;
import com.metamatrix.toolbox.ui.widget.property.PropertyDefinitionLabel;

/**
 * @version 1.0
 * @author Dan Florian
 */
public final class ServiceDefinitionPanel
    extends DetailPanel
    implements ActionListener,
               ConfigurationModifier,
               PropertyChangeListener,
               PropertyConstants {

    ///////////////////////////////////////////////////////////////////////////
    // CONTROLS
    ///////////////////////////////////////////////////////////////////////////

    private CheckBox chkEnabled;
    private CheckBox chkEssential;
    private ServiceDefPOP pnlProps;
    private JPanel pnlPropsOuter;
    private TextFieldWidget txfProd;
    private TextFieldWidget txfPsc;
    private TextFieldWidget txfService;

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private PanelAction actionApply;
    private PanelAction actionReset;
    private ConfigurationPropertiedObjectEditor propEditor;
    private boolean saveEnabled;
    private ServiceComponentDefn service;
    private HashMap propValueMap = new HashMap();
    private PropertiedObject propObj;
    private HashMap propDefsMap = new HashMap();
    private boolean propsDifferent = false;
    
    /**Set<String> set of the names of properties that have been changed*/
    private Set changedPropertyNames = new HashSet();
    
    
    //whether any properties have changed that require a restart
    private RestartType propsDifferentRequiresRestart = RestartType.NONE;

    private PscDefinitionPanel parentPanel;
    private boolean editMode;
        
    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////


    public ServiceDefinitionPanel(boolean includingHdr,
                                  PscDefinitionPanel parentPanel,
                                  ConfigurationID theConfigId,
                                  ConnectionInfo connInfo) {
        
        super(includingHdr, connInfo);
        this.parentPanel = parentPanel;
        setTitle(getString("sdp.title")); //$NON-NLS-1$
        setConfigId(theConfigId);
        
        this.editMode = UserCapabilities.getInstance().canUpdateConfiguration(connInfo);
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    public void actionPerformed(ActionEvent theEvent) {
        checkResetState();
    }

    private void checkResetState() {
        if (includingHdr()) {
            if (isPropertiesValid() &&
                    (propsDifferent || (chkEnabled.isSelected() != saveEnabled))) {
                if (!actionApply.isEnabled()) {
                    actionApply.setEnabled(true);
                    actionReset.setEnabled(true);
                }
            } else {
                if (actionApply.isEnabled()) {
                    actionApply.setEnabled(false);
                    actionReset.setEnabled(false);
                }
            }
        } else {
            parentPanel.checkResetState();
        }
    }

    protected JPanel construct(boolean readOnly) {
        // setup actions first
        actionApply = new PanelAction(PanelAction.APPLY);
        actionApply.setEnabled(false);
        actionReset = new PanelAction(PanelAction.RESET);
        actionReset.setEnabled(false);

        JPanel pnl = new JPanel(new GridBagLayout());

        LabelWidget lblProd = DeployPkgUtils.createLabel("sdp.lblProd"); //$NON-NLS-1$
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(3, 3, 10, 3);
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.anchor = GridBagConstraints.EAST;
        if (includingHdr()) {
            pnl.add(lblProd, gbc);
        }

        txfProd = DeployPkgUtils.createTextField("productname"); //$NON-NLS-1$
        txfProd.setEditable(false);
        gbc.gridx = 1;
        gbc.gridy = 0;
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.anchor = GridBagConstraints.WEST;
        if (includingHdr()) {
            pnl.add(txfProd, gbc);
        }

        LabelWidget lblPsc = DeployPkgUtils.createLabel("sdp.lblPsc"); //$NON-NLS-1$
        gbc.gridx = 2;
        gbc.gridy = 0;
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.anchor = GridBagConstraints.EAST;
        if (includingHdr()) {
            pnl.add(lblPsc, gbc);
        }

        txfPsc = DeployPkgUtils.createTextField("pscname"); //$NON-NLS-1$
        txfPsc.setEditable(false);
        gbc.gridx = 3;
        gbc.gridy = 0;
        gbc.weightx = 0.2;
        gbc.weighty = 0;
        gbc.gridwidth = GridBagConstraints.REMAINDER;
        gbc.anchor = GridBagConstraints.WEST;
        if (includingHdr()) {
            pnl.add(txfPsc, gbc);
        }

        LabelWidget lblService = DeployPkgUtils.createLabel("sdp.lblService"); //$NON-NLS-1$
        gbc.gridx = 0;
        gbc.gridy = 1;
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.gridwidth = 1;
        gbc.insets = new Insets(3, 3, 3, 3);
        gbc.anchor = GridBagConstraints.EAST;
        if (includingHdr()) {
            pnl.add(lblService, gbc);
        }

        txfService = DeployPkgUtils.createTextField("servicename"); //$NON-NLS-1$
        txfService.setEditable(false);
        gbc.gridx = 1;
        gbc.gridy = 1;
        gbc.weightx = 0.3;
        gbc.weighty = 0;
        gbc.anchor = GridBagConstraints.WEST;
        if (includingHdr()) {
            pnl.add(txfService, gbc);
        }

        LabelWidget lblEnabled = DeployPkgUtils.createLabel("sdp.lblEnabled"); //$NON-NLS-1$
        gbc.gridx = 2;
        gbc.gridy = 1;
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.anchor = GridBagConstraints.EAST;
        if (includingHdr()) {
            pnl.add(lblEnabled, gbc);
        }

        chkEnabled = new CheckBox();
        chkEnabled.addActionListener(this);
        gbc.gridx = 3;
        gbc.gridy = 1;
        gbc.anchor = GridBagConstraints.WEST;
        if (includingHdr()) {
            pnl.add(chkEnabled, gbc);
        }

        LabelWidget lblEssential = DeployPkgUtils.createLabel("sdp.lblEssential"); //$NON-NLS-1$
        gbc.gridx = 4;
        gbc.gridy = 1;
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.anchor = GridBagConstraints.EAST;
        if (includingHdr()) {
            pnl.add(lblEssential, gbc);
        }

        chkEssential = new CheckBox();
        chkEssential.setEnabled(false);
        gbc.gridx = 5;
        gbc.gridy = 1;
        gbc.anchor = GridBagConstraints.WEST;
        if (includingHdr()) {
            pnl.add(chkEssential, gbc);
        }

        pnlPropsOuter = new JPanel(new GridLayout(1, 1));
        setPnlPropsOuterBorder(null);
        gbc.gridx = 0;
        gbc.gridy = 2;
        gbc.gridwidth = GridBagConstraints.REMAINDER;
        gbc.fill = GridBagConstraints.BOTH;
        if (includingHdr()) {
            gbc.insets = new Insets(3, 3, 20, 3);
        } else {
            gbc.insets = new Insets(0, 0, 0, 0);
        }
        gbc.anchor = GridBagConstraints.WEST;
        gbc.weightx = 1.0;
        gbc.weighty = 1.0;
        pnl.add(pnlPropsOuter, gbc);

        JPanel pnlOps = new JPanel();
        gbc.gridx = 0;
        gbc.gridy = 3;
        gbc.fill = GridBagConstraints.NONE;
        gbc.anchor = GridBagConstraints.CENTER;
        if (includingHdr()) {
            gbc.insets = new Insets(3, 3, 3, 3);
        } else {
            gbc.insets = new Insets(0, 0, 0, 0);
        }
        gbc.weightx = 0.0;
        gbc.weighty = 0.0;
        pnl.add(pnlOps, gbc);

        if (includingHdr()) {
            JPanel pnlOpsSizer = new JPanel(new GridLayout(1, 2, 10, 0));
            pnlOps.add(pnlOpsSizer);

            ButtonWidget btnApply = new ButtonWidget();
            setup(MenuEntry.ACTION_MENUITEM, btnApply, actionApply);
            pnlOpsSizer.add(btnApply);

            ButtonWidget btnReset = new ButtonWidget();
            setup(MenuEntry.ACTION_MENUITEM, btnReset, actionReset);
            pnlOpsSizer.add(btnReset);
        }

        // initialize the properties editor and panel
        try {
            propEditor = getConfigurationManager().getPropertiedObjectEditor();
            pnlProps = new ServiceDefPOP(propEditor);
            pnlProps.setReadOnlyForced(readOnly);
            pnlProps.createComponent();
            pnlProps.setColumnHeaderNames(getString("pop.propertyname.hdr"), //$NON-NLS-1$
                    getString("pop.propertyvalue.hdr")); //$NON-NLS-1$
            pnlProps.addPropertyChangeListener(this);
            pnlProps.setShowInvalidProperties(true);
            pnlProps.setShowRequiredProperties(true);
            pnlProps.setShowExpertProperties(true);
            pnlPropsOuter.add(pnlProps);
        } catch (ExternalException theException) {
            throw new IllegalStateException(
                getString("msg.configmgrproblem", //$NON-NLS-1$
                          new Object[] {getClass(), "construct"})); //$NON-NLS-1$
        }

        return pnl;
    }

    private void setPnlPropsOuterBorder(String serviceName) {
        String title;
        if (serviceName == null) {
            title = "Properties"; //$NON-NLS-1$
        } else {
            title = "Properties of " + serviceName; //$NON-NLS-1$
        }
        TitledBorder tBorder;
        tBorder = new TitledBorder(title);
        if (includingHdr()) {
            pnlPropsOuter.setBorder(
                    new CompoundBorder(tBorder,DeployPkgUtils.EMPTY_BORDER));
        } else {
            pnlPropsOuter.setBorder(tBorder);
        }
    }

    private boolean equivalent(
        Object theValue,
        Object theOtherValue) {

        return (((theValue == null) && (theOtherValue == null)) ||
                ((theValue != null) && (theOtherValue != null) &&
                theValue.equals(theOtherValue)));
    }

    public boolean isPersisted() {
        boolean persisted;
        if (includingHdr()) {
            persisted = (!actionApply.isEnabled());
        } else {
            persisted = parentPanel.isPersisted();
        }
        return persisted;
    }

    private boolean isPropertiesValid() {
        return pnlProps.getInvalidDefinitions().isEmpty();
    }

    public void persist() throws ExternalException {

        if (includingHdr() && (saveEnabled != chkEnabled.isSelected())) {
            saveEnabled = chkEnabled.isSelected();
        }
        if (propsDifferent) {
        	String message = null;
            switch (propsDifferentRequiresRestart) {
            case NONE:
            	message = "The change will take effect immediately."; //$NON-NLS-1$
            	break;
            case SERVICE:
                message = "The change(s) will not take effect until the affected services/connectors are restarted in the Runtime panel."; //$NON-NLS-1$
                break;
            case PROCESS:
                message = "You have changed some properties marked " + PropertyDefinitionLabel.REQUIRES_PROCESS_RESTART_LABEL + "These properties will not take effect until the server is restarted or bounced."; //$NON-NLS-1$ //$NON-NLS-2$
            	break;
            case ALL_PROCESSES:
            	message = "You have changed some properties marked " + PropertyDefinitionLabel.REQUIRES_BOUNCE_LABEL + "These properties will not take effect until the system is bounced."; //$NON-NLS-1$ //$NON-NLS-2$
            	break;
            case CLUSTER:
            	message = "You have changed some properties marked " + PropertyDefinitionLabel.REQUIRES_CLUSTER_RESTART_LABEL + "These properties will not take effect until the system, including host controllers, is restarted."; //$NON-NLS-1$ //$NON-NLS-2$
                break;
            }
            
            StaticUtilities.displayModalDialogWithOK("Modify Service Properties", message); //$NON-NLS-1$
            
            getConfigurationManager().modifyPropertiedObject(propEditor);
            propValueMap.clear();
            propsDifferent = false;
            propsDifferentRequiresRestart = RestartType.NONE;
            changedPropertyNames.clear();
        }
        checkResetState();
    }

    public void propertyChange(PropertyChangeEvent theEvent) {
        // if the property value has been changed before check
        // to see if it now agrees with the original value. if it
        // does, remove it from the list of changed properties.
        String eventProp = theEvent.getPropertyName();
        if (propValueMap.containsKey(eventProp)) {
            
            Object original = propValueMap.get(eventProp);
            Object current = theEvent.getNewValue();
            boolean different = !equivalent(original, current);
            if (different) {
                changedPropertyNames.add(eventProp);
            } else {
                changedPropertyNames.remove(eventProp);
            }
        } else {
            // save original value if not previously saved
            // propValueMap contains properties that have changed at one time
            // they may now hold the original value however
            propValueMap.put(eventProp, theEvent.getOldValue());
            changedPropertyNames.add(eventProp);
        }
        
        propsDifferent = (changedPropertyNames.size() > 0);        
        propsDifferentRequiresRestart = checkPropsDifferentRequiresRestart(); 
        
        checkResetState();
    }

    /**
     * Check if any properties have changed for which getRequiresRestart()==true
     * @return
     * @since 4.3
     */
    private RestartType checkPropsDifferentRequiresRestart() {
    	RestartType result = RestartType.NONE;
        if (propsDifferent) {
            Iterator itr = changedPropertyNames.iterator();
            while (itr.hasNext()) {
                String prop = (String) itr.next();
                PropertyDefinition def = (PropertyDefinition) propDefsMap.get(prop);
                if (def != null && def.getRequiresRestart().compareTo(result) > 0) {
                    result = def.getRequiresRestart();
                }
            }
        }
        return result;        
    }
    
    
    
    public boolean propertiesHaveChanged() {
        return propsDifferent;
    }
    
    public void reset() {
        if (chkEnabled.isSelected() != saveEnabled) {
          chkEnabled.setSelected(saveEnabled);
        }
        if (propsDifferent) {
            resetPropertiedObject();
        }
        checkResetState();
    }

    private void resetPropertiedObject() {
        propsDifferent = false;
        propsDifferentRequiresRestart = RestartType.NONE;
        changedPropertyNames.clear();
        Iterator itr = propValueMap.keySet().iterator();
        while (itr.hasNext()) {
            String prop = (String)itr.next();
            PropertyDefinition def = (PropertyDefinition)propDefsMap.get(prop);
            propEditor.setValue(propObj, def, propValueMap.get(prop));
        }
        pnlProps.refreshDisplay();
        propValueMap.clear();
    }

    private void savePropertyDefinitions() {
        if (propObj != null) {
            List defs = propEditor.getPropertyDefinitions(propObj);
            Iterator it = defs.iterator();
            while (it.hasNext()) {
                PropertyDefinition def = (PropertyDefinition)it.next();
                propDefsMap.put(def.getName(), def);
            }
        }
    }

    public void setConfigId(ConfigurationID theConfigId) {

        super.setConfigId(theConfigId);
        setTitleSuffix(getString("sdp.title.suffix")); //$NON-NLS-1$
    }

    public ServiceComponentDefn getService() {
        return service;
    }
    
    public void displayDetailFor(ServiceComponentDefn serviceDef,
            Object[] theAncestors) {
        setDomainObject(serviceDef, theAncestors);
    }

    public void setDomainObject(Object theDomainObject, Object[] theAncestors) {
        if (theDomainObject instanceof ServiceComponentDefn) {
            service = (ServiceComponentDefn)theDomainObject;
            setTitleSuffix(service.toString());
            setPnlPropsOuterBorder(service.toString());
        } else {
            if (theDomainObject != null) {
                throw new IllegalArgumentException(
                        getString("msg.invalidclass", //$NON-NLS-1$
                        new Object[] {"ServiceComponentDefn", //$NON-NLS-1$
                        theDomainObject.getClass()}));
            }
            service = null;
            setPnlPropsOuterBorder(null);
        }
        super.setDomainObject(service, theAncestors);

        if (includingHdr()) {
            ProductServiceConfig psc = (ProductServiceConfig)theAncestors[0];
            txfPsc.setText(psc.getName());
            txfService.setText(service.toString());
            String essential = service.getProperty(ESSENTIAL_PROP);
            if (essential == null) {
                essential = ""; //$NON-NLS-1$
            }
            chkEssential.setSelected((new Boolean(essential)).booleanValue());
            ProductType product = getConfigurationManager().getProduct(psc);
            txfProd.setText(product.getName());
            
            ServiceComponentDefnID svcID = (ServiceComponentDefnID) service.getID();
            
            if (!psc.containsService(svcID)) {
                throw new IllegalArgumentException("Service " + svcID + " not contained in PSC " + psc.getName());                    	 //$NON-NLS-1$ //$NON-NLS-2$
            }
            
            //               Boolean enabled = new Boolean(service.isEnabled());
            
            saveEnabled = psc.isServiceEnabled( (ServiceComponentDefnID) service.getID() ) ;
//          Boolean enabled = new Boolean(psc.isServiceEnabled( (ServiceComponentDefnID) service.getID() ) );
            
//          saveEnabled = enabled.booleanValue();
            chkEnabled.setSelected(saveEnabled);
        }
        
        propDefsMap.clear();
        propValueMap.clear();
        
        if (theDomainObject != null) {
            propObj = getConfigurationManager()
            .getPropertiedObjectForComponentObject(service);
            pnlProps.setNameColumnHeaderWidth(0);
            pnlProps.setPropertiedObject(propObj);
            
            setEnabled(editMode && getConfigurationManager().isEditable(service.getConfigurationID()));
            
        } else {
            propObj = null;
            pnlProps.setPropertiedObject(null);
        }
        
        pnlProps.resizeNameColumn();
        savePropertyDefinitions();
        
    }

    public void setEnabled(boolean theEnableFlag) {
        chkEnabled.setEnabled(theEnableFlag);
        pnlProps.setReadOnlyForced(!theEnableFlag);
        
        ///propEditor.setReadOnly(propObj, !theEnableFlag);

        pnlProps.refreshDisplay();
    }

    ///////////////////////////////////////////////////////////////////////////
    // INNER CLASSES
    ///////////////////////////////////////////////////////////////////////////

    private class PanelAction extends AbstractPanelAction {
        public static final int APPLY = 0;
        public static final int RESET = 1;

        public PanelAction(int theType) {
            super(theType);
            if (theType == APPLY) {
                putValue(NAME, getString("sdp.actionApply")); //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION, getString("sdp.actionApply.tip")); //$NON-NLS-1$
                setMnemonic(getMnemonicChar("sdp.actionApply.mnemonic")); //$NON-NLS-1$
            } else if (theType == RESET) {
                putValue(NAME, getString("sdp.actionReset")); //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION, getString("sdp.actionReset.tip")); //$NON-NLS-1$
                setMnemonic(getMnemonicChar("sdp.actionReset.mnemonic")); //$NON-NLS-1$
            } else {
                throw new IllegalArgumentException(
                        getString("msg.invalidactiontype") + theType); //$NON-NLS-1$
            }
        }
        protected void actionImpl(ActionEvent theEvent)
            throws ExternalException {
            if (type == APPLY) {
                persist();
            } else if (type == RESET) {
                reset();
            }
        }
    }
    
    

    class ServiceDefPOP extends PropertiedObjectPanel {
        public ServiceDefPOP(PropertiedObjectEditor poe) {
            super(poe, getEncryptor());
        }

        public void resizeNameColumn() {
            super.resizeNameColumn();
        }
    }//end ServiceDefPOP
}//end ServiceDefinitionPanel



