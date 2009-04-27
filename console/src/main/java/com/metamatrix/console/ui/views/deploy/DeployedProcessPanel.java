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

import java.awt.Color;
import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.swing.DefaultCellEditor;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JComboBox;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.border.CompoundBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumn;

import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConfigurationPropertiedObjectEditor;
import com.metamatrix.console.ui.layout.MenuEntry;
import com.metamatrix.console.ui.util.AbstractPanelAction;
import com.metamatrix.console.ui.views.deploy.event.ConfigurationModifier;
import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;
import com.metamatrix.console.ui.views.deploy.util.DeployTableSorter;
import com.metamatrix.console.ui.views.deploy.util.PropertyConstants;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.DialogWindow;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;
import com.metamatrix.toolbox.ui.widget.property.PropertiedObjectPanel;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;

/**
 * @version 1.0
 * @author Dan Florian
 */
public final class DeployedProcessPanel
    extends DetailPanel
    implements ConfigurationModifier,PropertyConstants,
        ActionListener,
        PropertyChangeListener,
        ItemListener {
    


/******** DESIGN NOTES

- Note 1: the process name was originally going to be editable.
  The server-side code has not been written to allow this. The GUI code
  that allowed the name to be changed is commented out in case we want
  to do that in the future. dan 8-30-01

********* END DESIGN NOTES */

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

//    private static final String NO_PSC =
//        DeployPkgUtils.getString("drp.nodeployedpsc"); //$NON-NLS-1$

    ///////////////////////////////////////////////////////////////////////////
    // CONTROLS
    ///////////////////////////////////////////////////////////////////////////

    private TableWidget tblSvcs;
    private TextFieldWidget txfHost;

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private PanelAction actionApply;
    private PanelAction actionDelete;
    private PanelAction actionReset;
    private VMComponentDefn process;
    private ProcessPOP pnlProps;
    private JPanel pnlPropsOuter;
    
    private DefaultTableModel tblModel;
    private HashMap prodRowMap = new HashMap();
    private HashMap propValueMap = new HashMap();   
    private HashMap propDefsMap = new HashMap();    
    private boolean processEvents = true;
    private ConfigurationPropertiedObjectEditor propEditor;
    private PropertiedObject propObj;
    private boolean deleting = false;
    private boolean propsDifferent = false;
    private String processName = ""; //$NON-NLS-1$
    
    
    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public DeployedProcessPanel(ConnectionInfo connInfo) {
        super(connInfo);
        setTitle(DeployPkgUtils.getString("drp.title")); //$NON-NLS-1$)
    }

    public DeployedProcessPanel(ConfigurationID theConfigId,
                                ConnectionInfo connInfo)
        throws ExternalException {

        this(connInfo);
        setConfigId(theConfigId);
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////
    
    public void actionPerformed(ActionEvent theEvent) {
        checkResetState();
    }    

    
    private void checkResetState() {
            if (isPropertiesValid() &&  (propsDifferent) ) {
            		//||  (numPscsDifferent > 0))) {
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
            
    }
    
    private boolean isPropertiesValid() {
        return pnlProps.getInvalidDefinitions().isEmpty();
    }    
    

    protected JPanel construct(boolean readOnly) {
        // setup actions first
        actionApply = new PanelAction(PanelAction.APPLY);
        actionApply.setEnabled(false);
        actionDelete = new PanelAction(PanelAction.DELETE);
        actionReset = new PanelAction(PanelAction.RESET);
        actionReset.setEnabled(false);

        JPanel pnl = new JPanel(new GridBagLayout());
        

        LabelWidget lblHost = DeployPkgUtils.createLabel("drp.lblHost"); //$NON-NLS-1$
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.insets = new Insets(3, 3, 3, 3);
        gbc.anchor = GridBagConstraints.EAST;
        pnl.add(lblHost, gbc);

        txfHost = DeployPkgUtils.createTextField("hostname"); //$NON-NLS-1$
        txfHost.setEditable(false);
        
        gbc.gridx = 1;
        gbc.gridy = 0;
        gbc.weightx = 0.1;
        gbc.weighty = 0;
        gbc.anchor = GridBagConstraints.WEST;
        pnl.add(txfHost, gbc);


        TitledBorder tBorder;
       
        pnlPropsOuter = new JPanel(new GridLayout(1, 1));
        setPnlPropsOuterBorder(null);
        
        
        gbc.gridx = 0;
        gbc.gridy = 1;
        gbc.gridwidth = GridBagConstraints.REMAINDER;
        gbc.fill = GridBagConstraints.BOTH;
        if (includingHdr()) {
            gbc.insets = new Insets(3, 3, 5, 3);
        } else {
            gbc.insets = new Insets(0, 0, 0, 0);
        }
        gbc.anchor = GridBagConstraints.WEST;
        gbc.weightx = 1.0;
        gbc.weighty = 1.0;
        pnl.add(pnlPropsOuter, gbc);

                
        // initialize the properties editor and panel
        try {
            propEditor = getConfigurationManager().getPropertiedObjectEditor();
            pnlProps = new ProcessPOP(propEditor);
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

        JPanel pnlPscs = new JPanel(new GridLayout(1, 1));
        tBorder = new TitledBorder(getString("drp.pnlSvcs.title")); //$NON-NLS-1$
        pnlPscs.setBorder(
            new CompoundBorder(tBorder,
                               DeployPkgUtils.EMPTY_BORDER));
        gbc.gridx = 0;
        gbc.gridy = 2;
        gbc.insets = new Insets(3, 3, 5, 3);
        gbc.gridwidth = GridBagConstraints.REMAINDER;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weightx = 1.0;
        gbc.weighty = 1.0;
        pnl.add(pnlPscs, gbc);

        tblSvcs = new TableWidget();
        tblModel =
            DeployPkgUtils.setup(
                    tblSvcs,
                    DeployPkgUtils.SERV_DEF_HDRS,
                    DeployPkgUtils.getInt("csp.psctblrows", 10), //$NON-NLS-1$
                    null); //  new int[] {SVC_COL, ENABLED_COL});
            tblSvcs.setComparator(new DeployTableSorter());
               
 //       TableColumn vmCol = tblSvcs.getColumnModel().getColumn(SVC_COL);
//        pscCellComp = new PscCellComponent();
//        vmCol.setCellEditor(pscCellComp);
//        pscCol.setCellRenderer(pscCellComp);
        tblSvcs.setSortable(false);
        tblSvcs.setComparator(new DeployTableSorter());

        JScrollPane spnPscs = new JScrollPane(tblSvcs);
        pnlPscs.add(spnPscs);
        
          
        
        JPanel pnlOps = new JPanel();
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 3;
        gbc.gridwidth = GridBagConstraints.REMAINDER;
        gbc.insets = new Insets(3, 3, 3, 3);
        pnl.add(pnlOps, gbc);
        
        
        JPanel pnlOpsSizer = new JPanel(new GridLayout(1, 3, 10, 0));
        pnlOps.add(pnlOpsSizer);

      ButtonWidget btnApply = new ButtonWidget();
      setup(MenuEntry.ACTION_MENUITEM, btnApply, actionApply);
      pnlOpsSizer.add(btnApply);

      ButtonWidget btnDelete = new ButtonWidget();
      setup(MenuEntry.ACTION_MENUITEM, btnDelete, actionDelete);
      pnlOpsSizer.add(btnDelete);

      ButtonWidget btnReset = new ButtonWidget();
      setup(MenuEntry.ACTION_MENUITEM, btnReset, actionReset);
      pnlOpsSizer.add(btnReset);
 
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
    

    private void delete() throws ExternalException {

        // show dialog to save/abort changes
        Object[] processName = {process.getName()};

        ConfirmationPanel pnlConfirm =
            new ConfirmationPanel("drp.msg.confirmdelete", processName); //$NON-NLS-1$
        DialogWindow.show(
            this,
            DeployPkgUtils.getString("drp.confirmdelete.title", processName), //$NON-NLS-1$
            pnlConfirm);
        if (pnlConfirm.isConfirmed()) {
        	deleting = true;
            getConfigurationManager().deleteProcess(process, getConfigId());
             propsDifferent = false;
            checkResetState();
            deleting = false;
        }

    }


    public String getProcessName() {
        String str = ""; //$NON-NLS-1$
        if (process != null) {
            str = process.toString();
        }
        return str;
    }
    
    private void initTable() {
        try {
        	Configuration config = getConfigurationManager().getConfig(getConfigId());
//            Collection services = config.getDeployedServicesForVM(process);
           
//            pscCellComp.setPscValues(prodPscs);
            if (tblModel.getRowCount() == 0) {
            	
                DeployPkgUtils.loadServiceDefintions(config, tblModel,
                        getConnectionInfo());

//                Iterator svcItr = services.iterator();
//                while (svcItr.hasNext()) {
//                    Vector row = new Vector(SVC_HDRS.length);
//                    row.setSize(SVC_HDRS.length);
//                    row.setElementAt(svcItr.next(), SVC_COL);
//                    row.setElementAt(NO_PSC, ENABLED_COL);
//                    tblModel.addRow(row);
//                }
            }
        }
        catch (Exception theException) {
            ExceptionUtility.showMessage(
                getString("msg.configmgrproblem", //$NON-NLS-1$
                          new Object[] {getClass(), "initTable"}), //$NON-NLS-1$
                ""+theException.getMessage(), //$NON-NLS-1$
                theException);
            LogManager.logError(LogContexts.CONFIG,
                                theException,
                                getClass() + ":initTable"); //$NON-NLS-1$
        }


    }


    public boolean isPersisted() {
    	boolean persisted = true;
    	if (!deleting) {
    		if (actionApply.isEnabled()) {
    			persisted = false;
    		}
    	}
    	return persisted;
    }

    
    public void propertyChange(PropertyChangeEvent theEvent) {
        propsDifferent = false;
        // if the property value has been changed before check
        // to see if it now agrees with the original value. if it
        // does check to see if other differences exist
        String eventProp = theEvent.getPropertyName();
        if (propValueMap.containsKey(eventProp)) {
            Object original = propValueMap.get(eventProp);
            Object current = theEvent.getNewValue();
            propsDifferent = !equivalent(original, current);

            // cycle through and see if any values are different now
            if (!propsDifferent) {
                Iterator itr = propValueMap.keySet().iterator();
                while (itr.hasNext()) {
                    String prop = (String)itr.next();
                    if (!prop.equals(eventProp)) {
                        original = propValueMap.get(prop);
                        PropertyDefinition def = (PropertyDefinition)propDefsMap.get(prop);
                        current = propEditor.getValue(propObj, def);
                        propsDifferent = !equivalent(original, current);
                        if (propsDifferent) {
                            break;
                        }
                    }
                }
            }
        } else {
            // save original value if not previously saved
            // propValueMap contains properties that have changed at one time
            // they may now hold the original value however
            propsDifferent = true;
            propValueMap.put(eventProp, theEvent.getOldValue());
        }
        checkResetState();
    }
    
    private boolean equivalent(
       Object theValue,
       Object theOtherValue) {

       return (((theValue == null) && (theOtherValue == null)) ||
               ((theValue != null) && (theOtherValue != null) &&
               theValue.equals(theOtherValue)));
   }
    
    

    public void itemStateChanged(ItemEvent theEvent) {
        if (processEvents) {
            JComboBox pscEditor = (JComboBox)theEvent.getSource();
            int row = tblSvcs.getSelectedRow();
            if ((theEvent.getStateChange() == ItemEvent.SELECTED) &&
                (row != -1) &&
                (pscEditor.getSelectedIndex() != -1)) {

                Object vm = pscEditor.getSelectedItem();
                tblModel.setValueAt(vm, row, DeployPkgUtils.SERV_COL);
                Object saveValue = prodRowMap.get(tblModel.getValueAt(row, DeployPkgUtils.ENABLED_COL));
                boolean diff = false;
                if (!vm.equals(saveValue)) {
                    diff = true;
                }

                checkResetState();
            }
        }
    }

    public void persist()
        throws ExternalException {
        
        if (propsDifferent) {
            StaticUtilities.displayModalDialogWithOK("Modify Process Properties", "Note change will not take effect until Process is restarted in the Runtime panel."); //$NON-NLS-1$ //$NON-NLS-2$
            getConfigurationManager().modifyPropertiedObject(propEditor);
            propValueMap.clear();
            propsDifferent = false;
        }
        checkResetState();
        
        checkResetState();
    }


    public void reset() {
        
        if (propsDifferent) {
            resetPropertiedObject();
        }
        
        propsDifferent = false;
        checkResetState();
    }
    

    private void resetPropertiedObject() {
        propsDifferent = false;
        Iterator itr = propValueMap.keySet().iterator();
        while (itr.hasNext()) {
            String prop = (String)itr.next();
            PropertyDefinition def = (PropertyDefinition)propDefsMap.get(prop);
            propEditor.setValue(propObj, def, propValueMap.get(prop));
        }
        pnlProps.refreshDisplay();
        propValueMap.clear();
    }
    
    

    public void setConfigId(ConfigurationID theConfigId) {
        super.setConfigId(theConfigId);
        //This needs checking out.  Changed by dropping oper. config.  BWP 11/08/02:
   //     setMMLEnabled(true);
    }

    public void setDomainObject(
        Object theDomainObject,
        Object[] theAncestors) {

        try {
            if (theDomainObject == null) {
                process = getConfigurationManager().createProcess(
                		getString("drp.newprocess"), null, (Host)theAncestors[0], //$NON-NLS-1$
                		getConfigId());
            }
            else {
                if (theDomainObject instanceof VMComponentDefn) {
                    process = (VMComponentDefn)theDomainObject;
                    processName = process.getName();
                }
                else {
                    throw new IllegalArgumentException(
                        getString("msg.invalidclass", //$NON-NLS-1$
                                  new Object[] {"VMComponentDefn", //$NON-NLS-1$
                                                theDomainObject.getClass()}));
                }
            }

            super.setDomainObject(process, theAncestors);
            
            

            // save values that can change
            //Note 1:saveProcess = process.getName();

            // get heap sizes and log file name or set to defaults
            if (propEditor == null) {
                propEditor = getConfigurationManager().getPropertiedObjectEditor();
            }
            propDefsMap.clear();
            propValueMap.clear();

            propObj = getConfigurationManager()
                .getPropertiedObjectForComponentObject(process);
            pnlProps.setNameColumnHeaderWidth(0);
            pnlProps.setPropertiedObject(propObj);

            pnlProps.resizeNameColumn();
            savePropertyDefinitions();
            
            // set gui components
            setTitleSuffix(processName);

            Host host = (Host)theAncestors[0];
            txfHost.setText(host.getName());


            // first get product pscs and set each selected psc to none
            initTable();
            // populate PSC column
            prodRowMap.clear();
            tblSvcs.sizeColumnsToFitData();
        }
        catch (Exception theException) {
            ExceptionUtility.showMessage(
                getString("msg.configmgrproblem", //$NON-NLS-1$
                          new Object[] {getClass(), "setDomainObject"}), //$NON-NLS-1$
                ""+theException.getMessage(), //$NON-NLS-1$
                theException);
            LogManager.logError(LogContexts.PSCDEPLOY,
                                theException,
                                getClass() + ":setDomainObject"); //$NON-NLS-1$
        }
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

    public void setEnabled(boolean theEnableFlag) {
        if (isEnabled() != theEnableFlag) {
            super.setEnabled(theEnableFlag);
            pnlProps.setReadOnlyForced(!theEnableFlag);
            pnlProps.refreshDisplay();
            
            
            actionDelete.setEnabled(theEnableFlag);
            tblSvcs.setColumnEditable(DeployPkgUtils.ENABLED_COL, theEnableFlag);
            if (!theEnableFlag) {
                tblSvcs.editingStopped(new ChangeEvent(this));
            }
        }
    }


    ///////////////////////////////////////////////////////////////////////////
    // INNER CLASSES
    ///////////////////////////////////////////////////////////////////////////

    private class PanelAction extends AbstractPanelAction {
        public static final int APPLY = 0;
        public static final int DELETE = 1;
        public static final int RESET = 2;

        public PanelAction(int theType) {
            super(theType);
            if (theType == APPLY) {
                putValue(NAME, getString("drp.actionApply")); //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION, getString("drp.actionApply.tip")); //$NON-NLS-1$
                setMnemonic(getMnemonicChar("drp.actionApply.mnemonic")); //$NON-NLS-1$
            }
            else if (theType == DELETE) {
                putValue(NAME, getString("drp.actionDelete")); //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION, getString("drp.actionDelete.tip")); //$NON-NLS-1$
                setMnemonic(getMnemonicChar("drp.actionDelete.mnemonic")); //$NON-NLS-1$
            }
            else if (theType == RESET) {
                putValue(NAME, getString("drp.actionReset")); //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION, getString("drp.actionReset.tip")); //$NON-NLS-1$
                setMnemonic(getMnemonicChar("drp.actionReset.mnemonic")); //$NON-NLS-1$
            }
            else {
                throw new IllegalArgumentException(
                    getString("msg.invalidactiontype", new Object[] {""+theType})); //$NON-NLS-1$ //$NON-NLS-2$
            }
        }
        protected void actionImpl(ActionEvent theEvent)
            throws ExternalException {

            if (type == APPLY) {
                persist();
            }
            else if (type == DELETE) {
                delete();
            }
            else if (type == RESET) {
                reset();
            }
        }
        public String toString() {
            if (type == APPLY) {
                return getString("drp.actionApply.msg", //$NON-NLS-1$
                                 new Object[] {processName});
            }
            else if (type == DELETE) {
                return getString("drp.actionDelete.msg", //$NON-NLS-1$
                                 new Object[] {processName});
            }
            else if (type == RESET) {
                return getString("drp.actionReset.msg", //$NON-NLS-1$
                                 new Object[] {processName});
            }
            return null;
        }
    }

    
    class ProcessPOP extends PropertiedObjectPanel {
        public ProcessPOP(PropertiedObjectEditor poe) {
            super(poe, getEncryptor());
        }

        public void resizeNameColumn() {
            super.resizeNameColumn();
        }
    }//end ServiceDefPOP    
}

