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
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.border.CompoundBorder;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.table.TableColumn;

import com.metamatrix.admin.api.server.ServerAdmin;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnType;
import com.metamatrix.common.config.model.BasicHost;
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
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;
import com.metamatrix.toolbox.ui.widget.property.PropertiedObjectPanel;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumnModel;

/**
 * @version 1.0
 * @author Dan Florian
 */
public final class DeployedHostPanel
    extends DetailPanel
    implements ConfigurationModifier,
               DocumentListener,
               PropertyChangeListener,
               PropertyConstants {

/******** DESIGN NOTES

- Note 1: the host name was originally going to be editable.
  The server-side code has not been written to allow this. The GUI code
  that allowed the name to be changed is commented out in case we want
  to do that in the future. dan 8-30-01

********* END DESIGN NOTES */

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    private static /*final*/ String[] PROC_HDRS;
    private static final int PROC_COL = 0;
    private static final int MIN_HEAP_COL = 1;
    private static final int MAX_HEAP_COL = 2;
    private static final int LOG_COL = 3;

    ///////////////////////////////////////////////////////////////////////////
    // INITIALIZER
    ///////////////////////////////////////////////////////////////////////////

    static {
        PROC_HDRS = new String[4];
        PROC_HDRS[PROC_COL] = DeployPkgUtils.getString("dhp.process.hdr"); //$NON-NLS-1$
        PROC_HDRS[MIN_HEAP_COL] = DeployPkgUtils.getString("dhp.minheap.hdr"); //$NON-NLS-1$
        PROC_HDRS[MAX_HEAP_COL] = DeployPkgUtils.getString("dhp.maxheap.hdr"); //$NON-NLS-1$
        PROC_HDRS[LOG_COL] = DeployPkgUtils.getString("dhp.vmport.hdr");//$NON-NLS-1$
    }

    ///////////////////////////////////////////////////////////////////////////
    // CONTROLS
    ///////////////////////////////////////////////////////////////////////////

    private TableWidget tblProcs;

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private ProcessPOP pnlProps;
    private JPanel pnlPropsOuter;    
    
    private PanelAction actionApply;
    private PanelAction actionNew;
    private PanelAction actionDelete;
    private PanelAction actionReset;
    private Host host;
    private DefaultTableModel procTblModel;
    private ArrayList treeActions;

    // the next default assignment of the vm socket port
    private int nextVMPort = 31000;
    
    private DeployMainPanel mainPanel;
    
    private boolean propsDifferent = false;
    private HashMap propValueMap = new HashMap();   
    private HashMap propDefsMap = new HashMap();    

    private ConfigurationPropertiedObjectEditor propEditor;
    private PropertiedObject propObj;

    
    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public DeployedHostPanel(ConnectionInfo connInfo) {
        super(connInfo);
        setTitle(getString("dhp.title")); //$NON-NLS-1$
    }

    public DeployedHostPanel(DeployMainPanel mainPanel, ConfigurationID theConfigId,
                             ConnectionInfo connInfo)
        throws ExternalException {

        this(connInfo);
        this.mainPanel = mainPanel;
        setConfigId(theConfigId);
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    public void changedUpdate(DocumentEvent theEvent) {        
        checkResetState();
    }
    public void checkResetState() {
        if (isPropertiesValid() &&  propsDifferent) {
            if (!actionApply.isEnabled()) {
                actionApply.setEnabled(true);
                actionReset.setEnabled(true);
            }
        }
        else {
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
        treeActions = new ArrayList();
        actionApply = new PanelAction(PanelAction.APPLY);
        actionApply.setEnabled(false);
        actionNew = new PanelAction(PanelAction.NEW);
        //Adding "New Process..." action here adds it a second time, so 
        //commenting out.  BWP 09/10/02
        //treeActions.add(actionNew);
        actionDelete = new PanelAction(PanelAction.DELETE);
        actionReset = new PanelAction(PanelAction.RESET);
        actionReset.setEnabled(false);

        JPanel pnl = new JPanel(new GridBagLayout());

        GridBagConstraints gbc = new GridBagConstraints();
       
       
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
        
        
        
        JPanel pnlProcs = new JPanel(new GridLayout(1, 1));
        TitledBorder tBorder = new TitledBorder(getString("dhp.pnlProcs.title")); //$NON-NLS-1$
        pnlProcs.setBorder(
            new CompoundBorder(tBorder,
                               DeployPkgUtils.EMPTY_BORDER));
        gbc.gridx = 0;
        gbc.gridy = 2;
        gbc.gridwidth = GridBagConstraints.REMAINDER;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weightx = 0.6;
        gbc.weighty = 0.6;
//        gbc.insets = new Insets(1, 1, 5, 1);         
        pnl.add(pnlProcs, gbc);
        
                
        

        tblProcs = new TableWidget();
        procTblModel =
            DeployPkgUtils.setup(
                tblProcs,
                PROC_HDRS,
                DeployPkgUtils.getInt("dhp.procstblrows", 5), //$NON-NLS-1$
                null);
        procTblModel = (DefaultTableModel)tblProcs.getModel();
        tblProcs.setComparator(new DeployTableSorter());

        JScrollPane spnProcs = new JScrollPane(tblProcs);
        pnlProcs.add(spnProcs);


        JPanel pnlOps = new JPanel();
        gbc.gridx = 0;
        gbc.gridy = 4;
        gbc.anchor = GridBagConstraints.CENTER;
        gbc.gridwidth = GridBagConstraints.REMAINDER;
        gbc.fill = GridBagConstraints.NONE;
        gbc.weightx = 0.0;
        gbc.weighty = 0.0;
        pnl.add(pnlOps, gbc);

        JPanel pnlOpsSizer = new JPanel(new GridLayout(1, 4, 10, 0));
        pnlOps.add(pnlOpsSizer);

        ButtonWidget btnApply = new ButtonWidget();
        setup(MenuEntry.ACTION_MENUITEM, btnApply, actionApply);
        pnlOpsSizer.add(btnApply);

        ButtonWidget btnNew = new ButtonWidget();
        setup(MenuEntry.ACTION_MENUITEM, btnNew, actionNew);
        pnlOpsSizer.add(btnNew);

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
    
    private void delete()
        throws ExternalException {
        // show dialog to save/abort changes
        Object[] hostName = {host.getName()};
        ConfirmationPanel pnlConfirm =
            new ConfirmationPanel("dhp.msg.confirmdelete", hostName); //$NON-NLS-1$
        DialogWindow.show(
            this,
            DeployPkgUtils.getString("dhp.confirmdelete.title", hostName), //$NON-NLS-1$
            pnlConfirm);
        if (pnlConfirm.isConfirmed()) {
            getConfigurationManager().deleteHost(host, getConfigId());
            this.mainPanel.refresh();
            checkResetState();
        }
    }

    public String getHostName() {
        String str = ""; //$NON-NLS-1$
        if (host != null) {
            str = host.toString();
        }
        return str;
    }

    public List getTreeActions() {
        return treeActions;
    }

    public void insertUpdate(DocumentEvent theEvent) {
        changedUpdate(theEvent);
    }

    public boolean isPersisted() {
        return !actionApply.isEnabled();
    }

    private void newProcess()
        throws ExternalException {

        // show dialog asking for new process name
        CreatePanel pnl = new CreatePanel("dhp.msg.createprocess", //$NON-NLS-1$
                                          "icon.process.big", //$NON-NLS-1$
                                          "dhp.lblnewprocess", //$NON-NLS-1$
                                          "processname"); //$NON-NLS-1$
        DialogWindow.show(
            this,
            DeployPkgUtils.getString("dhp.newprocessdlg.title"), //$NON-NLS-1$
            pnl);
        if (pnl.isConfirmed()) {
            // get the process name from the panel
            String processName = pnl.getName();
			getConfigurationManager().createProcess(processName, Integer.toString(nextVMPort),  host, 
					getConfigId());
        }
    }

    public void persist()
        throws ExternalException {
        
        if (propsDifferent) {
            StaticUtilities.displayModalDialogWithOK("Modify Process Properties", "Note change will not take effect until Host is restarted."); //$NON-NLS-1$ //$NON-NLS-2$
            getConfigurationManager().modifyPropertiedObject(propEditor);
            propValueMap.clear();
            propsDifferent = false;
        }
        
        checkResetState();
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
    

    public void removeUpdate(DocumentEvent theEvent) {
        changedUpdate(theEvent);
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
        
    private void sortFirstColumnInTable(TableWidget twidget)
    {
        // Connector Binding Table
        EnhancedTableColumnModel etcmTabelCM
            = twidget.getEnhancedColumnModel();
        TableColumn tColumn = etcmTabelCM.getColumn(0);
        etcmTabelCM.setColumnSortedAscending((EnhancedTableColumn)tColumn, false);
    }

    public void setDomainObject(
        Object theDomainObject,
        Object[] theAncestors) {
        try {
            if (theDomainObject == null) {
                // this shouldn't happen since new hosts are created elsewhere
                host = getConfigurationManager().createHost(getString("dhp.newhost")); //$NON-NLS-1$
            }
            else {
                if (theDomainObject instanceof Host) {
                    host = (Host)theDomainObject;
                    ServerAdmin admin = getConnectionInfo().getServerAdmin();
                    Collection<com.metamatrix.admin.api.objects.Host> adminHosts = admin.getHosts(host.getFullName());
                    for (com.metamatrix.admin.api.objects.Host adminHost:adminHosts) {
                    	((BasicHost)host).setProperties(adminHost.getProperties());
                    }
                }
                else {
                    throw new IllegalArgumentException(getString("msg.invalidclass", new Object[] {"Host", theDomainObject.getClass()})); //$NON-NLS-1$ //$NON-NLS-2$
                }
            }

            super.setDomainObject(host, theAncestors);

            setTitleSuffix(host.getName());
            
            if (propEditor == null) {
                propEditor = getConfigurationManager().getPropertiedObjectEditor();
            }
            propDefsMap.clear();
            propValueMap.clear();
            propObj = getConfigurationManager()
                    .getPropertiedObjectForComponentObject(host);

            pnlProps.setNameColumnHeaderWidth(0);
            pnlProps.setPropertiedObject(propObj);

            pnlProps.resizeNameColumn();
            savePropertyDefinitions();
            

            ConfigurationID configId = getConfigId();

            // populate tables
            procTblModel.setNumRows(0);

            Collection procs = getConfigurationManager().getConfig(configId).getVMsForHost((HostID)host.getID());
            String port = null;
            if (procs != null) {
                Iterator procItr = procs.iterator();
                while (procItr.hasNext()) {
                    // populate process table
                    VMComponentDefn process = (VMComponentDefn)procItr.next();
                    Vector processRow = new Vector(PROC_HDRS.length);
                    processRow.setSize(PROC_HDRS.length);
                    processRow.setElementAt(process, PROC_COL);
                    processRow.setElementAt(process.getProperty(VMComponentDefnType.VM_MINIMUM_HEAP_SIZE_PROPERTY_NAME),
                                            MIN_HEAP_COL);
                    processRow.setElementAt(process.getProperty(VMComponentDefnType.VM_MAXIMUM_HEAP_SIZE_PROPERTY_NAME),
                                            MAX_HEAP_COL);

                    port = process.getPort();
                    setNextVMPort(port);
                    processRow.setElementAt(port,
                                            LOG_COL);
                    procTblModel.addRow(processRow);

                }
            }
            tblProcs.sizeColumnsToFitData();
            sortFirstColumnInTable(tblProcs);
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
    
    
    private void setNextVMPort(String port) {
        int p;
        if (port == null || port.length() == 0) {
            p = nextVMPort;
        } else {
            p = Integer.parseInt(port);
        }
        
        if (p < nextVMPort) {
            return;
        } else if (p > nextVMPort) {
            nextVMPort = p + 1;
        } else {
            ++nextVMPort;
        }
    }

    public void setEnabled(boolean theEnableFlag) {
        actionNew.setEnabled(theEnableFlag);
        
        
        Collection hosts = getConfigurationManager().getHosts(this.getConfigId());
        if (hosts != null && hosts.size() > 1) {

            actionDelete.setEnabled(theEnableFlag);
        } else {
            actionDelete.setEnabled(false);
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // INNER CLASSES
    ///////////////////////////////////////////////////////////////////////////

    private class PanelAction extends AbstractPanelAction {
        public static final int APPLY = 0;
        public static final int NEW = 1;
        public static final int DELETE = 2;
        public static final int RESET = 3;

        public PanelAction(int theType) {
            super(theType);
            if (theType == APPLY) {
                putValue(NAME, getString("dhp.actionApply")); //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION, getString("dhp.actionApply.tip")); //$NON-NLS-1$
                setMnemonic(getMnemonicChar("dhp.actionApply.mnemonic")); //$NON-NLS-1$
            }
            else if (theType == NEW) {
                putValue(NAME, getString("dhp.actionNew")); //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION, getString("dhp.actionNew.tip")); //$NON-NLS-1$
                setMnemonic(getMnemonicChar("dhp.actionNew.mnemonic")); //$NON-NLS-1$
            }
            else if (theType == DELETE) {
                putValue(NAME, getString("dhp.actionDelete")); //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION, getString("dhp.actionDelete.tip")); //$NON-NLS-1$
                setMnemonic(getMnemonicChar("dhp.actionDelete.mnemonic")); //$NON-NLS-1$
            }
            else if (theType == RESET) {
                putValue(NAME, getString("dhp.actionReset")); //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION, getString("dhp.actionReset.tip")); //$NON-NLS-1$
                setMnemonic(getMnemonicChar("dhp.actionReset.mnemonic")); //$NON-NLS-1$
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
            else if (type == NEW) {
                newProcess();
            }
            else if (type == DELETE) {
                delete();
            }
            else if (type == RESET) {
                reset();
            }
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
