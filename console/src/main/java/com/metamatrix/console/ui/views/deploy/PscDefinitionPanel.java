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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.SwingUtilities;
import javax.swing.border.CompoundBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
import javax.swing.table.TableCellEditor;
import javax.swing.table.TableColumn;

import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ProductServiceConfig;
import com.metamatrix.common.config.api.ProductServiceConfigID;
import com.metamatrix.common.config.api.ProductType;
import com.metamatrix.common.config.api.ProductTypeID;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.NotifyOnExitConsole;
import com.metamatrix.console.ui.layout.MenuEntry;
import com.metamatrix.console.ui.util.AbstractPanelAction;
import com.metamatrix.console.ui.views.deploy.event.ConfigurationModifier;
import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;
import com.metamatrix.console.ui.views.deploy.util.DeployTableSorter;
import com.metamatrix.console.ui.views.deploy.util.PropertyConstants;
import com.metamatrix.console.util.DialogUtility;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.core.util.MetaMatrixProductVersion;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.CheckBox;
import com.metamatrix.toolbox.ui.widget.DialogWindow;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.Splitter;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumnModel;

/**
 * @version 1.0
 * @author Dan Florian
 */
public final class PscDefinitionPanel
    extends DetailPanel
    implements ActionListener,
               ConfigurationModifier,
               NotifyOnExitConsole,
               //Note 1:DocumentListener,
               ListSelectionListener,
               PropertyConstants,
               TableModelListener {


    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    private static /*final*/ String[] SERVICE_HDRS;
    private static final int SERV_COL = 0;
    private static final int ENABLED_COL = 1;
    private static final int ESSENTIAL_COL = 2;
    public static /*final*/ SimpleDateFormat DATE_FORMATTER;

    ///////////////////////////////////////////////////////////////////////////
    // INITIALIZER
    ///////////////////////////////////////////////////////////////////////////

    static {
        SERVICE_HDRS = new String[3];
        SERVICE_HDRS[SERV_COL] = DeployPkgUtils.getString("pfp.service.hdr"); //$NON-NLS-1$
        SERVICE_HDRS[ENABLED_COL] = DeployPkgUtils.getString("pfp.enabled.hdr"); //$NON-NLS-1$
        SERVICE_HDRS[ESSENTIAL_COL] = DeployPkgUtils.getString("pfp.essential.hdr"); //$NON-NLS-1$

        String pattern = DeployPkgUtils.getString("pfp.datepattern", true); //$NON-NLS-1$
        if (pattern == null) {
            pattern = "MMM dd, yyyy hh:mm:ss"; //$NON-NLS-1$
        }
        DATE_FORMATTER = new SimpleDateFormat(pattern);
    }

    ///////////////////////////////////////////////////////////////////////////
    // CONTROLS
    ///////////////////////////////////////////////////////////////////////////

    private TableWidget tblServices;
    private TextFieldWidget txfProd;
//    private TextFieldWidget txfPsc;
    private TextFieldWidget txfCreated;
    private TextFieldWidget txfCreatedBy;
    private TextFieldWidget txfModified;
    private TextFieldWidget txfModifiedBy;
    private ServiceDefinitionPanel sdp;
    private DeployMainPanel dmp;

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private PanelAction actionApply;
    private PanelAction actionCopy;
    
    private PanelAction actionNew;
    private PanelAction actionAssign;
    
    private PanelAction actionDelete;
    private PanelAction actionReset;
    private ProductServiceConfig pscDef;
    private DefaultTableModel tblModel;
    private ArrayList treeActions;

    private HashMap saveServEnabledMap = new HashMap();
    private int numRowsDifferent = 0;
    private CheckBox chk; // the table cell editor component for the enabled col
    private boolean programmaticTableSelectionChange = false;
    private Map /*<ProductServiceConfig to SelectedServiceInfo>*/
            pscToSelectedServiceMap = new HashMap();
    private boolean deleting = false;
	
    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public PscDefinitionPanel(ConnectionInfo connInfo, DeployMainPanel mainPanel) {
        super(connInfo);
        dmp=mainPanel;
        setTitle(getString("pfp.title")); //$NON-NLS-1$
    }

    public PscDefinitionPanel(ConfigurationID theConfigId,
                              ConnectionInfo connInfo, DeployMainPanel mainPanel) throws ExternalException {
		this(connInfo, mainPanel);
        setConfigId(theConfigId);
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    public void actionPerformed(ActionEvent theEvent) {
        tblModel.setValueAt(new Boolean(chk.isSelected()),
                           tblServices.convertRowIndexToModel(
                               tblServices.getSelectedRow()),
                           ENABLED_COL);
        tblServices.editingCanceled(new ChangeEvent(this));
    }

    public void checkResetState() {
        if ((numRowsDifferent > 0) || sdp.propertiesHaveChanged()) {
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

    protected JPanel construct(boolean readOnly) {
        // setup actions first
        treeActions = new ArrayList();
        actionApply = new PanelAction(PanelAction.APPLY);
        actionApply.setEnabled(false);
        actionCopy = new PanelAction(PanelAction.COPY);
        //Adding action "Copy PSC..." adds it a second time, so commenting out.
        //BWP 09/10/02
        //treeActions.add(actionCopy);
        actionNew = new PanelAction(PanelAction.NEW);
        actionAssign = new PanelAction(PanelAction.ASSIGN);
        
        actionDelete = new PanelAction(PanelAction.DELETE);
        actionReset = new PanelAction(PanelAction.RESET);
        actionReset.setEnabled(false);

        JPanel pnl = new JPanel(new GridBagLayout());

        LabelWidget lblProd = DeployPkgUtils.createLabel("pfp.lblProduct"); //$NON-NLS-1$
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(3, 3, 5, 3);
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.anchor = GridBagConstraints.EAST;
        pnl.add(lblProd, gbc);

        txfProd = DeployPkgUtils.createTextField("productname"); //$NON-NLS-1$
        txfProd.setEditable(false);
        gbc.gridx = 1;
        gbc.gridy = 0;
        gbc.anchor = GridBagConstraints.WEST;
        pnl.add(txfProd, gbc);

//        LabelWidget lblPsc = DeployPkgUtils.createLabel("pfp.lblPsc"); //$NON-NLS-1$
//        gbc.gridx = 2;
//        gbc.gridy = 0;
//        gbc.anchor = GridBagConstraints.EAST;
//        pnl.add(lblPsc, gbc);
//
//        txfPsc = DeployPkgUtils.createTextField("pscname"); //$NON-NLS-1$
//        txfPsc.setEditable(false); //Note 1
//        gbc.gridx = 3;
//        gbc.gridy = 0;
//        gbc.anchor = GridBagConstraints.WEST;
//        pnl.add(txfPsc, gbc);

        LabelWidget lblCreated = DeployPkgUtils.createLabel("pfp.lblCreated"); //$NON-NLS-1$
        gbc.gridx = 0;
        gbc.gridy = 1;
        gbc.insets = new Insets(3, 3, 3, 3);
        gbc.anchor = GridBagConstraints.EAST;
        pnl.add(lblCreated, gbc);

        txfCreated = DeployPkgUtils.createTextField("timestamp"); //$NON-NLS-1$
        txfCreated.setEditable(false);
        gbc.gridx = 1;
        gbc.gridy = 1;
        gbc.anchor = GridBagConstraints.WEST;
        pnl.add(txfCreated, gbc);

        LabelWidget lblCreatedBy = DeployPkgUtils.createLabel("pfp.lblCreatedBy"); //$NON-NLS-1$
        gbc.gridx = 2;
        gbc.gridy = 1;
        gbc.anchor = GridBagConstraints.EAST;
        pnl.add(lblCreatedBy, gbc);

        txfCreatedBy = DeployPkgUtils.createTextField("username"); //$NON-NLS-1$
        txfCreatedBy.setEditable(false);
        gbc.gridx = 3;
        gbc.gridy = 1;
        gbc.anchor = GridBagConstraints.WEST;
        pnl.add(txfCreatedBy, gbc);

        LabelWidget lblModified = DeployPkgUtils.createLabel("pfp.lblModified"); //$NON-NLS-1$
        gbc.gridx = 0;
        gbc.gridy = 2;
        gbc.anchor = GridBagConstraints.EAST;
        pnl.add(lblModified, gbc);

        txfModified = DeployPkgUtils.createTextField("timestamp"); //$NON-NLS-1$
        txfModified.setEditable(false);
        gbc.gridx = 1;
        gbc.gridy = 2;
        gbc.anchor = GridBagConstraints.WEST;
        pnl.add(txfModified, gbc);

        LabelWidget lblModifiedBy = DeployPkgUtils.createLabel("pfp.lblModifiedBy"); //$NON-NLS-1$
        gbc.gridx = 2;
        gbc.gridy = 2;
        gbc.anchor = GridBagConstraints.EAST;
        pnl.add(lblModifiedBy, gbc);

        txfModifiedBy = DeployPkgUtils.createTextField("username"); //$NON-NLS-1$
        txfModifiedBy.setEditable(false);
        gbc.gridx = 3;
        gbc.gridy = 2;
        gbc.anchor = GridBagConstraints.WEST;
        pnl.add(txfModifiedBy, gbc);

        JPanel pnlServices = new JPanel(new GridLayout(1, 1));
        TitledBorder tBorder;
        tBorder = new TitledBorder(getString("pfp.pnlServices.title")); //$NON-NLS-1$
        pnlServices.setBorder(
            new CompoundBorder(tBorder, DeployPkgUtils.EMPTY_BORDER));


        gbc.gridx = 0;
        gbc.gridy = 3;
        gbc.gridwidth = GridBagConstraints.REMAINDER;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.insets = new Insets(3, 3, 5, 3);
        gbc.weightx = 1.0;
        gbc.weighty = 1.0;
        pnl.add(pnlServices, gbc);

        tblServices = new TableWidget();
        tblModel =
            DeployPkgUtils.setup(
                tblServices,
                SERVICE_HDRS,
                DeployPkgUtils.getInt("pfp.servicestblrows", 10), //$NON-NLS-1$
                new int[] {ENABLED_COL});
        tblServices.getSelectionModel().addListSelectionListener(this);
        tblServices.getSelectionModel().addListSelectionListener(
        		new ListSelectionListener() {
            public void valueChanged(ListSelectionEvent ev) {
                listSelectionChanged();
            }
        });
        tblServices.setComparator(new DeployTableSorter());
        tblModel.addTableModelListener(this);

        JScrollPane spnServices = new JScrollPane(tblServices);
		
		sdp = new ServiceDefinitionPanel(false, this, getConfigId(), getConnectionInfo());
        

        final JSplitPane splitPane = new Splitter(JSplitPane.VERTICAL_SPLIT, 
        		true, spnServices, sdp);
        pnlServices.add(splitPane);
        SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                splitPane.setDividerLocation(0.5);
            }
        });

        JPanel pnlOps = new JPanel();
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 4;
        gbc.gridwidth = GridBagConstraints.REMAINDER;
        gbc.insets = new Insets(3, 3, 3, 3);
        pnl.add(pnlOps, gbc);

        JPanel pnlOpsSizer = new JPanel(new GridLayout(1, 4, 10, 0));
        pnlOps.add(pnlOpsSizer);

        ButtonWidget btnApply = new ButtonWidget();
        setup(MenuEntry.ACTION_MENUITEM, btnApply, actionApply);
        pnlOpsSizer.add(btnApply);
        
        ButtonWidget btnNew = new ButtonWidget();
        setup(MenuEntry.ACTION_MENUITEM, btnNew, actionNew);
        pnlOpsSizer.add(btnNew);


        ButtonWidget btnAssign = new ButtonWidget();
        setup(MenuEntry.ACTION_MENUITEM, btnAssign, actionAssign);
        pnlOpsSizer.add(btnAssign);
        
        ButtonWidget btnCopy = new ButtonWidget();
        setup(MenuEntry.ACTION_MENUITEM, btnCopy, actionCopy);
        pnlOpsSizer.add(btnCopy);

        ButtonWidget btnDelete = new ButtonWidget();
        setup(MenuEntry.ACTION_MENUITEM, btnDelete, actionDelete);
        pnlOpsSizer.add(btnDelete);

        ButtonWidget btnReset = new ButtonWidget();
        setup(MenuEntry.ACTION_MENUITEM, btnReset, actionReset);
        pnlOpsSizer.add(btnReset);
        
        

        return pnl;
    }

    private void copy()
        throws ExternalException {
        // show dialog asking for new process name
        CreatePanel pnl = new CreatePanel("pfp.msg.createpscdef", //$NON-NLS-1$
                                          "icon.psc.big", //$NON-NLS-1$
                                          "pfp.lblnewpscdef", //$NON-NLS-1$
                                          "pscname");                                                                                   //$NON-NLS-1$
                                          
        DialogWindow.show(
            this,
            DeployPkgUtils.getString("pfp.newpscdefdlg.title"), //$NON-NLS-1$
            pnl);
        if (pnl.isConfirmed()) {
            // get the process name from the panel
//            Object[] ancestors =
                getAncestors();
            String pscDefName = pnl.getName();
//            ProductServiceConfig newPscDef = 
                getConfigurationManager().copyPscDef(pscDefName, pscDef, 
                		getConfigId());
        }
    }
    
    private void newPSC() 
    	throws ExternalException {
		Configuration config = this.getConfigurationManager().getConfig(Configuration.NEXT_STARTUP_ID);
		
        List pscServiceNames = null;
        if (pscDef.getComponentTypeID().getName().equals(MetaMatrixProductVersion.CONNECTOR_PRODUCT_TYPE_NAME)) {

            Collection bindings = config.getConnectorBindings();

            // get all the names of the available bindings to choose from
            if (bindings != null) {
                                        
                pscServiceNames = new ArrayList(bindings.size());
                Iterator itCbs = bindings.iterator();
                while (itCbs.hasNext()) {
                    ConnectorBinding cb = (ConnectorBinding) itCbs.next();
                    pscServiceNames.add(cb.getID());                                                    
                }
            } else {
               
                pscServiceNames = Collections.EMPTY_LIST;
            }           
            
        } else {
            Collection d = config.getServiceComponentDefns();
            // get all the names of the available bindings to choose from
            if (d != null) {                        
                pscServiceNames = new ArrayList(d.size());
                Iterator itIDs = d.iterator();
                while (itIDs.hasNext()) {
                    ServiceComponentDefn cd = (ServiceComponentDefn) itIDs.next();
                    ServiceComponentDefnID id = (ServiceComponentDefnID) cd.getID();
                    pscServiceNames.add(id);                                                    
                }
            } else {
                pscServiceNames = Collections.EMPTY_LIST;
            }           
            
        }
        CreatePSCPanel pnl = new CreatePSCPanel("pfp.msg.createpscdef", //$NON-NLS-1$
                                          "icon.psc.big", //$NON-NLS-1$
                                          "pfp.lblnewpscdef", //$NON-NLS-1$
                                          "pscname", //$NON-NLS-1$
                                          pscDef,
                                          pscServiceNames);

                                          
        DialogWindow.show(
            this,
            DeployPkgUtils.getString("pfp.newpscdefdlg.title"), //$NON-NLS-1$
            pnl);
        if (pnl.isConfirmed()) {
            // get the process name from the panel
            String pscDefName = pnl.getName();
            List services = pnl.getSelectedServices();
            
           getConfigurationManager().createPscDef(pscDefName, (ProductTypeID) pscDef.getComponentTypeID(), 
                    services,
                        getConfigId());
        }
            
    }
    
    private void assign() 
        throws ExternalException {
        Configuration config = this.getConfigurationManager().getConfig(Configuration.NEXT_STARTUP_ID);
        
        List pscServiceNames = null;
        if (pscDef.getComponentTypeID().getName().equals(MetaMatrixProductVersion.CONNECTOR_PRODUCT_TYPE_NAME)) {
//          System.out.println("NEW PSC - use bindings");

            Collection bindings = config.getConnectorBindings();

            // get all the names of the available bindings to choose from
            if (bindings != null) {
//              System.out.println("NEW PSC - bindings found " + bindings.size());
                                        
                pscServiceNames = new ArrayList(bindings.size());
                Iterator itCbs = bindings.iterator();
                while (itCbs.hasNext()) {
                    ConnectorBinding cb = (ConnectorBinding) itCbs.next();
                    pscServiceNames.add(cb.getID());                                                    
                }
            } else {
//              System.out.println("NEW PSC - no bindings found ");
                
                pscServiceNames = Collections.EMPTY_LIST;
            }           
            
        } else {
            Collection d = config.getServiceComponentDefns();
            // get all the names of the available bindings to choose from
            if (d != null) {                        
                pscServiceNames = new ArrayList(d.size());
                Iterator itIDs = d.iterator();
                while (itIDs.hasNext()) {
                    ServiceComponentDefn cd = (ServiceComponentDefn) itIDs.next();
                    ServiceComponentDefnID id = (ServiceComponentDefnID) cd.getID();
                    pscServiceNames.add(id);                                                    
                }
            } else {
                pscServiceNames = Collections.EMPTY_LIST;
            }           
            
        }
      
        
        UpdatePSCPanel pnl = new UpdatePSCPanel("pfp.msg.editpscdef", //$NON-NLS-1$
                                          "icon.psc.big", //$NON-NLS-1$
                                          "pfp.lbleditpscdef", //$NON-NLS-1$
                                          "pscname", //$NON-NLS-1$
                                          pscDef,
                                          pscServiceNames,
                                          true);
                                          
        DialogWindow.show(
            this,
            DeployPkgUtils.getString("pfp.editpscdefdlg.title"), //$NON-NLS-1$
            pnl);
        if (pnl.isConfirmed()) {
            // get the process name from the panel
            List services = pnl.getSelectedServices();
            pscDef = getConfigurationManager().updatePscDef(pscDef, services);
            getConfigurationManager().setRefreshNeeded();
            dmp.refresh();

 //           super.setDomainObject(pscDef, this.getAncestors()); 

        }
            
    }
    
    	
   private void delete()
        throws ExternalException {
            
             if (getConfigurationManager().getConfig(Configuration.NEXT_STARTUP_ID).isPSCDeployed((ProductServiceConfigID) pscDef.getID()) )  {
                 String msg = "PSC " + pscDef.getID() + " cannot be deleted until it has be undeployed." //$NON-NLS-1$ //$NON-NLS-2$
                         + "  Please undeploy the PSC and then try again."; //$NON-NLS-1$
                 String hdr = "Deleting PSC"; //$NON-NLS-1$
                 StaticUtilities.displayModalDialogWithOK(hdr, msg); 
                return;
            }        
            

            
        // show dialog to save/abort changes
        ConfirmationPanel pnlConfirm =
            new ConfirmationPanel("pfp.msg.confirmdelete"); //$NON-NLS-1$
        DialogWindow.show(
            this,
            DeployPkgUtils.getString("pfp.confirmdelete.title", //$NON-NLS-1$
                                     new Object[] {pscDef.getName()}),
            pnlConfirm);
        if (pnlConfirm.isConfirmed()) {
        	deleting = true;
            ProductType product = (ProductType)getAncestors()[0];
            getConfigurationManager().deletePscDefinition(pscDef, product, 
            		getConfigId());
            deleting = false;
        }
    }

    public List getTreeActions() {
        return treeActions;
    }

     public boolean havePendingChanges() {
        return (!isPersisted());
    }

    public boolean finishUp() {
        boolean continuing = true;
        if (sdp.propertiesHaveChanged()) {
            continuing = savePropertiesForService();
        }
        if (continuing) {
            if (numRowsDifferent > 0) {
                String msg = "Save changes to PSC " + pscDef.toString() + "?"; //$NON-NLS-1$ //$NON-NLS-2$
                int response = DialogUtility.showPendingChangesDialog(msg,
                		getConfigurationManager().getConnection().getURL(),
                		getConfigurationManager().getConnection().getUser());
                switch (response) {
                    case DialogUtility.YES:
                        try {
                            persist();
                        } catch (ExternalException ex) {
                            String errMsg = "Error saving changes to PSC"; //$NON-NLS-1$
                            LogManager.logError(LogContexts.PSCDEPLOY, ex, errMsg);
                            ExceptionUtility.showMessage(errMsg, ex);
                        }
                        break;
                    case DialogUtility.NO:
                        continuing = true;
                        reset();
                        break;
                    case DialogUtility.CANCEL:
                        continuing = false;
                        break;
                }
            }
        }
        return continuing;
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

    public void persist() throws ExternalException {
        if (sdp.propertiesHaveChanged()) {
            sdp.persist();
        }

        if (numRowsDifferent > 0) {
            //change deployed services here
            for (int rows=tblModel.getRowCount(), i=0; i<rows; i++) {
                Object service = tblModel.getValueAt(i, SERV_COL);
                Boolean saveEnabled = (Boolean)saveServEnabledMap.get(service);
                Boolean enabled = (Boolean)tblModel.getValueAt(i, ENABLED_COL);
                if (saveEnabled.booleanValue() != enabled.booleanValue()) {
                    getConfigurationManager().setEnabled(
                    		(ServiceComponentDefn)service,
                    		pscDef,
                    		enabled.booleanValue(), 
                    		getConfigurationManager().getConfig(getConfigId()));
                    saveServEnabledMap.put(service, enabled);
                }
            }
            numRowsDifferent = 0;
        }
        checkResetState();
    }

    public void reset() {
        if (numRowsDifferent > 0) {
            // reset service enabled state if necessary
            for (int rows=tblModel.getRowCount(), i=0; i<rows; i++) {
                Object saveEnabled =
                    saveServEnabledMap.get(tblModel.getValueAt(i, SERV_COL));
                if (!saveEnabled.equals(tblModel.getValueAt(i, ENABLED_COL))) {
                    tblModel.setValueAt(saveEnabled, i, ENABLED_COL);
                    tblServices.editingCanceled(new ChangeEvent(this));
                }
            }
        }
        numRowsDifferent = 0;
        sdp.reset();
        checkResetState();
    }

    public void setConfigId(ConfigurationID theConfigId) {

        super.setConfigId(theConfigId);
        setTitleSuffix(getString("pfp.title.suffix")); //$NON-NLS-1$
    }

    private void setCreateModifiedFields(ProductServiceConfig thePsc) {
        Date createDate = thePsc.getCreatedDate();
        if (createDate != null) {
            txfCreated.setText(DATE_FORMATTER.format(createDate));
            txfCreatedBy.setText(thePsc.getCreatedBy());
        }

        Date modDate = thePsc.getLastChangedDate();
        if (modDate != null) {
            txfModified.setText(DATE_FORMATTER.format(modDate));
            txfModifiedBy.setText(thePsc.getLastChangedBy());
        }
    }

    private void sortFirstColumnInTable(TableWidget twidget) {
        // Connector Binding Table
        EnhancedTableColumnModel etcmTabelCM
            = twidget.getEnhancedColumnModel();
        TableColumn tColumn = etcmTabelCM.getColumn(0);
        etcmTabelCM.setColumnSortedAscending((EnhancedTableColumn)tColumn, false);
    }

    public void setDomainObject(Object theDomainObject, Object[] theAncestors) {

        if (theDomainObject instanceof ProductServiceConfig) {
            pscDef = (ProductServiceConfig)theDomainObject;
            setTitleSuffix(pscDef.getName());
            SelectedServiceInfo servInfo =
                    (SelectedServiceInfo)pscToSelectedServiceMap.get(pscDef);
            if (servInfo == null) {
                sdp.displayDetailFor(null, null);
            } else {
                sdp.displayDetailFor(servInfo.getService(), servInfo.getAncestors());
            }
        } else {
            throw new IllegalArgumentException(
                getString("msg.invalidclass", //$NON-NLS-1$
                          new Object[] {"ProductServiceConfig", //$NON-NLS-1$
                                        theDomainObject.getClass()}));
        }
        super.setDomainObject(pscDef, theAncestors);

//        String savePsc = pscDef.getName();
//        txfPsc.setText(savePsc);

        setCreateModifiedFields(pscDef);

        // populate table
        saveServEnabledMap.clear();
        tblModel.setNumRows(0);
        try {
            ProductType product = (ProductType)theAncestors[0];
            txfProd.setText(product.getName());

            Configuration config = getConfigurationManager().getConfig(
            		getConfigId());
            Collection services = getConfigurationManager().getServiceDefinitions(
            		pscDef, config);
            if (services != null) {
                Iterator servItr = services.iterator();
                while (servItr.hasNext()) {
                    ServiceComponentDefn service =
                        (ServiceComponentDefn)servItr.next();
                    Vector row = new Vector(SERVICE_HDRS.length);
                    row.setSize(SERVICE_HDRS.length);
                    row.setElementAt(service, SERV_COL);
                    
                    ServiceComponentDefnID svcID = (ServiceComponentDefnID) service.getID();
                    if (!pscDef.containsService(svcID)) {
                    	throw new Exception("Service " + svcID + " not contained in PSC " + pscDef.getName());                    	 //$NON-NLS-1$ //$NON-NLS-2$
                    }
                    Boolean enabled = new Boolean(pscDef.isServiceEnabled( svcID ) );
//service.isEnabled());
                    
                    row.setElementAt(enabled, ENABLED_COL);
                    saveServEnabledMap.put(service, enabled);
                    row.setElementAt(
                        new Boolean(service.getProperty(ESSENTIAL_PROP)),
                        ESSENTIAL_COL);
                    tblModel.addRow(row);
                }
            }
            tblServices.sizeColumnsToFitData();
            sortFirstColumnInTable(tblServices);
        } catch (Exception theException) {
            ExceptionUtility.showMessage(
                getString("msg.configmgrproblem", //$NON-NLS-1$
                          new Object[] {getClass(), "setDomainObject"}), //$NON-NLS-1$
                theException);
            LogManager.logError(
                LogContexts.PSCDEPLOY,
                theException,
                getClass() + ":setDomainObject"); //$NON-NLS-1$
        }

    }

    public void setEnabled(boolean theEnableFlag) {
        actionCopy.setEnabled(theEnableFlag);
        actionNew.setEnabled(theEnableFlag);
        actionAssign.setEnabled(theEnableFlag);
        
        actionDelete.setEnabled(theEnableFlag);
        tblServices.setColumnEditable(PscDefinitionPanel.ENABLED_COL, theEnableFlag);
    }

    public void tableChanged(TableModelEvent theEvent) {
        // should be the only editable column but check anyways
        if (theEvent.getColumn() == ENABLED_COL) {
            int row = theEvent.getFirstRow();
            Object service = tblModel.getValueAt(row, SERV_COL);
            Object saveEnabled = saveServEnabledMap.get(service);
            if (!saveEnabled.equals(tblModel.getValueAt(row, ENABLED_COL))) {
                numRowsDifferent++;
            } else {
                if (numRowsDifferent > 0) {
                    numRowsDifferent--;
                }
            }
            checkResetState();
        }
    }

    public void valueChanged(ListSelectionEvent theEvent) {
        if (!programmaticTableSelectionChange) {
            // done one time to setup the checkbox action listener
            int row = tblServices.getSelectedRow();
            if (row != -1) {
                TableCellEditor editor = tblServices.getCellEditor(row,
                        ENABLED_COL);
                int modelRow = tblServices.convertRowIndexToModel(row);
                chk = (CheckBox)editor.getTableCellEditorComponent(tblServices,
                        tblServices.getValueAt(modelRow, ENABLED_COL), true, row,
                        ENABLED_COL);
                chk.addActionListener(this);
                tblServices.getSelectionModel().removeListSelectionListener(this);
            }
        }
    }

    private void listSelectionChanged() {
        if (!programmaticTableSelectionChange) {
            boolean cancellingChange = false;
            if (sdp.propertiesHaveChanged()) {
                cancellingChange = (!savePropertiesForService());
            }
            if (cancellingChange) {
                int prevModelRow = getModelRowForService(sdp.getService());
                int prevViewRow = tblServices.convertRowIndexToView(prevModelRow);
                programmaticTableSelectionChange = true;
                tblServices.getSelectionModel().setSelectionInterval(prevViewRow,
                        prevViewRow);
                programmaticTableSelectionChange = false;
            } else {
                int row = tblServices.getSelectedRow();
                if (row < 0) {
                    pscToSelectedServiceMap.put(pscDef, null);
                    sdp.displayDetailFor(null, null);
                } else {
                    int modelRow = tblServices.convertRowIndexToModel(row);
                    ServiceComponentDefn serviceDef = getServiceDefForRow(
                            modelRow);
                    Object[] ancestors = getAncestors();
                    pscToSelectedServiceMap.put(pscDef, new SelectedServiceInfo(
                            serviceDef, ancestors));
                    sdp.displayDetailFor(serviceDef, ancestors);
                }
            }
        }
    }

    /**
     * @Return  true if proceeding (with changing selected item or exiting program), false if cancelling
     */
    private boolean savePropertiesForService() {
        boolean cancellingChange = false;
        String msg = "Save changes to properties for service " + //$NON-NLS-1$
                sdp.getService().toString() + "?"; //$NON-NLS-1$
        int response = DialogUtility.showPendingChangesDialog(msg,
        		getConfigurationManager().getConnection().getURL(),
        		getConfigurationManager().getConnection().getUser());
        switch (response) {
            case DialogUtility.YES:
                try {
                    sdp.persist();
                } catch (ExternalException ex) {
                    String errMsg = "Error saving service property changes"; //$NON-NLS-1$
                    LogManager.logError(LogContexts.PSCDEPLOY, ex, errMsg);
                    ExceptionUtility.showMessage(errMsg, ex);
                }
                cancellingChange = false;
                break;
            case DialogUtility.NO:
                cancellingChange = true;
                reset();
                break;
            case DialogUtility.CANCEL:
                cancellingChange = true;
                break;
        }
        return (!cancellingChange);
    }

    private int getModelRowForService(ServiceComponentDefn service) {
        int matchRow = -1;
        int curRow = 0;
        int numRows = tblServices.getRowCount();
        while ((matchRow < 0) && (curRow < numRows)) {
            ServiceComponentDefn curService =
                    (ServiceComponentDefn)tblModel.getValueAt(curRow, SERV_COL);
            if (curService.equals(service)) {
                matchRow = curRow;
            } else {
                curRow++;
            }
        }
        return matchRow;
    }

    private ServiceComponentDefn getServiceDefForRow(int modelRow) {
        ServiceComponentDefn def =
                (ServiceComponentDefn)tblModel.getValueAt(modelRow, SERV_COL);
        return def;
    }

    ///////////////////////////////////////////////////////////////////////////
    // INNER CLASSES
    ///////////////////////////////////////////////////////////////////////////

    private class PanelAction extends AbstractPanelAction {
        public static final int APPLY = 0;
        public static final int COPY = 1;
        public static final int DELETE = 2;
        public static final int RESET = 3;
        public static final int NEW = 4;
        public static final int ASSIGN = 5;


        public PanelAction(int theType) {
            super(theType);
            if (theType == APPLY) {
                putValue(NAME, getString("pfp.actionApply")); //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION, getString("pfp.actionApply.tip")); //$NON-NLS-1$
                setMnemonic(getMnemonicChar("pfp.actionApply.mnemonic")); //$NON-NLS-1$
            } else if (theType == COPY) {
                putValue(NAME, getString("pfp.actionCopy")); //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION, getString("pfp.actionCopy.tip")); //$NON-NLS-1$
                setMnemonic(getMnemonicChar("pfp.actionCopy.mnemonic")); //$NON-NLS-1$
            } else if (theType == DELETE) {
                putValue(NAME, getString("pfp.actionDelete")); //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION, getString("pfp.actionDelete.tip")); //$NON-NLS-1$
                setMnemonic(getMnemonicChar("pfp.actionDelete.mnemonic")); //$NON-NLS-1$
            } else if (theType == RESET) {
                putValue(NAME, getString("pfp.actionReset")); //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION, getString("pfp.actionReset.tip")); //$NON-NLS-1$
                setMnemonic(getMnemonicChar("pfp.actionReset.mnemonic")); //$NON-NLS-1$
                
            } else if (theType == NEW) {
                putValue(NAME, getString("pfp.actionNew")); //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION, getString("pfp.actionNew.tip")); //$NON-NLS-1$
                setMnemonic(getMnemonicChar("pfp.actionNew.mnemonic")); //$NON-NLS-1$
            } else if (theType == ASSIGN) {
                putValue(NAME, getString("pfp.actionEdit")); //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION, getString("pfp.actionEdit.tip")); //$NON-NLS-1$
                setMnemonic(getMnemonicChar("pfp.actionEdit.mnemonic")); //$NON-NLS-1$
                
            } else {
                throw new IllegalArgumentException(
                    getString("msg.invalidactiontype") + theType); //$NON-NLS-1$
            }
        }

        protected void actionImpl(ActionEvent theEvent)
            throws ExternalException {

            if (type == APPLY) {
                persist();
            } else if (type == NEW) {
                newPSC();
            } else if (type == ASSIGN) {
                assign();
                
            } else if (type == COPY) {
                copy();
            } else if (type == DELETE) {
                delete();
            } else if (type == RESET) {
                reset();
            } 
        }
    }
}//end PscDefinitionPanel



class SelectedServiceInfo {
    private ServiceComponentDefn service;
    private Object[] ancestors;

    public SelectedServiceInfo(ServiceComponentDefn srv, Object[] anc) {
        super();
        service = srv;
        ancestors = anc;
    }

    public ServiceComponentDefn getService() {
        return service;
    }

    public Object[] getAncestors() {
        return ancestors;
    }
}//end SelectedServiceInfo

