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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.border.CompoundBorder;
import javax.swing.table.TableColumn;

import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ProductServiceConfig;
import com.metamatrix.common.config.api.ProductType;
import com.metamatrix.common.config.api.ProductTypeID;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConfigurationManager;
import com.metamatrix.console.ui.layout.MenuEntry;
import com.metamatrix.console.ui.util.AbstractPanelAction;
import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;
import com.metamatrix.console.ui.views.deploy.util.DeployTableSorter;
import com.metamatrix.console.ui.views.deploy.util.PropertyConstants;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.core.util.MetaMatrixProductVersion;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.DialogWindow;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
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
public final class ProductDefinitionPanel
    extends DetailPanel
    implements PropertyConstants {

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    private static final String[] SERVICES_HDRS;
    private static final int PSC_COL = 0;
    private static final int SERVICE_COL = 1;
    private static final int ENABLED_COL = 2;
    private static final int ESSENTIAL_COL = 3;

    ///////////////////////////////////////////////////////////////////////////
    // INITIALIZER
    ///////////////////////////////////////////////////////////////////////////

    static {
        SERVICES_HDRS = new String[4];
        SERVICES_HDRS[PSC_COL] = DeployPkgUtils.getString("pdp.psc.hdr"); //$NON-NLS-1$
        SERVICES_HDRS[SERVICE_COL] = DeployPkgUtils.getString("pdp.service.hdr"); //$NON-NLS-1$
        SERVICES_HDRS[ENABLED_COL] = DeployPkgUtils.getString("pdp.enabled.hdr"); //$NON-NLS-1$
        SERVICES_HDRS[ESSENTIAL_COL] = DeployPkgUtils.getString("pdp.essential.hdr"); //$NON-NLS-1$
    }

    ///////////////////////////////////////////////////////////////////////////
    // CONTROLS
    ///////////////////////////////////////////////////////////////////////////

    private TableWidget tblServices;
    private TextFieldWidget txfProduct;
    
    
    private PanelAction actionNew;
    

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private ProductType product;
    private DefaultTableModel tblModel;

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public ProductDefinitionPanel(ConnectionInfo connectionInfo) {
        super(connectionInfo);
        setTitle(getString("pdp.title")); //$NON-NLS-1$
    }

    public ProductDefinitionPanel(ConfigurationID theConfigId,
                                  ConnectionInfo connectionInfo)
        throws ExternalException {

        this(connectionInfo);
        setConfigId(theConfigId);
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    protected JPanel construct(boolean readOnly) {
        
        
        JPanel pnl = new JPanel(new GridBagLayout());

        actionNew = new PanelAction(PanelAction.NEW);
        
        
        LabelWidget lblProduct = DeployPkgUtils.createLabel("pdp.lblProduct"); //$NON-NLS-1$
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(3, 3, 3, 3);
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.anchor = GridBagConstraints.EAST;
        pnl.add(lblProduct, gbc);

        txfProduct = DeployPkgUtils.createTextField("productname"); //$NON-NLS-1$
        txfProduct.setEditable(false);
        gbc.gridx = 1;
        gbc.gridy = 0;
        gbc.anchor = GridBagConstraints.WEST;
        pnl.add(txfProduct, gbc);

        JPanel pnlServices = new JPanel(new GridLayout(1, 1));
        TitledBorder tBorder;
        tBorder = new TitledBorder(getString("pdp.pnlServices.title")); //$NON-NLS-1$
        pnlServices.setBorder(
            new CompoundBorder(tBorder,
                               DeployPkgUtils.EMPTY_BORDER));

        gbc.gridx = 0;
        gbc.gridy = 1;
        gbc.gridwidth = GridBagConstraints.REMAINDER;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weightx = 1.0;
        gbc.weighty = 1.0;
        pnl.add(pnlServices, gbc);

        tblServices = new TableWidget();
        tblModel =
            DeployPkgUtils.setup(
                tblServices,
                SERVICES_HDRS,
                DeployPkgUtils.getInt("pdp.servicestblrows", 10), //$NON-NLS-1$
                null);
        tblServices.setComparator(new DeployTableSorter());

        JScrollPane spnServices = new JScrollPane(tblServices);
        pnlServices.add(spnServices);
        
        JPanel pnlOps = new JPanel();
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 2;
        gbc.gridwidth = GridBagConstraints.REMAINDER;
        gbc.insets = new Insets(3, 3, 3, 3);
        pnl.add(pnlOps, gbc);
        
        
        JPanel pnlOpsSizer = new JPanel(new GridLayout(1, 1, 10, 0));
        pnlOps.add(pnlOpsSizer);

        
        ButtonWidget btnNew = new ButtonWidget();
        setup(MenuEntry.ACTION_MENUITEM, btnNew, actionNew);
        pnlOpsSizer.add(btnNew);
        

        return pnl;
    }
    
    public void setEnabled(boolean theEnableFlag) {
        actionNew.setEnabled(theEnableFlag);
    }
    

    public void setConfigId(ConfigurationID theConfigId) {

        super.setConfigId(theConfigId);
        setTitleSuffix(getString("pdp.title.suffix")); //$NON-NLS-1$
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

        if (theDomainObject instanceof ProductType) {
            product = (ProductType)theDomainObject;
            setTitleSuffix(product.getName());
        }
        else {
            throw new IllegalArgumentException(
                getString("msg.invalidclass", //$NON-NLS-1$
                          new Object[] {"ProductType", //$NON-NLS-1$
                                        theDomainObject.getClass()}));
        }
        super.setDomainObject(product, theAncestors);
        txfProduct.setText(product.getName());

        Configuration config = (Configuration)theAncestors[0];

        tblModel.setNumRows(0);
        try {
            ConfigurationManager configMgr = getConfigurationManager();
            Collection pscs = configMgr.getPscDefinitions(product, config);
            if (pscs != null) {
                Iterator pscItr = pscs.iterator();
                while (pscItr.hasNext()) {
                    ProductServiceConfig psc =
                        (ProductServiceConfig)pscItr.next();
                    Collection services =
                        configMgr.getServiceDefinitions(psc, config);
                        
                    if (services != null) {
                        Iterator servItr = services.iterator();
                        while (servItr.hasNext()) {
                            ServiceComponentDefn service =
                                (ServiceComponentDefn)servItr.next();
                            Vector row = new Vector(SERVICES_HDRS.length);
                            row.setSize(SERVICES_HDRS.length);
                            row.setElementAt(psc, PSC_COL);
                            row.setElementAt(service, SERVICE_COL);
                            
                    ServiceComponentDefnID svcID = (ServiceComponentDefnID) service.getID();
                    if (!psc.containsService(svcID)) {
                    	throw new Exception("Service " + svcID + " not contained in PSC " + psc.getName());                    	 //$NON-NLS-1$ //$NON-NLS-2$
                    }
                    Boolean enabled = new Boolean(psc.isServiceEnabled( svcID ) );
                            
//                            Boolean enabled = new Boolean(service.isEnabled());
                            row.setElementAt(enabled, ENABLED_COL);
                            row.setElementAt(
                                new Boolean(service.getProperty(ESSENTIAL_PROP)),
                                ESSENTIAL_COL);
                            tblModel.addRow(row);
                        }
                    }
                }
            }
            tblServices.sizeColumnsToFitData();
            sortFirstColumnInTable(tblServices);
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
    
    private void newPSC() 
    throws ExternalException {
    Configuration config = this.getConfigurationManager().getConfig(Configuration.NEXT_STARTUP_ID);
    
    List pscServiceNames = null;
    if (product.getName().equals(MetaMatrixProductVersion.CONNECTOR_PRODUCT_TYPE_NAME)) {
        
        
//    if (pscDef.getComponentTypeID().getName().equals(ProductType.CONNECTOR_PRODUCT_TYPE_NAME)) {

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
                                      null,
                                      pscServiceNames);

                                      
    DialogWindow.show(
        this,
        DeployPkgUtils.getString("pfp.newpscdefdlg.title"), //$NON-NLS-1$
        pnl);
    if (pnl.isConfirmed()) {
        // get the process name from the panel
        String pscDefName = pnl.getName();
        List services = pnl.getSelectedServices();
        
       getConfigurationManager().createPscDef(pscDefName, (ProductTypeID) product.getID(), 
                services,
                    getConfigId());
    }
        
}
    
    
    private class PanelAction extends AbstractPanelAction {
        public static final int NEW = 4;


        public PanelAction(int theType) {
            super(theType);
                
            if (theType == NEW) {
                putValue(NAME, getString("pfp.actionNew")); //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION, getString("pfp.actionNew.tip")); //$NON-NLS-1$
                setMnemonic(getMnemonicChar("pfp.actionNew.mnemonic")); //$NON-NLS-1$
            }  else {
                throw new IllegalArgumentException(
                    getString("msg.invalidactiontype") + theType); //$NON-NLS-1$
            }
        }

        protected void actionImpl(ActionEvent theEvent)
            throws ExternalException {


            if (type == NEW) {
                newPSC();
            } 
        }
    }
    

}
