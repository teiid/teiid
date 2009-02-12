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
import java.util.Collection;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.border.CompoundBorder;
import javax.swing.table.TableColumn;

import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.ProductServiceConfig;
import com.metamatrix.common.config.api.ProductType;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;
import com.metamatrix.console.ui.views.deploy.util.DeployTableSorter;
import com.metamatrix.console.ui.views.deploy.util.PropertyConstants;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.LogContexts;
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
public final class DeployedPscPanel
    extends DetailPanel
    implements PropertyConstants {

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    private static final String[] SERVICE_HDRS;
    private static final int SERV_COL = 0;
    private static final int ESSENTIAL_COL = 1;

    ///////////////////////////////////////////////////////////////////////////
    // INITIALIZER
    ///////////////////////////////////////////////////////////////////////////

    static {
        SERVICE_HDRS = new String[2];
        SERVICE_HDRS[SERV_COL] = DeployPkgUtils.getString("dpp.service.hdr"); //$NON-NLS-1$
        SERVICE_HDRS[ESSENTIAL_COL] = DeployPkgUtils.getString("dpp.essential.hdr"); //$NON-NLS-1$
    }

    ///////////////////////////////////////////////////////////////////////////
    // CONTROLS
    ///////////////////////////////////////////////////////////////////////////

    private TableWidget tblServices;
    private TextFieldWidget txfHost;
    private TextFieldWidget txfProcess;
    private TextFieldWidget txfProduct;

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private ProductServiceConfig psc;
    private DefaultTableModel tblModel;
    
    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public DeployedPscPanel(ConnectionInfo connInfo) {
        super(connInfo);
        setTitle(getString("dpp.title") ); //$NON-NLS-1$
    }

    public DeployedPscPanel(ConnectionInfo connInfo, ConfigurationID theConfigId)
        throws ExternalException {

        this(connInfo);
        setConfigId(theConfigId);
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    protected JPanel construct(boolean readOnly) {
        JPanel pnl = new JPanel(new GridBagLayout());

        LabelWidget lblHost = DeployPkgUtils.createLabel("dpp.lblHost"); //$NON-NLS-1$
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(3, 3, 3, 3);
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.anchor = GridBagConstraints.EAST;
        pnl.add(lblHost, gbc);

        txfHost = DeployPkgUtils.createTextField("hostname"); //$NON-NLS-1$
        txfHost.setEditable(false);
        gbc.gridx = 1;
        gbc.gridy = 0;
        gbc.weightx = 0.2;
        gbc.weighty = 0;
        gbc.anchor = GridBagConstraints.WEST;
        pnl.add(txfHost, gbc);

        LabelWidget lblProcess = DeployPkgUtils.createLabel("dpp.lblProcess"); //$NON-NLS-1$
        gbc.gridx = 0;
        gbc.gridy = 1;
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.anchor = GridBagConstraints.EAST;
        pnl.add(lblProcess, gbc);

        txfProcess = DeployPkgUtils.createTextField("processname"); //$NON-NLS-1$
        txfProcess.setEditable(false);
        gbc.gridx = 1;
        gbc.gridy = 1;
        gbc.weightx = 0.2;
        gbc.weighty = 0;
        gbc.anchor = GridBagConstraints.WEST;
        pnl.add(txfProcess, gbc);

        LabelWidget lblProduct = DeployPkgUtils.createLabel("dpp.lblProduct"); //$NON-NLS-1$
        gbc.gridx = 0;
        gbc.gridy = 2;
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.anchor = GridBagConstraints.EAST;
        pnl.add(lblProduct, gbc);

        txfProduct = DeployPkgUtils.createTextField("productname"); //$NON-NLS-1$
        txfProduct.setEditable(false);
        gbc.gridx = 1;
        gbc.gridy = 2;
        gbc.weightx = 0.2;
        gbc.weighty = 0;
        gbc.anchor = GridBagConstraints.WEST;
        pnl.add(txfProduct, gbc);

//        LabelWidget lblPsc = DeployPkgUtils.createLabel("dpp.lblPsc"); //$NON-NLS-1$
//        gbc.gridx = 2;
//        gbc.gridy = 1;
//        gbc.weightx = 0;
//        gbc.weighty = 0;
//        gbc.anchor = GridBagConstraints.EAST;
//        pnl.add(lblPsc, gbc);
//
//        txfPsc = DeployPkgUtils.createTextField("pscname"); //$NON-NLS-1$
//        txfPsc.setEditable(false);
//        gbc.gridx = 3;
//        gbc.gridy = 1;
//        gbc.weightx = 0.2;
//        gbc.weighty = 0;
//        gbc.anchor = GridBagConstraints.WEST;
//        pnl.add(txfPsc, gbc);

        JPanel pnlServices = new JPanel(new GridLayout(1, 1));
        TitledBorder tBorder;
        tBorder = new TitledBorder(getString("dpp.pnlServices.title")); //$NON-NLS-1$
        pnlServices.setBorder(
            new CompoundBorder(tBorder , DeployPkgUtils.EMPTY_BORDER));


        gbc.gridx = 0;
        gbc.gridy = 3;
        gbc.gridwidth = GridBagConstraints.REMAINDER;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weightx = 1.0;
        gbc.weighty = 1.0;
        pnl.add(pnlServices, gbc);

        tblServices = new TableWidget();
        tblModel =
            DeployPkgUtils.setup(
                tblServices,
                SERVICE_HDRS,
                DeployPkgUtils.getInt("dpp.servicestblrows", 10), //$NON-NLS-1$
                null);
        tblServices.setComparator(new DeployTableSorter());

        JScrollPane spnServices = new JScrollPane(tblServices);
        pnlServices.add(spnServices);

        return pnl;
    }

    public void setConfigId(ConfigurationID theConfigId) {

        super.setConfigId(theConfigId);
        setTitleSuffix(getString("dpp.title.suffix")); //$NON-NLS-1$
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

        if (theDomainObject instanceof ProductServiceConfig) {
            psc = (ProductServiceConfig)theDomainObject;
            setTitleSuffix(psc.getName());
        }
        else {
            throw new IllegalArgumentException(
                getString("msg.invalidclass", //$NON-NLS-1$
                          new Object[] {"ProductServiceConfig", //$NON-NLS-1$
                                        theDomainObject.getClass()}));
        }
        super.setDomainObject(psc, theAncestors);

        try {
            Configuration config = getConfigurationManager().getConfig(getConfigId());

            VMComponentDefn process = (VMComponentDefn)theAncestors[0];
            txfProcess.setText(process.getName());

            Host host = (Host)theAncestors[1];
            txfHost.setText(host.getName());

            ProductType product = getConfigurationManager().getProduct(psc);
            txfProduct.setText(product.getName());

            // populate table
            tblModel.setNumRows(0);

            Collection services = getConfigurationManager().getDeployedServices(
            		psc, process);
            if (services != null) {
                Iterator servItr = services.iterator();
                while (servItr.hasNext()) {
                    DeployedComponent servDp =
                        (DeployedComponent)servItr.next();
                    ServiceComponentDefnID id = servDp.getServiceComponentDefnID();
                    ServiceComponentDefn service =
                        (ServiceComponentDefn)config.getComponentDefn(id);
                    Vector row = new Vector(SERVICE_HDRS.length);
                    row.setSize(SERVICE_HDRS.length);
                    row.setElementAt(service, SERV_COL);
                    String essential = service.getProperty(ESSENTIAL_PROP);
                    if (essential == null) essential = ""; //$NON-NLS-1$
                    row.setElementAt(new Boolean(essential),
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
                ""+theException.getMessage(), //$NON-NLS-1$
                theException);
            LogManager.logError(
                LogContexts.PSCDEPLOY,
                theException,
                getClass() + ":setDomainObject"); //$NON-NLS-1$
        }
    }
}
