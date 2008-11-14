/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.border.CompoundBorder;
import javax.swing.table.TableColumn;

import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;
import com.metamatrix.console.ui.views.deploy.util.DeployTableSorter;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumnModel;

/**
 * @version 1.0
 * @author Dan Florian
 */
public final class ConfigSummaryPanel
    extends DetailPanel {

    ///////////////////////////////////////////////////////////////////////////
    // CONTROLS
    ///////////////////////////////////////////////////////////////////////////

    private TableWidget tblDeployments;
    private TableWidget tblPscs;

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private Configuration config;
    private DefaultTableModel deployTblModel;
    private DefaultTableModel pscsTblModel;

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public ConfigSummaryPanel(ConnectionInfo conn) {
        super(conn);
        setTitle(getString("csp.title")); //$NON-NLS-1$
    }

    public ConfigSummaryPanel(ConfigurationID theConfigId,
    		ConnectionInfo conn)
        throws ExternalException {

        this(conn);
        setConfigId(theConfigId);
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    protected JPanel construct(boolean readOnly) {
        JPanel pnl = new JPanel(new GridBagLayout());

        JPanel pnlDeployments = new JPanel();
        pnlDeployments.setLayout(new GridLayout(1, 1));
        TitledBorder tBorder;
        tBorder = new TitledBorder(getString("csp.pnlDeployments.title")); //$NON-NLS-1$
        pnlDeployments.setBorder(
            new CompoundBorder(tBorder, DeployPkgUtils.EMPTY_BORDER));
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.insets = new Insets(3, 3, 3, 3);
        gbc.gridwidth = GridBagConstraints.REMAINDER;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weightx = 0.3;
        gbc.weighty = 0.3;
        pnl.add(pnlDeployments, gbc);

        tblDeployments = new TableWidget();
        deployTblModel =
            DeployPkgUtils.setup(
                tblDeployments,
                DeployPkgUtils.DEPLOY_HDRS,
                DeployPkgUtils.getInt("csp.deploytblrows", 10), //$NON-NLS-1$
                null);
        tblDeployments.setComparator(new DeployTableSorter());

        JScrollPane spnDeployments = new JScrollPane(tblDeployments);
        pnlDeployments.add(spnDeployments);

        JPanel pnlPscs = new JPanel(new GridLayout(1, 1));
        tBorder = new TitledBorder(getString("csp.pnlPscs.title")); //$NON-NLS-1$
                                               
        pnlPscs.setBorder(
            new CompoundBorder(tBorder,
                               DeployPkgUtils.EMPTY_BORDER));
        gbc.gridx = 0;
        gbc.gridy = 1;
        gbc.gridwidth = GridBagConstraints.REMAINDER;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weightx = 1.0;
        gbc.weighty = 1.0;
        pnl.add(pnlPscs, gbc);

        tblPscs = new TableWidget();
        pscsTblModel =
            DeployPkgUtils.setup(
                tblPscs,
                DeployPkgUtils.PSC_SERV_DEF_HDRS,
                DeployPkgUtils.getInt("csp.psctblrows", 10), //$NON-NLS-1$
                null);
        tblPscs.setComparator(new DeployTableSorter());

        JScrollPane spnPscs = new JScrollPane(tblPscs);
        pnlPscs.add(spnPscs);
        return pnl;
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

        if (theDomainObject instanceof Configuration) {
            config = (Configuration)theDomainObject;
        }
        else {
            throw new IllegalArgumentException(
                getString("msg.invalidclass", //$NON-NLS-1$
                          new Object[] {"Configuration", //$NON-NLS-1$
                                        theDomainObject.getClass()}));
        }
        super.setDomainObject(config, theAncestors);

        try {
            // clear and load deployments table
            DeployPkgUtils.loadDeployments(config, deployTblModel, getConnectionInfo());
            tblDeployments.sizeColumnsToFitData();
            sortFirstColumnInTable(tblDeployments);
            // clear and load psc service defintion table
            DeployPkgUtils.loadPscServiceDefintions(config, pscsTblModel,
                                                    getConnectionInfo());
            tblPscs.sizeColumnsToFitData();
            sortFirstColumnInTable(tblPscs);
        }
        catch (ExternalException theException) {
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

}
