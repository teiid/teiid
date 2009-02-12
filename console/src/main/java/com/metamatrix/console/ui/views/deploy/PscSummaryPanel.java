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

import java.awt.GridLayout;

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
public final class PscSummaryPanel
    extends DetailPanel {

    ///////////////////////////////////////////////////////////////////////////
    // CONTROLS
    ///////////////////////////////////////////////////////////////////////////

    private TableWidget tblPscs;

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private Configuration config;
    private DefaultTableModel pscsTblModel;
    private ConnectionInfo connection;

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public PscSummaryPanel(ConnectionInfo connInfo) {
        super(connInfo);
        this.connection = connInfo;
        setTitle(getString("psp.title")); //$NON-NLS-1$
    }

    public PscSummaryPanel(ConfigurationID theConfigId, 
    		ConnectionInfo connInfo) 
    		throws ExternalException {
		this(connInfo);
        setConfigId(theConfigId);
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    protected JPanel construct(boolean readOnly) {
        JPanel pnl = new JPanel(new GridLayout(1, 1));
        TitledBorder tBorder;
        tBorder = new TitledBorder(getString("psp.pnl.title")); //$NON-NLS-1$
        pnl.setBorder(
            new CompoundBorder(tBorder,
                               DeployPkgUtils.EMPTY_BORDER));

        tblPscs = new TableWidget();
        pscsTblModel =
            DeployPkgUtils.setup(
                tblPscs,
                DeployPkgUtils.PSC_SERV_DEF_HDRS,
                DeployPkgUtils.getInt("psp.psctblrows", 10), //$NON-NLS-1$
                null);
        tblPscs.setComparator(new DeployTableSorter());

        JScrollPane spnPscs = new JScrollPane(tblPscs);
        pnl.add(spnPscs);

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
            // clear and load psc service defintion table
            DeployPkgUtils.loadPscServiceDefintions(config, pscsTblModel,
            		connection);
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
