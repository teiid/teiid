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
import java.util.ArrayList;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
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
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.DialogPanel;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumnModel;

/**
 * @version 1.0
 * @author Dan Florian
 */
public final class DeploymentsSummaryPanel
    extends DetailPanel {

    ///////////////////////////////////////////////////////////////////////////
    // CONTROLS
    ///////////////////////////////////////////////////////////////////////////

    private TableWidget tblDeployments;

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

//    private PanelAction actionNew;
    private Configuration config;
    private DefaultTableModel deployTblModel;
    private ArrayList treeActions;

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public DeploymentsSummaryPanel(ConnectionInfo connInfo) {
        super(connInfo);
        setTitle(getString("dsp.title")); //$NON-NLS-1$
    }

    public DeploymentsSummaryPanel(ConfigurationID theConfigId,
    		ConnectionInfo conn)
        throws ExternalException {

        this(conn);
        setConfigId(theConfigId);
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    protected JPanel construct(boolean readOnly) {
        // construct actions first
        treeActions = new ArrayList();
 //       actionNew = new PanelAction(PanelAction.NEW);
        //Adding action "New Host..." here adds it a second time, so commenting
        //out.  BWP 09/10/02
        //treeActions.add(actionNew);

        JPanel pnl = new JPanel(new GridBagLayout());

        JPanel pnlDeployments = new JPanel(new GridLayout(1, 1));
        TitledBorder tBorder;
        tBorder = new TitledBorder(getString("dsp.pnlDeployments.title")); //$NON-NLS-1$
        pnlDeployments.setBorder(
            new CompoundBorder(tBorder,
                               DeployPkgUtils.EMPTY_BORDER));
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.insets = new Insets(3, 3, 0, 3);
        gbc.weightx = 1.0;
        gbc.weighty = 1.0;
        pnl.add(pnlDeployments, gbc);

        tblDeployments = new TableWidget();
        deployTblModel =
            DeployPkgUtils.setup(
                tblDeployments,
                DeployPkgUtils.DEPLOY_HDRS,
                DeployPkgUtils.getInt("dsp.deploytblrows", 10), //$NON-NLS-1$
                null);
        tblDeployments.setComparator(new DeployTableSorter());

        JScrollPane spnDeployments = new JScrollPane(tblDeployments);
        pnlDeployments.add(spnDeployments);

        JPanel pnlOps = new JPanel();
        gbc.gridx = 0;
        gbc.gridy = 1;
        gbc.fill = GridBagConstraints.NONE;
        gbc.insets = new Insets(3, 3, 0, 3);
        gbc.weightx = 0.0;
        gbc.weighty = 0.0;
        pnl.add(pnlOps, gbc);

        JPanel pnlOpsSizer = new JPanel(new GridLayout(1, 1));
        pnlOps.add(pnlOpsSizer);

//        ButtonWidget btnNew = new ButtonWidget();
//        setup(MenuEntry.ACTION_MENUITEM, btnNew, actionNew);
//        pnlOpsSizer.add(btnNew);

        return pnl;
    }

    public List getTreeActions() {
        return treeActions;
    }

//    private void newHost()
//        throws ExternalException {
//
//        // show dialog asking for new process name
//        CreatePanel pnl = new CreatePanel("dsp.msg.createhost", //$NON-NLS-1$
//                                          "icon.host.big", //$NON-NLS-1$
//                                          "dsp.lblnewhost", //$NON-NLS-1$
//                                          "hostname"); //$NON-NLS-1$
//        DialogWindow.show(
//            this,
//            DeployPkgUtils.getString("dsp.newhostdlg.title"), //$NON-NLS-1$
//            pnl);
//        if (pnl.isConfirmed()) {
//            // get the host name from the panel
////            ConfigurationID configId =
////                getConfigId();
//            ConfigurationManager configMgr =
//                getConfigurationManager();
//            String hostName = pnl.getName();
//            Host host = configMgr.createHost(hostName);
//            // a null host will be returned if it is already deployed
//            // in the current configuration
//            if (host == null) {
//                HostExistsInfoPanel pnlHostExists =
//                    new HostExistsInfoPanel(hostName);
//                DialogWindow.show(
//                    this,
//                    DeployPkgUtils.getString("dsp.hostexists.title"), //$NON-NLS-1$
//                    pnlHostExists);
//            }
//        }
//    }

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

//    public void setEnabled(boolean theEnableFlag) {
//        actionNew.setEnabled(theEnableFlag);
//    }

    ///////////////////////////////////////////////////////////////////////////
    // INNER CLASSES
    ///////////////////////////////////////////////////////////////////////////

//    private class PanelAction extends AbstractPanelAction {
//        public static final int NEW = 0;
//        private int type = -1;
//
//        public PanelAction(int theType) {
//            super(theType);
//            if (theType == NEW) {
//                putValue(NAME, getString("dsp.actionNew")); //$NON-NLS-1$
//                putValue(SHORT_DESCRIPTION, getString("dsp.actionNew.tip")); //$NON-NLS-1$
//                setMnemonic(getMnemonicChar("dsp.actionNew.mnemonic")); //$NON-NLS-1$
//            }
//            else {
//                throw new IllegalArgumentException(
//                    getString("msg.invalidactiontype") + theType); //$NON-NLS-1$
//            }
//            type = theType;
//        }
//        public void actionImpl(ActionEvent theEvent)
//            throws ExternalException {
//            if (type == NEW) {
//                newHost();
//            }
//        }
//    }

    public class HostExistsInfoPanel extends DialogPanel {
        public HostExistsInfoPanel(String theHostName) {
            super();
            JPanel pnl = new JPanel(new GridBagLayout());
            pnl.setBorder(BorderFactory.createEmptyBorder(30, 40, 30, 40));
            JLabel lbl =
                new LabelWidget(DeployPkgUtils.getString("dsp.hostexists.msg", //$NON-NLS-1$
                                                         new Object[] {theHostName}));
            lbl.setIcon(DeployPkgUtils.getIcon("icon.info")); //$NON-NLS-1$
            pnl.add(lbl);
            setContent(pnl);
            ButtonWidget btnOk = getAcceptButton();
            btnOk.setText(DeployPkgUtils.getString("dsp.hostexists.btnOk")); //$NON-NLS-1$
            removeNavigationButton(getCancelButton());
        }

    }
}
