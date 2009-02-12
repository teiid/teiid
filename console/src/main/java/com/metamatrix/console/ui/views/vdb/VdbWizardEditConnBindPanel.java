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

package com.metamatrix.console.ui.views.vdb;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import javax.swing.AbstractButton;
import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.JPanel;
import javax.swing.JRadioButton;

import com.metamatrix.common.vdb.api.VDBDefn;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.models.VdbManager;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.vdb.runtime.BasicModelInfo;

public class VdbWizardEditConnBindPanel extends BasicWizardSubpanelContainer {    
    private static final String USE_VDB_CONNECTOR_BINDINGS = "Use Connector Bindings From Current VDB";
    private static final String USE_MIGRATE_CONNECTOR_BINDINGS = "Migrate Connector Bindings From Previous VDB Version";
    private VdbAssignConnBindPanel vacbConnectorBindingEditPanel;

    // used when this panel is doing a new version
    private VirtualDatabase vdbSourceVdb = null;

    private boolean newVDBVersion;
    boolean editCBPInitSuccessful = true;

    private JRadioButton vdbBindings = new JRadioButton(USE_VDB_CONNECTOR_BINDINGS);
    private JRadioButton migratedBindings = new JRadioButton(USE_MIGRATE_CONNECTOR_BINDINGS, true);
    private ButtonGroup migrateBtnGroup = new ButtonGroup();
    
    private JPanel pnlOuter = new JPanel();
    private VDBDefn vdbDefn = null;
    private ConnectionInfo connection;
    private int panelOrder;

    public VdbWizardEditConnBindPanel(int step, WizardInterface wizardInterface, ConnectionInfo connection) {
        super(wizardInterface);
        this.panelOrder = step;
        this.connection = connection;
        this.newVDBVersion = false;

        try {
            init();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public VdbWizardEditConnBindPanel(int step, VirtualDatabase vdb, WizardInterface wizardInterface, ConnectionInfo connection) {
        super(wizardInterface);

        this.panelOrder = step;
        this.connection = connection;
        this.vdbSourceVdb = vdb;
        this.newVDBVersion = (this.vdbSourceVdb != null);
        try {
            init();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private VdbManager getVdbManager() {
        return ModelManager.getVdbManager(connection);
    }

    private void init() throws Exception {

        pnlOuter.setLayout(new BorderLayout());

        vacbConnectorBindingEditPanel = new VdbAssignConnBindPanel(connection);
        
        JPanel migratePanel = new JPanel();
        GridBagLayout migrateLayout = new GridBagLayout();
        migrateLayout.setConstraints(vdbBindings, new GridBagConstraints(0, 0, 1, 1, 0, 0,GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        migrateLayout.setConstraints(migratedBindings, new GridBagConstraints(0, 1, 1, 1, 0, 0,GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        migratePanel.setLayout(migrateLayout);
        migratePanel.setBorder(BorderFactory.createCompoundBorder(BorderFactory.createEtchedBorder(),BorderFactory.createEmptyBorder(1,0,1,0)));
        migratePanel.add(vdbBindings);
        migratePanel.add(migratedBindings);
        migrateBtnGroup.add(vdbBindings);
        migrateBtnGroup.add(migratedBindings);
        
        ActionListener listener = new ActionListener() {
            public void actionPerformed(ActionEvent event) {
                runMigrateBindings();
            }
        };
        
        vdbBindings.addActionListener(listener);
        migratedBindings.addActionListener(listener);
        
        pnlOuter.add(vacbConnectorBindingEditPanel, BorderLayout.CENTER);

        // if this is new version of a previous VDB, then automatically assign the
        // previous bindings to new VDB        
        if (newVDBVersion) {
            pnlOuter.add(migratePanel, BorderLayout.SOUTH);
        }

        setMainContent(pnlOuter);
        String hdr = "Assign Connector Bindings to the Physical Models.";
        String[] paragraphs = new String[] {
            "Select a Binding from the table at the left, then select one or more rows in the Model table at the right, then click the Assign button.",
            "To remove an assignment, select the row in the Model table and click the Unassign button."
        };
        setStepText(panelOrder, true, hdr, paragraphs);
    }

    /**
     * Switch the connector bindings from original --> Migrate
     * of Migrate --> Original
     */
    private void runMigrateBindings() {
        vacbConnectorBindingEditPanel.switchConnectorBindings(!vdbBindings.isSelected());
    }
    
    void useBindingsFromPreviousVDB() {
        VDBDefn rve = getVDB();
        try {
            /* <String model name to Collection of String UUIDs> */
            Map bindings = getVdbManager().migrateConnectorBindingNames(vdbSourceVdb, rve);
            vacbConnectorBindingEditPanel.setMigratedBindings(bindings);
            runMigrateBindings();
        } catch (Exception e) {
            ExceptionUtility.showMessage("Failed while retrieving the Models to Bindings map", e);
            editCBPInitSuccessful = false;
        }
    }
    
    public boolean getEditCBPInitSuccessful() {
        if (!vacbConnectorBindingEditPanel.getVdbACBPSuccessful()) {
            editCBPInitSuccessful = false;
        }
        return editCBPInitSuccessful;
    }

    public void setVDB(VDBDefn rmcVersionEntry) {
        this.vdbDefn = rmcVersionEntry;
    }

    public void loadAdditionalBindings() {

        if (vdbDefn.getConnectorBindings() != null) {
            vacbConnectorBindingEditPanel.setVDBVersion(vdbDefn);
        }
    }

    public VDBDefn getVDB() {
        return vdbDefn;
    }

    public void setModels(Collection /* <RMCModelEntry> */colModels) {

        // convert these models into modelwrappers

        ArrayList arylModelWrappers = new ArrayList();
        Iterator it = colModels.iterator();

        while (it.hasNext()) {
            arylModelWrappers.add(new ModelWrapper((BasicModelInfo)it.next()));
        }

        vacbConnectorBindingEditPanel.setModels(arylModelWrappers);
    }

    public void updateMultiSource(Map /* <String model name to Boolean multi-source selected> */multiSourceInfo) {
        vacbConnectorBindingEditPanel.updateMultiSource(multiSourceInfo);
    }

    /**
     *  <String binding name to Collection of String (UUIDs)> 
     */
    public Map getModelsToConnBindsMap() {
        return vacbConnectorBindingEditPanel.getModelsToConnectorBindingsMap();
    }

    public void updateSelectionForModelsTable() {
        vacbConnectorBindingEditPanel.updateSelectionForModelsTable();
    }

    public boolean hasBindingAssigned(String modelName) {
        Map /* <String binding name to Collection of String (UUIDs)> */bindingMap = getModelsToConnBindsMap();
        boolean found = false;
        boolean assigned = false;
        Iterator it = bindingMap.entrySet().iterator();
        while (it.hasNext() && (!found)) {
            Map.Entry me = (Map.Entry)it.next();
            String thisModel = (String)me.getKey();
            if (thisModel.equals(modelName)) {
                found = true;
                Collection /* <String> */uuids = (Collection)me.getValue();
                Iterator it2 = uuids.iterator();
                while ((!assigned) && it2.hasNext()) {
                    String uuid = (String)it2.next();
                    if (uuid != null) {
                        uuid = uuid.trim();
                        if (uuid.length() > 0) {
                            assigned = true;
                        }
                    }
                }
            }
        }
        return assigned;
    }

    public void resolveForwardButton() {
        // set to true because this page is optional:
        AbstractButton forwardButton = getWizardInterface().getForwardButton();
        forwardButton.setEnabled(true);
    }
}
