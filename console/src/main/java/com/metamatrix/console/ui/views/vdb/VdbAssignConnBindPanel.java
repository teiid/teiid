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
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.vdb.api.VDBDefn;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConnectorManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.notification.AddedConnectorBindingNotification;
import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.ui.layout.WorkspaceController;
import com.metamatrix.console.ui.util.ColumnSortInfo;
import com.metamatrix.console.ui.views.connectorbinding.ConnectorAndBinding;
import com.metamatrix.console.ui.views.connectorbinding.NewBindingWizardController;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumnModel;
import com.metamatrix.toolbox.ui.widget.table.TableColumnSortListener;

public class VdbAssignConnBindPanel extends JPanel implements
                                                  MultiSourceModelBindingEditRequestHandler {

    public static final String CONNECTOR_BINDINGS_HDR = "Connector Bindings";
    public static final String MODEL_NAME_HDR = VDBConnectorBindingAssignmentModelTable.MODEL_NAME;
    private BorderLayout borderLayout1 = new BorderLayout();
    private JPanel pnlOuter = new JPanel();
    private JPanel pnlModelsTable = new JPanel();
    private JPanel pnlConnBindTable = new JPanel();
    private JPanel pnlAssignButtons = new JPanel();
    private JScrollPane scpConnBindScroller = new JScrollPane();
    private TableWidget tblConnBinds = new TableWidget();
    private ColumnSortInfo[] connBindsSortInfo = null;
    private VDBConnectorBindingAssignmentModelTable tblModels = null;
    private ColumnSortInfo[] modelsSortInfo = null;
    private JButton btnAssign = new JButton();
    private JButton btnUnassign = new JButton();
    private JButton btnNewConnectorBinding = new JButton();
    private BorderLayout borderLayout2 = new BorderLayout();
    private BorderLayout borderLayout3 = new BorderLayout();
    private ListSelectionModel smConnBindsSelModel = null;
    private ListSelectionModel smModelsSelModel = null;
    private String sSelectedConnBind = "";
    private static final int CONN_BIND_COL_IN_TBL_CONN_BIND = 0;
    private static final int CONN_BIND_COL_IN_TBL_MODELS = 1;
    private static final int MODEL_COL_IN_TBL_MODELS = 0;
    private boolean bAssignableRowSelectedInModelsTable = false;
    private boolean bUnassignableRowSelectedInModelsTable = false;
    private boolean bRowSelectedInConnBindTable = false;
    private int[] aryModelsTableSelectedRows = null;
    private ArrayList arylConnBinds = null;
    private HashMap /* <String conn. bind name to ConnectorBinding> */hmConnBindsXref = new HashMap();
    private String[] aryCBColNames = {
        CONNECTOR_BINDINGS_HDR
    };

    private Map /* <String model name to ConnectorBindingNameAndUUID[]> */mapModelsToBindings = null;
    private HashMap hmUUIDConnectorBindingsMap = null;
    private Map /* <String UUID to String name> */addTohmUUIDConnectorBindingsMap = null;
    private Collection addToListOfBindings = null;
    private boolean vdbACBPInitSuccessful = true;

    private ConnectionInfo connection;
    private Map migratedBindings = null;
    private Map vdbBindings = null;
    
    public VdbAssignConnBindPanel(ConnectionInfo connection) {
        super();
        this.connection = connection;
        try {
            jbInit();
            setConnBindTableListening();
            setModelsTableListening();
            tblConnBinds.setSortable(true);
            tblModels.setSortable(true);
            setInitialTableSorting();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private ConnectorManager getConnectorManager() {
        return ModelManager.getConnectorManager(connection);
    }

    public Dimension getPreferredSize() {
        Dimension startingSize = super.getPreferredSize();
        int newWidth = Math.max(startingSize.width, (int)(Toolkit.getDefaultToolkit().getScreenSize().width * 0.625));
        return new Dimension(newWidth, startingSize.height);
    }

    private String getSelectedConnBind() {
        return sSelectedConnBind;
    }

    private void setConnBindTableListening() {
        tblConnBinds.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        smConnBindsSelModel = tblConnBinds.getSelectionModel();
        smConnBindsSelModel.addListSelectionListener(new ListSelectionListener() {

            public void valueChanged(ListSelectionEvent e) {
                // Ignore extra messages.
                if (e.getValueIsAdjusting())
                    return;

                ListSelectionModel lsm = (ListSelectionModel)e.getSource();
                int iTrueModelRow = 0;

                if (!lsm.isSelectionEmpty()) {
                    iTrueModelRow = tblConnBinds.convertRowIndexToModel(lsm.getMinSelectionIndex());
                    updateSelectionForConnBindTable(iTrueModelRow);
                } else {
                    updateDeselectionForConnBindTable();
                }
            }
        });

        EnhancedTableColumnModel etcm = tblConnBinds.getEnhancedColumnModel();
        etcm.addColumnSortListener(new TableColumnSortListener() {

            public void columnSorted() {
                tblConnBinds.clearSelection();
                updateDeselectionForConnBindTable();
            }
        });
    }

    private void updateSelectionForConnBindTable(int iRow) {
        String sSelectedConnBind = "";
        sSelectedConnBind = (String)tblConnBinds.getModel().getValueAt(iRow, CONN_BIND_COL_IN_TBL_CONN_BIND);

        setSelectedConnBind(sSelectedConnBind);
        bRowSelectedInConnBindTable = true;
        enableAssignButtons();
    }

    private void updateDeselectionForConnBindTable() {
        setSelectedConnBind("");
        bRowSelectedInConnBindTable = false;
        enableAssignButtons();
    }

    private void setSelectedConnBind(String sSelectedConnBind) {
        this.sSelectedConnBind = sSelectedConnBind;
    }

    public void setVDBVersion(VDBDefn vdb) {
        this.vdbBindings = getBindingsFromVDB(vdb);
        
        if (this.migratedBindings == null) {
            this.mapModelsToBindings = this.vdbBindings;
        }
        else{
            this.mapModelsToBindings = this.migratedBindings;
        }
        
        setAdditionalBindings(vdb.getConnectorBindings());        
        loadAvailableBindings(null);        
    }

    private Map getBindingsFromVDB(VDBDefn vdb) {        
        Map tempMap = vdb.getModelToBindingMappings(); //  <String model name to List of String binding UUIDs>         
        Map newMap = new HashMap(); // <String model name to ConnectorBindingNameAndUUID[]> 
        
        Iterator it = tempMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry me = (Map.Entry)it.next();
            String modelName = (String)me.getKey();
            List uuids = (List)me.getValue(); // <String- binding UUIDs> 
            List newList = new ArrayList(uuids.size()); // <ConnectorBindingNameAndUUID> 
            Iterator ix = uuids.iterator();
            while (ix.hasNext()) {
                String uuid = (String)ix.next();
                ConnectorBinding cb = vdb.getConnectorBindingByRouting(uuid);
                String bindingName = cb.getName();
                newList.add(new ConnectorBindingNameAndUUID(bindingName, uuid));
            }
            ConnectorBindingNameAndUUID[] bindingNames = new ConnectorBindingNameAndUUID[newList.size()];
            ix = newList.iterator();
            for (int i = 0; ix.hasNext(); i++) {
                bindingNames[i] = (ConnectorBindingNameAndUUID)ix.next();
            }
            newMap.put(modelName, bindingNames);
        }
        return newMap;        
    }
    
    private void setBindingsFromMap(Map bindingsMap) throws Exception {
        this.mapModelsToBindings = bindingsMap;
    }

    public void refreshBindingMap() {
        refreshBindingsTable(true);
    }

    // this allows the import process to add newly (to be created)
    // bindings to the list to be selected from
    private void setAdditionalBindings(Map addBindings) {
        if (addBindings == null || addBindings.isEmpty()) {
            return;
        }
        addTohmUUIDConnectorBindingsMap = new HashMap(addBindings.size());

        for (Iterator bit = addBindings.keySet().iterator(); bit.hasNext();) {
            String bname = (String)bit.next();
            ConnectorBinding cb = (ConnectorBinding)addBindings.get(bname);
            addTohmUUIDConnectorBindingsMap.put(cb.getRoutingUUID(), cb.getName());
        }

        addToListOfBindings = addBindings.values();

    }

    private void refreshBindingsTable(boolean refresh) {
        String sModelName = "";

        try {
            hmUUIDConnectorBindingsMap = getConnectorManager().getUUIDConnectorBindingsMap(true);
            if (addTohmUUIDConnectorBindingsMap != null) {
                hmUUIDConnectorBindingsMap.putAll(addTohmUUIDConnectorBindingsMap);
            }

        } catch (Exception e) {
            ExceptionUtility.showMessage("Failed retrieving UUID-ConnBind Map", e);
            vdbACBPInitSuccessful = false;
        }
        // if bindings are available:, also if they are un-assigned we should not populate
        if (refresh && mapModelsToBindings != null) {
            int iRowCount = tblModels.getModel().getRowCount();

            // walk the models table model:
            for (int ixRow = 0; ixRow < iRowCount; ixRow++) {
                // get the model name from this row:
                sModelName = (String)tblModels.getModel().getValueAt(ixRow, MODEL_COL_IN_TBL_MODELS);

                ConnectorBindingNameAndUUID[] bindings = (ConnectorBindingNameAndUUID[])mapModelsToBindings.get(sModelName);
                if (bindings != null) {
                    VDBConnectorBindingNames bindingNames = tblModels.getObjectForRow(ixRow);
                    bindingNames.setBindings(bindings);

                    // apply it to the table model for this model:
                    tblModels.reviseRow(bindingNames, ixRow);
                } else {
                    // no op
                }
            }
        }
        saveColumnSortInfo();
        setTableAttributes();
        restoreColumnSortOrder();
    }

    public void updateMultiSource(Map /* <String model name to Boolean multi-source selected> */multiSourceInfo) {
        tblModels.updateMultiSource(multiSourceInfo);
    }

    public boolean getVdbACBPSuccessful() {
        return vdbACBPInitSuccessful;
    }

    // ===
    private void setModelsTableListening() {
        tblModels.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
        smModelsSelModel = tblModels.getSelectionModel();
        smModelsSelModel.addListSelectionListener(new ListSelectionListener() {

            public void valueChanged(ListSelectionEvent e) {
                // Ignore extra messages.
                if (e.getValueIsAdjusting())
                    return;

                ListSelectionModel lsm = (ListSelectionModel)e.getSource();
                if (!lsm.isSelectionEmpty()) {
                    updateSelectionForModelsTable();
                } else {
                    updateDeselectionForModelsTable();
                }
            }
        });

        EnhancedTableColumnModel etcm = tblModels.getEnhancedColumnModel();
        etcm.addColumnSortListener(new TableColumnSortListener() {

            public void columnSorted() {
                tblModels.clearSelection();
                updateDeselectionForModelsTable();
            }
        });

    }

    public void updateSelectionForModelsTable() {
        aryModelsTableSelectedRows = tblModels.getSelectedRows();
        if (aryModelsTableSelectedRows.length > 0) {
            int i = 0;
            bAssignableRowSelectedInModelsTable = false;
            bUnassignableRowSelectedInModelsTable = false;
            while ((i < aryModelsTableSelectedRows.length)
                   && (!(bAssignableRowSelectedInModelsTable && bUnassignableRowSelectedInModelsTable))) {
                int modelRowIndex = tblModels.convertRowIndexToModel(aryModelsTableSelectedRows[i]);
                if (!tblModels.isRowMultiSource(modelRowIndex)) {
                    bAssignableRowSelectedInModelsTable = true;
                    if (tblModels.isAssigned(modelRowIndex)) {
                        bUnassignableRowSelectedInModelsTable = true;
                    }
                }
                i++;
            }
        } else {
            bAssignableRowSelectedInModelsTable = false;
            bUnassignableRowSelectedInModelsTable = false;
        }
        enableAssignButtons();
    }

    private void updateDeselectionForModelsTable() {
        bAssignableRowSelectedInModelsTable = false;
        bUnassignableRowSelectedInModelsTable = false;
        enableAssignButtons();
    }

    private void enableAssignButtons() {
        btnAssign.setEnabled(bAssignableRowSelectedInModelsTable && bRowSelectedInConnBindTable);
        btnUnassign.setEnabled(bUnassignableRowSelectedInModelsTable);
    }

    private void processAssignButton() {
        applyStringToSelectedModels(getSelectedConnBind());

        // now reselect the currently selected rows to get them to repaint
        reselectModelsTableSelectedRows();
        bUnassignableRowSelectedInModelsTable = true;
        enableAssignButtons();
    }

    private void reselectModelsTableSelectedRows() {
        int iTrueRow = 0;
        aryModelsTableSelectedRows = tblModels.getSelectedRows();

        if (aryModelsTableSelectedRows.length > 0) {
            for (int ixRow = 0; ixRow < aryModelsTableSelectedRows.length; ixRow++) {
                iTrueRow = aryModelsTableSelectedRows[ixRow];
                tblModels.addRowSelectionInterval(iTrueRow, iTrueRow);
            }
        }
    }

    private void processUnassignButton() {
        applyStringToSelectedModels("");
        bUnassignableRowSelectedInModelsTable = false;
        enableAssignButtons();
    }

    private void processNewBindingButton() {
        ServiceComponentDefn scdNewBinding = doNewBindingWizard();
        if (scdNewBinding != null) {
            loadAvailableBindings(scdNewBinding.getName());
        }
    }

    private void loadAvailableBindings(String bindingName) {
        tblConnBinds.setModel(getConnBindsTableModel());
        refreshBindingsTable(bindingName == null);

        // Then select its row:
        if (bindingName != null) {
            selectConnBindByName(bindingName);
        }
        // Inform WorkspaceController of new connector binding. If there
        // is a connector binding panel instantiated, it must be notified so
        // that it will update its bindings table.
        WorkspaceController.getInstance().handleUpdateNotification(connection, new AddedConnectorBindingNotification());

    }

    private void selectConnBindByName(String sConnBindName) {
        int iTargetRow = 0;
        boolean bFoundRow = false;

        // Determine which Connector Binding row holds this name:
        for (int iRow = 0; iRow < tblConnBinds.getRowCount(); iRow++) {
            String sAConnBindName = (String)tblConnBinds.getValueAt(iRow, CONN_BIND_COL_IN_TBL_CONN_BIND);

            if (sAConnBindName.equals(sConnBindName)) {
                iTargetRow = iRow;
                bFoundRow = true;
                break;
            }
        }

        // If the row was found, do 'setSelectionInterval' on it:
        if (bFoundRow) {
            tblConnBinds.getSelectionModel().setSelectionInterval(iTargetRow, iTargetRow);
        }
    }

    private ServiceComponentDefn doNewBindingWizard() {
        NewBindingWizardController controller = new NewBindingWizardController(connection);
        ServiceComponentDefn binding = controller.runWizard();
        return binding;
    }

    private void applyStringToSelectedModels(String s) {
        aryModelsTableSelectedRows = tblModels.getSelectedRows();
        ConnectorBinding cb = (ConnectorBinding)hmConnBindsXref.get(s);
        String uuid = null;
        if (cb != null) {
            uuid = cb.getRoutingUUID();
        }
        for (int ixRow = 0; ixRow < aryModelsTableSelectedRows.length; ixRow++) {
            int iTrueRow = aryModelsTableSelectedRows[ixRow];
            VDBConnectorBindingNames bindingInfo = (VDBConnectorBindingNames)tblModels.getValueAt(iTrueRow,
                                                                                                  CONN_BIND_COL_IN_TBL_MODELS);
            if (!bindingInfo.isMultiSource()) {
                bindingInfo = bindingInfo.copy();
                ConnectorBindingNameAndUUID[] newBindings;
                if (s.trim().length() > 0) {
                    newBindings = new ConnectorBindingNameAndUUID[1];
                    newBindings[0] = new ConnectorBindingNameAndUUID(s, uuid);
                } else {
                    newBindings = new ConnectorBindingNameAndUUID[0];
                }
                bindingInfo.setBindings(newBindings);
                tblModels.reviseRow(bindingInfo);
            }
        }
        tblModels.repaint();
    }

    public Map /* <String model name to Collection of String (UUIDs)> */
    getModelsToConnectorBindingsMap() {
        HashMap hmap = new HashMap();

        com.metamatrix.toolbox.ui.widget.table.DefaultTableModel tmdl = (DefaultTableModel)tblModels.getModel();

        int iRowCount = tmdl.getRowCount();
        int iModelColumn = 0;
        int iConnBindColumn = 1;
        String sModelName = "";
        String sParsedModelName = "";

        // then on the tablemodel: getRowCount(), getValueAt (row, col)

        for (int x = 0; x < iRowCount; x++) {
            sModelName = (String)tmdl.getValueAt(x, iModelColumn);
            sParsedModelName = parseModelName(sModelName);
            VDBConnectorBindingNames bindings = (VDBConnectorBindingNames)tmdl.getValueAt(x, iConnBindColumn);
            ConnectorBindingNameAndUUID[] bindingNames = bindings.getBindings();
            List bindingUUIDs = new ArrayList(bindingNames.length);
            for (int i = 0; i < bindingNames.length; i++) {
                bindingUUIDs.add(bindingNames[i].getUUID());
            }
            hmap.put(sParsedModelName, bindingUUIDs);
        }
        return hmap;
    }

    private String parseModelName(String sModelNameDottedName) {
        // if has single dot, drop dot and following
        String sWorkString = sModelNameDottedName;
        String sDot = ".";
        String sResult = "";

        // 1. parse out the first component that remains, before
        // the first '.'
        int iDotPos = sWorkString.indexOf(sDot);

        if (iDotPos > -1)
            sWorkString = sWorkString.substring(0, iDotPos);

        sResult = sWorkString;

        return sResult;
    }

    private void setTableAttributes() {
        tblConnBinds.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        tblConnBinds.setColumnSelectionAllowed(false);
        tblConnBinds.setEditable(false);
        tblConnBinds.sizeColumnsToFitData(100);
        tblConnBinds.setRowHeight(VDBConnectorBindingAssignmentModelTable.ROW_HEIGHT);

        tblModels.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        tblModels.setColumnSelectionAllowed(false);
        // tblModels.sizeColumnsToFitData(100);
    }

    private void jbInit() throws Exception {
        tblModels = new VDBConnectorBindingAssignmentModelTable(this);

        setTableAttributes();

        this.setLayout(borderLayout1);
        pnlOuter.setLayout(new GridBagLayout());
        scpConnBindScroller.setPreferredSize(new Dimension(300, 220));
        btnAssign.setText("Assign >");
        btnUnassign.setText("< Unassign");
        btnNewConnectorBinding.setText("New...");

        btnAssign.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent event) {
                processAssignButton();
            }
        });

        btnUnassign.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent event) {
                processUnassignButton();
            }
        });

        btnNewConnectorBinding.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent event) {
                processNewBindingButton();
            }
        });

        btnAssign.setEnabled(false);

        btnUnassign.setEnabled(false);
        btnNewConnectorBinding.setEnabled(true);

        pnlModelsTable.setLayout(borderLayout2);
        pnlConnBindTable.setLayout(borderLayout3);
        this.add(pnlOuter, BorderLayout.CENTER);
        pnlConnBindTable.add(scpConnBindScroller, BorderLayout.CENTER);
        scpConnBindScroller.getViewport().add(tblConnBinds, null);

        pnlAssignButtons.setLayout(new GridLayout(3, 1, 5, 15));
        pnlAssignButtons.add(btnAssign, null);
        pnlAssignButtons.add(btnUnassign, null);
        pnlAssignButtons.add(btnNewConnectorBinding, null);

        tblConnBinds.setModel(getConnBindsTableModel());
        JScrollPane scpModelsScroller = new JScrollPane(tblModels);
        pnlModelsTable.add(scpModelsScroller, BorderLayout.CENTER);
        tblModels.setContainerScrollPane(scpModelsScroller);

        sortConnBindTable();

        pnlOuter.add(pnlConnBindTable, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0, GridBagConstraints.EAST,
                                                              GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));

        pnlOuter.add(pnlAssignButtons, new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                                                              GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));

        pnlOuter.add(pnlModelsTable, new GridBagConstraints(2, 0, 1, 1, 2.0, 1.0, GridBagConstraints.WEST,
                                                            GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));

        setTableAttributes();
    }

    private void sortConnBindTable() {
        // Connector Binding Table
        EnhancedTableColumnModel etcmConnBinds = tblConnBinds.getEnhancedColumnModel();
        // TableColumn clmConnBindColumn =
        etcmConnBinds.getColumn(CONN_BIND_COL_IN_TBL_CONN_BIND);
    }

    private void sortModelsTable() {
        // Models Table
        EnhancedTableColumnModel etcmModels = tblModels.getEnhancedColumnModel();
        // TableColumn clmModelsColumn =
        etcmModels.getColumn(MODEL_COL_IN_TBL_MODELS);
    }

    public void setModels(Collection /* <ModelWrapper> */colModels) {
        // this is the key method: the owner of the panel MUST call
        // this. For the wizard, the models belong to the DTC;
        // for the ongoing Edit Conn Binds dialog, the models
        // belong to the selected VDB.

        // We will pare down the collection to only those models requiring a connector
        // binding.
        Collection displayableModels = getDisplayableModels(colModels);

        setModelsTableData(displayableModels);

        setTableAttributes();

        sortModelsTable();
        sortConnBindTable();
    }

    private Collection /* <ModelWrapper> */getDisplayableModels(Collection /* <ModelWrapper> */models) {
        Collection displayableModels = new ArrayList();
        Iterator it = models.iterator();
        while (it.hasNext()) {
            ModelWrapper mw = (ModelWrapper)it.next();
            boolean isDisplayable = mw.requiresConnectorBinding();
            if (isDisplayable) {
                displayableModels.add(mw);
            }
        }
        return displayableModels;
    }

    private void setModelsTableData(Collection /* <ModelWrapper> */colModels) {
        Map hmConnBindUUIDToName = null;
        try {
            hmConnBindUUIDToName = getConnectorManager().getUUIDConnectorBindingsMap(true);
        } catch (Exception ex) {
            String msg = "Exception retrieving existing connector bindings.";
            LogManager.logError(LogContexts.CONNECTOR_BINDINGS, ex, msg);
            ExceptionUtility.showMessage(msg, ex);
        }
        if (hmConnBindUUIDToName != null) {
            List rows = new ArrayList();
            Iterator it = colModels.iterator();
            while (it.hasNext()) {
                ModelWrapper mdlWrapper = (ModelWrapper)it.next();
                String modelName = mdlWrapper.getName();
                boolean supportsMultiSource = mdlWrapper.isMultiSourceBindingsEnabled();
                List /* <String> */bindingUUIDs = mdlWrapper.getConnectorBindingNames();
                List /* <String> */bindingsList = new ArrayList(bindingUUIDs.size());
                Iterator iter = bindingUUIDs.iterator();
                while (iter.hasNext()) {
                    String bindingUUID = (String)iter.next();
                    String bindingName = (String)hmConnBindUUIDToName.get(bindingUUID);
                    if ((bindingName != null) && (bindingName.trim().length() > 0)) {
                        bindingsList.add(new ConnectorBindingNameAndUUID(bindingName, bindingUUID));
                    }
                }
                ConnectorBindingNameAndUUID[] bindingsArray = new ConnectorBindingNameAndUUID[bindingsList.size()];
                iter = bindingsList.iterator();
                for (int i = 0; iter.hasNext(); i++) {
                    bindingsArray[i] = (ConnectorBindingNameAndUUID)iter.next();
                }
                rows.add(new VDBConnectorBindingNames(modelName, bindingsArray, supportsMultiSource));
            }
            it = rows.iterator();
            VDBConnectorBindingNames[] array = new VDBConnectorBindingNames[rows.size()];
            for (int i = 0; it.hasNext(); i++) {
                array[i] = (VDBConnectorBindingNames)it.next();
            }
            tblModels.populate(array);
        }
    }

    private DefaultTableModel getConnBindsTableModel() {

        DefaultTableModel tmdl = new DefaultTableModel(new Vector(Arrays.asList(aryCBColNames)));

        // retrieve the available Connector Bindings
        try {
            arylConnBinds /* ServiceComponentDefns */
            = getConnectorManager().getConnectorBindings(false);
        } catch (Exception e) {
            ExceptionUtility.showMessage("Failed to retrieve Connector Bindings", e);
            vdbACBPInitSuccessful = false;
        }

        hmConnBindsXref.clear();

        Iterator itConnBinds = arylConnBinds.iterator();

        while (itConnBinds.hasNext()) {
            ServiceComponentDefn scdTemp = ((ConnectorAndBinding)itConnBinds.next()).getBinding();
            hmConnBindsXref.put(scdTemp.toString(), scdTemp);

            Vector vCBRow = new Vector();
            vCBRow.add(scdTemp.toString());
            tmdl.addRow(vCBRow.toArray());
        }
        if (addToListOfBindings != null) {
            for (Iterator addIT = addToListOfBindings.iterator(); addIT.hasNext();) {
                ConnectorBinding cb = (ConnectorBinding)addIT.next();
                String key = cb.toString();
                if (!hmConnBindsXref.containsKey(key)) {
                    hmConnBindsXref.put(key, cb);
                    Vector vCBRow = new Vector();
                    vCBRow.add(cb.toString());
                    tmdl.addRow(vCBRow.toArray());
                }
            }
        }
        return tmdl;
    }

    private void setInitialTableSorting() {
        EnhancedTableColumn bndgColumn = (EnhancedTableColumn)tblConnBinds.getColumn(CONNECTOR_BINDINGS_HDR);
        tblConnBinds.setColumnSortedAscending(bndgColumn, false);
        EnhancedTableColumn mdlNameColumn = (EnhancedTableColumn)tblModels.getColumn(MODEL_NAME_HDR);
        tblModels.setColumnSortedAscending(mdlNameColumn, false);
        saveColumnSortInfo();
    }

    private void saveColumnSortInfo() {
        connBindsSortInfo = ColumnSortInfo.getTableColumnSortInfo(tblConnBinds);
        modelsSortInfo = ColumnSortInfo.getTableColumnSortInfo(tblModels);
    }

    private void restoreColumnSortOrder() {
        ColumnSortInfo.setColumnSortOrder(connBindsSortInfo, tblConnBinds);
        ColumnSortInfo.setColumnSortOrder(modelsSortInfo, tblModels);
    }

    public void editRequested(VDBConnectorBindingNames bindingInfo) {
        String modelName = bindingInfo.getModelName();
        // Create a copy of the already assigned bindings
        ConnectorBindingNameAndUUID[] assignedBindings = bindingInfo.getBindings();
        List /* <ConnectorBindingNameAndUUID> */availableBindingsInfo = null;
        availableBindingsInfo = getAvailableBindings();
        List /* <String binding name> */availableBindingNames = new ArrayList(availableBindingsInfo.size());
        Map bindingNameToUUIDMap = new HashMap();
        Iterator it = availableBindingsInfo.iterator();
        while (it.hasNext()) {
            ConnectorBindingNameAndUUID binding = (ConnectorBindingNameAndUUID)it.next();
            String bindingName = binding.getBindingName();
            String uuid = binding.getUUID();
            availableBindingNames.add(bindingName);
            bindingNameToUUIDMap.put(bindingName, uuid);
        }
        List assignedBindingNames = new ArrayList(assignedBindings.length);
        for (int i = 0; i < assignedBindings.length; i++) {
            assignedBindingNames.add(assignedBindings[i].getBindingName());
        }
        VDBMultiConnectorBindingAssignmentDlg dlg = new VDBMultiConnectorBindingAssignmentDlg(ViewManager.getMainFrame(),
                                                                                              modelName, availableBindingNames,
                                                                                              assignedBindingNames,
                                                                                              bindingNameToUUIDMap, connection);
        dlg.show();
        if (!dlg.wasCanceled()) {
            // Update the table object (VDBConnectorBindingNames) for this model.
            // Will be saved upon exiting the parent dialog.
            ConnectorBindingNameAndUUID[] selectedBindings = dlg.getSelectedBindings();
            bindingInfo.setBindings(selectedBindings);
            tblModels.reviseRow(bindingInfo);
        }
    }

    private List /* <ConnectorBindingNameAndUUID> */getAvailableBindings() {
        List /* <String> */availableBindings = new ArrayList();
        Iterator it = hmConnBindsXref.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry me = (Map.Entry)it.next();
            String bindingName = (String)me.getKey();
            ConnectorBinding cb = (ConnectorBinding)me.getValue();
            String uuid = cb.getRoutingUUID();
            ConnectorBindingNameAndUUID newItem = new ConnectorBindingNameAndUUID(bindingName, uuid);
            availableBindings.add(newItem);
        }
        return availableBindings;
    }

    public void setMigratedBindings(Map bindings) throws Exception {        
        Map newMap = new HashMap(); // <String model name to ConnectorBindingNameAndUUID[]> 
        Iterator it = bindings.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry me = (Map.Entry)it.next();
            String modelName = (String)me.getKey();
            List uuids = (List)me.getValue(); // <String- binding UUIDs>  
            List newList = new ArrayList(uuids.size()); // <ConnectorBindingNameAndUUID> 
            Iterator ix = uuids.iterator();
            while (ix.hasNext()) {
                String uuid = (String)ix.next();
                ConnectorBinding cb = null;
                try {
                    cb = getConnectorManager().getConnectorBindingByUUID(uuid);
                } catch (Exception ex) {
                    throw ex;
                }
                if (cb != null) {
                    String bindingName = cb.getName();
                    newList.add(new ConnectorBindingNameAndUUID(bindingName, uuid));
                }
            }
            ConnectorBindingNameAndUUID[] bindingNames = new ConnectorBindingNameAndUUID[newList.size()];
            ix = newList.iterator();
            for (int i = 0; ix.hasNext(); i++) {
                bindingNames[i] = (ConnectorBindingNameAndUUID)ix.next();
            }
            newMap.put(modelName, bindingNames);
        }
        
        // now save this migrated bindings
        this.migratedBindings = newMap;
    }
    
    void switchConnectorBindings(boolean useMigratedBindings) {
        boolean exceptionOccurred = false;
        try {
            if (useMigratedBindings && this.migratedBindings != null) {
                setBindingsFromMap(this.migratedBindings);
            }
            else {
                setBindingsFromMap(this.vdbBindings);
            }
        } catch (Exception ex) {
            exceptionOccurred = true;
            String msg = "Exception retrieving existing connector bindings for migration.";
            LogManager.logError(LogContexts.CONNECTOR_BINDINGS, ex, msg);
            ExceptionUtility.showMessage(msg, ex);
        }
        if (!exceptionOccurred) {
            refreshBindingMap();
        }
    }    
    
}// end VdbAssignConnBindPanel
