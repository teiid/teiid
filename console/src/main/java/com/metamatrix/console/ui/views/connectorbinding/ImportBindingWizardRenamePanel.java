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

package com.metamatrix.console.ui.views.connectorbinding;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import javax.swing.AbstractButton;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.event.*;
import javax.swing.event.TableModelListener;
import javax.swing.table.*;


import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.models.ConnectorManager;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.ui.views.connector.ImportWizardController;
import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;
import com.metamatrix.console.ui.views.deploy.util.DeployTableSorter;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.CheckBox;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumnModel;

// ===

public class ImportBindingWizardRenamePanel
        extends BasicWizardSubpanelContainer
     implements ActionListener,
                TableModelListener 
                            {
    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    private static /*final*/ String[] SERVICE_HDRS;
    private static final int CURRENT_NAME_COL           = 0;
    private static final int ALREADY_EXIST_COL          = 1;
    private static final int NEW_NAME_COL               = 2;
    private static final int DO_NOT_IMPORT              = 3;
    private static final int COL_COUNT                  = 4;
    
// binding states
    private static final int OK_TO_IMPORT = 1;
    private static final int DO_NO_IMPORT = 0;
    private static final int INVALID_ROW = -1;
    
    private static final String PANEL_TITLE="Specify a new name for existing or other binding(s) or select not to import the binding(s).";//$NON-NLS-1$
    
    public static /*final*/ SimpleDateFormat DATE_FORMATTER;

    ///////////////////////////////////////////////////////////////////////////
    // INITIALIZER
    ///////////////////////////////////////////////////////////////////////////

    static {
        SERVICE_HDRS = new String[COL_COUNT];
        SERVICE_HDRS[CURRENT_NAME_COL]      = "Current Name"; //$NON-NLS-1$
        SERVICE_HDRS[ALREADY_EXIST_COL]     = "Exist"; //$NON-NLS-1$
        SERVICE_HDRS[NEW_NAME_COL]          = "New Name"; //$NON-NLS-1$
        SERVICE_HDRS[DO_NOT_IMPORT]         = "Do Not Import"; //$NON-NLS-1$

        String pattern = DeployPkgUtils.getString("pfp.datepattern", true); //$NON-NLS-1$
        if (pattern == null) {
            pattern = "MMM dd, yyyy hh:mm:ss"; //$NON-NLS-1$
        }
        DATE_FORMATTER = new SimpleDateFormat(pattern);
    }

    ///////////////////////////////////////////////////////////////////////////
    // CONTROLS
    ///////////////////////////////////////////////////////////////////////////

	private TableWidget tblBindings;

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

	private DefaultTableModel tmdlBindings;

    private HashMap mapDoesBindingExist           = new HashMap();

    private CheckBox donotimportchk; 
    
    private LabelWidget lblConnectorFileName        = new LabelWidget("File Name:"); //$NON-NLS-1$
    private TextFieldWidget txfConnectorFileName    = new TextFieldWidget();
    private JPanel pnlOuter                     = new JPanel();

	private JPanel pnlTable         = new JPanel();

	private ConnectorManager connectorManager;
    private Configuration config;
    
    private int numOfBindingsToImport = 0;
    // each row will have a state @see #OK_TO_IMPORT
    private int[] bindingState;
    

    private boolean isDisplayingWarning = false;
    

    public ImportBindingWizardRenamePanel(WizardInterface wizardInterface, ConnectorManager connecMgr) {
        super(wizardInterface);
        connectorManager = connecMgr;
        init();
    }


    private void init() {
        createTablePanel();
        txfConnectorFileName.setEditable(false);
        Insets insDefault = new Insets(3, 3, 3, 3);

        pnlOuter.setLayout(new GridBagLayout());

        pnlOuter.add(lblConnectorFileName, new GridBagConstraints(0, 1, 1, 1, 
        		0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE, 
        		insDefault, 0, 0));
        pnlOuter.add(txfConnectorFileName, new GridBagConstraints(1, 1, 1, 1, 
        		1.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, 
        		insDefault, 0, 0));

        pnlOuter.add(pnlTable, new GridBagConstraints(0, 2, 
        		GridBagConstraints.REMAINDER, GridBagConstraints.REMAINDER, 
        		1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, 
        		new Insets(12, 3, 3, 3), 0, 0));

        setMainContent(pnlOuter);
        setStepText(2, PANEL_TITLE);
	}

    private JPanel createTablePanel() {
        pnlTable = new JPanel();
        pnlTable.setLayout(new GridLayout(1, 1));        
        tblBindings = new TableWidget();
        tblBindings.setComparator(new DeployTableSorter()); 
                

		JScrollPane spnServices = new JScrollPane(tblBindings);
        pnlTable.add(spnServices);

        return pnlTable;
    }
    
    
    
    public void setConnectorBindings(String fileName, Collection bindingNames) {
        if (bindingState != null) {
            tmdlBindings.removeTableModelListener(this);
        }
        
        numOfBindingsToImport = 0;
        mapDoesBindingExist.clear();
        bindingState = null;
        
        txfConnectorFileName.setText(
                ImportWizardController.stripFileExtensionFromName(fileName));
        
        
        tmdlBindings = DeployPkgUtils.setup(
                tblBindings,
                SERVICE_HDRS,
                bindingNames.size(),
                    new int[] {NEW_NAME_COL, DO_NOT_IMPORT});
        
 
        populateTable(bindingNames);                
        tmdlBindings.addTableModelListener(this);
        
        doTableSetup();
        resolveForwardButton();        
                
    }
    
    private boolean okToMoveForward() {
        numOfBindingsToImport = 0;       
        int numRows = tblBindings.getRowCount();
        boolean anyToImport = false;
        for (int i = 0; i < numRows; i++) {
            switch (bindingState[i]) {            
                case OK_TO_IMPORT: {
                    anyToImport = true;
                    ++numOfBindingsToImport;
                    break;
                }
                case DO_NO_IMPORT: {
                    // DO NOT IMPORTS are still ok to moveforward
                    // they just wont be imported
                    break;
                }
                
                default: {
                    // any invalid row is not ok to move forward
                    return false;
                }
            }
        }
        
        return anyToImport;
         
    }
    
    
    private int isValidRow(int row) {
        // 1 = ok to import
        // -1 = invalid
        // 0 = do not import
        if (row < 0) {
            return INVALID_ROW;
        }
              
            Boolean doNotImportBool = (Boolean)tblBindings.getModel().getValueAt(row,
                DO_NOT_IMPORT);
            
            if (doNotImportBool == null || doNotImportBool.booleanValue()) {
                return DO_NO_IMPORT;
                
            } 
            Boolean existBool = (Boolean)tblBindings.getModel().getValueAt(row,
                     ALREADY_EXIST_COL);
            
            String newName = (String)tblBindings.getModel().getValueAt(row,
                    NEW_NAME_COL);
            
            if (newName != null) {
                newName = newName.trim();
            }
            
            String binding = 
                    (String)tblBindings.getModel().getValueAt(row,
                        CURRENT_NAME_COL);
            
            // if the binding already exist and no name is provided
            // then present message
            if ( (existBool.booleanValue()) && (newName != null) && newName.length() == 0) {
                String hdr = "Existing Connector Binding Error"; //$NON-NLS-1$

                String msg = "The connector binding " + binding + " already exists.  " //$NON-NLS-1$ //$NON-NLS-2$
                         + "Please enter a unique name."; //$NON-NLS-1$
                
                
                displayWarning(hdr, msg);
                return INVALID_ROW;
                
            }
            
            if (newName != null && newName.length() > 0) {
                try {
                    if (doesBindingExist(newName)) {
                            String hdr = "Existing Connector Binding Error"; //$NON-NLS-1$

                            String msg = "The new name " + newName + " entered for connector binding " + binding + " already exists.  " //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                                     + "Please enter another name."; //$NON-NLS-1$


                            displayWarning(hdr, msg);
                            
                            return INVALID_ROW;
                        
                    }
                } catch (Exception err) {
                    ExceptionUtility.showMessage("Error verifying if binding exists", //$NON-NLS-1$
                        err);
                    LogManager.logError(
                            LogContexts.CONNECTOR_BINDINGS,
                            err,
                            getClass() + ":setDomainObject"); //$NON-NLS-1$
                        return INVALID_ROW;
                        
                    }
                }
 


        return OK_TO_IMPORT;
        
    }
    
    
    /**
     * Display a warning message, if we're not already displaying one.
     * @param header
     * @param message
     * @since 4.3
     */
    private void displayWarning(String header, String message) {
        //Check that we're not already displaying a warning.
        //This prevents recursive behavior: creating a modal dialog causes a FocusLostEvent,
        //which causes isValidRow() to be called, which displays another warning...
        if (! isDisplayingWarning) {
            isDisplayingWarning = true;
            StaticUtilities.displayModalDialogWithOK(header, message);
            isDisplayingWarning = false;
        }
    }

    
    
    public Map getConnectorBindingMapping() {
       
        int numRows = tblBindings.getRowCount();
        Map bindings = new HashMap(numRows);

        for (int i = 0; i < numRows; i++) {
            Boolean doNotImportBool = (Boolean)tblBindings.getModel().getValueAt(i,
                DO_NOT_IMPORT);
            
            if (doNotImportBool != null && doNotImportBool.booleanValue()) {
                
            } else {
               
                String newName = (String)tblBindings.getModel().getValueAt(i,
                        NEW_NAME_COL);
                
                String binding = 
                        (String)tblBindings.getModel().getValueAt(i,
                            CURRENT_NAME_COL);
                
                // if the binding isn't renamed, then assign the original name
                // - each key is assumed to have a mapped value 
                if (newName == null || newName.trim().length() == 0) {
                    newName = binding;
                }
                
                bindings.put(binding, newName);
            }
        }

        return bindings;
    }

    private void populateTable(Collection bindings) {
        mapBindingExists(bindings);

        bindingState = new int[bindings.size()];
        tmdlBindings.setRowCount(0);
        try {

            // Process Next Startup set
            int i = 0;
            if (mapDoesBindingExist != null) {
                Iterator itBinding
                    = mapDoesBindingExist.keySet().iterator();

                // drive the process by walking the NextStartup hashmap
                while (itBinding.hasNext()) {
                    String binding =
                        (String)itBinding.next();

                    Vector row = new Vector(SERVICE_HDRS.length);
                    row.setSize(SERVICE_HDRS.length);

                    row.setElementAt(binding, CURRENT_NAME_COL);

                    String newName =
                        (String)mapDoesBindingExist.get(binding);

                    Boolean bindingexist;
                    if (newName == null) {
                        bindingexist = Boolean.FALSE;
                    } else {
                        bindingexist = Boolean.TRUE;
                    }

                    row.setElementAt(bindingexist, ALREADY_EXIST_COL);
                    
                    row.setElementAt(newName, NEW_NAME_COL);
                    
                    row.setElementAt(Boolean.FALSE, DO_NOT_IMPORT);
                    
                    tmdlBindings.addRow(row);
                    
                    // set initial state of the binding
                    bindingState[i] = OK_TO_IMPORT;
                    
                    
                    TableCellEditor editor = tblBindings.getCellEditor(i, DO_NOT_IMPORT);
                    donotimportchk = (CheckBox)editor.getTableCellEditorComponent(
                            tblBindings,
                            Boolean.FALSE,
                            false,
                            i,
                            DO_NOT_IMPORT);
                    donotimportchk.addActionListener(this);
                    
                    ++i;
                    
                    
                }
            }
		} catch (Exception theException) {
            ExceptionUtility.showMessage("  ", //$NON-NLS-1$
                //getString("msg.configmgrproblem",
                //          new Object[] {getClass(), "setDomainObject"}),
                theException);
            LogManager.logError(
                LogContexts.CONNECTOR_BINDINGS,
                theException,
                getClass() + ":setDomainObject"); //$NON-NLS-1$
        }
	}

    private void doTableSetup() {
        // customize the table
        tblBindings.sizeColumnsToFitData();

        // fix column for Next Startup
        EnhancedTableColumnModel etcm = 
        		tblBindings.getEnhancedColumnModel();
        TableColumn column = 
            etcm.getColumn(ALREADY_EXIST_COL);
        tblBindings.sizeColumnToFitData(
        		(EnhancedTableColumn)column);
        
        column = 
            etcm.getColumn(DO_NOT_IMPORT);
        tblBindings.sizeColumnToFitData(
                (EnhancedTableColumn)column);
        
        column = 
            etcm.getColumn(CURRENT_NAME_COL);
        tblBindings.sizeColumnToFitData(
                (EnhancedTableColumn)column);
        
        column = 
            etcm.getColumn(NEW_NAME_COL);
        tblBindings.sizeColumnToFitData(
                (EnhancedTableColumn)column);        
        
		sortTable();
    }

    private void sortTable() {
        EnhancedTableColumnModel etcmBindings = tblBindings.getEnhancedColumnModel();
        TableColumn clmCurrentColumn = etcmBindings.getColumn(CURRENT_NAME_COL);
        etcmBindings
            .setColumnSortedAscending((EnhancedTableColumn)clmCurrentColumn, false);
    }

    /**
     * This mapping will indicate if a binding already exists and 
     * places a new name in the column for suggestion purposes only
     * to indicate it needs to be changed 
     * @param bindings
     * @since 4.2
     */
    
	private void mapBindingExists(Collection bindings) {
		mapDoesBindingExist.clear();
        try {
        	config = getNextStartupConfig();
            
//            Collection colPsc = connectorManager.getAllConnectorsPSCsByConfig(
//            		config);

 //           if (colPsc != null) {
                Iterator itBinding = bindings.iterator();
                while (itBinding.hasNext()) {
                    String binding =
                        (String)itBinding.next();

                    String name = null;
                    // if the binding already exist, then create a new
                    // name to indicate the binding name must be changed
                    // inorder to import it
                    if (config.getConnectorBinding(binding) != null) {
                        name = nextBindingName(binding);
               		} 
					mapDoesBindingExist.put(binding, name);
				}
 //           }
        } catch (Exception theException) {
            ExceptionUtility.showMessage("  ", //$NON-NLS-1$
                //getString("msg.configmgrproblem",
                //          new Object[] {getClass(), "setDomainObject"}),
                theException);
            LogManager.logError(LogContexts.CONNECTOR_BINDINGS, theException,
            		getClass() + ":setDomainObject"); //$NON-NLS-1$
        }
	}
    
    private String nextBindingName(String bindingName) throws Exception {
        
        int next = 1;
        boolean foundName = false;
        while(!foundName) {
            String newName = bindingName + "_" + next;//$NON-NLS-1$
            if (!doesBindingExist(newName)) {
                return newName;
            }
            ++next;
        }
        
        return "ChangeTheBindingName";//$NON-NLS-1$
    }
    
    private boolean doesBindingExist(String sName) throws Exception {
        return connectorManager.connectorBindingNameAlreadyExists(sName);
          }

        
    
    public void tableChanged(TableModelEvent theEvent) {
               
        
		int iSelectedRow = theEvent.getFirstRow();
        if (iSelectedRow == -1) {
            return;
        }
        AbstractButton forwardButton = getWizardInterface().getForwardButton();
        int isvalid = isValidRow(iSelectedRow);
        
        bindingState[iSelectedRow] = isvalid;
        
        forwardButton.setEnabled((okToMoveForward())); 
                
	}
    
    
    /**
     * If a check box is checked/unchecked, call stopCellEditing() so that the table is updated.
     * @see java.awt.event.ActionListener#actionPerformed(java.awt.event.ActionEvent)
     * @since 4.3
     */
    public void actionPerformed(ActionEvent theEvent) {
     
       Object source = theEvent.getSource();
       if (source instanceof CheckBox) {
           int row = tblBindings.getSelectedRow();
           int column = tblBindings.getSelectedColumn();
           
           if (row != -1 || column != -1) {
               tblBindings.getCellEditor(row, column).stopCellEditing();
           }
           
           
           AbstractButton forwardButton = getWizardInterface().getForwardButton();           
           forwardButton.setEnabled((okToMoveForward())); 
       }
    }    
    
    
    
    public void resolveForwardButton() {
        AbstractButton forwardButton = getWizardInterface().getForwardButton();
        forwardButton.setEnabled((okToMoveForward())); //$NON-NLS-1$
    }    
    


    

	private Configuration getNextStartupConfig() {
        Configuration cfg =  null;
        try {
            cfg = connectorManager.getNextStartupConfig();
        } catch (Exception e) {
            ExceptionUtility.showMessage(
            		"Failed retrieving the Next Startup Config", e); //$NON-NLS-1$
        }
        return cfg;
    }
    
}