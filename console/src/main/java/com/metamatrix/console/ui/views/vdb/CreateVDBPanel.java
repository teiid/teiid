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

import java.awt.Component;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.swing.AbstractButton;
import javax.swing.JOptionPane;
import javax.swing.JPanel;

import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.tree.directory.DirectoryEntry;
import com.metamatrix.common.tree.directory.FileSystemEntry;
import com.metamatrix.common.tree.directory.FileSystemFilter;
import com.metamatrix.common.tree.directory.FileSystemView;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.common.vdb.api.VDBDefn;
import com.metamatrix.common.vdb.api.VDBInfo;
import com.metamatrix.common.vdb.api.VDBStreamImpl;
import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConfigurationManager;
import com.metamatrix.console.models.ConnectorManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.models.RuntimeMgmtManager;
import com.metamatrix.console.models.VdbManager;
import com.metamatrix.console.notification.DataEntitlementChangeNotification;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.layout.WorkspaceController;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.ConsoleConstants;
import com.metamatrix.console.ui.util.MDCPOpenStateListener;
import com.metamatrix.console.ui.util.ModifiedDirectoryChooserPanel;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.ui.util.WizardInterfaceImpl;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.vdb.VDBStatus;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.metadata.runtime.exception.VirtualDatabaseDoesNotExistException;
import com.metamatrix.metadata.runtime.exception.VirtualDatabaseException;
import com.metamatrix.platform.admin.api.EntitlementMigrationReport;
import com.metamatrix.server.admin.api.MaterializationLoadScripts;
import com.metamatrix.server.admin.api.RuntimeMetadataAdminAPI;
import com.metamatrix.toolbox.preference.UserPreferences;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.DirectoryChooserPanel;
import com.metamatrix.vdb.runtime.BasicModelInfo;
import com.metamatrix.vdb.runtime.BasicVDBDefn;

/**
 * Replaces the old ImportVdbVersionWizardPanel, NewVdbVersionWizardPanel, and NewVdbWizardPanel, which among them had way too
 * much duplicated code.
 * 
 * @since 4.2
 */
public class CreateVDBPanel extends WizardInterfaceImpl {

    private final static String CONNECTION_PROPERTIES_FILE_DESC = 
        ConsolePlugin.Util.getString("VdbWizardSaveMaterializationFilesPanel.connectionPropertiesFile"); //$NON-NLS-1$
    private final static String CREATE_SCRIPT_FILE_DESC = 
        ConsolePlugin.Util.getString("VdbWizardSaveMaterializationFilesPanel.createScriptFile"); //$NON-NLS-1$
    private final static String LOAD_SCRIPT_FILE_DESC = 
        ConsolePlugin.Util.getString("VdbWizardSaveMaterializationFilesPanel.loadScriptFile"); //$NON-NLS-1$
    private final static String SWAP_SCRIPT_FILE_DESC = 
        ConsolePlugin.Util.getString("VdbWizardSaveMaterializationFilesPanel.swapScriptFile"); //$NON-NLS-1$
    private final static String TRUNCATE_SCRIPT_FILE_DESC = 
        ConsolePlugin.Util.getString("VdbWizardSaveMaterializationFilesPanel.truncateScriptFile"); //$NON-NLS-1$
    
    private final static int NUM_MATERIALIZATION_FILES = 5;


    private final static int ID_STEP_NUM = 2;

    final static String[] VDB_IMPORT_EXTENSIONS = new String[] {
        "def", "vdb"}; //$NON-NLS-1$ //$NON-NLS-2$

    final static String VDB_TYPE_FILE_DESC = "VDB Import Types (*.def, *.vdb)"; //$NON-NLS-1$

    /**
     * No extra pages have been added yet
     */
    private final static int IMPORT_INITIALIZED_NONE = -1;
    /**
     * Extra pages for new VDB have been added
     */
    private final static int IMPORT_INITIALIZED_NEW_VDB = -2;
    /**
     * Extra pages for new version have been added
     */
    private final static int IMPORT_INITIALIZED_NEW_VERSION = -3;

    public static Map /*
                         * <String (model name) to Short (MetadataConstants.VISIBILITY_TYPES)>
                         */modelVisibilityMap(ModelVisibilityInfo[] visInf) {
        Map map = new HashMap();
        if (visInf != null) {
            for (int i = 0; i < visInf.length; i++) {
                String modelName = visInf[i].getModelName();
                short visibilityType;
                if (visInf[i].isVisible()) {
                    visibilityType = ModelInfo.PUBLIC;
                } else {
                    visibilityType = ModelInfo.PRIVATE;
                }
                map.put(modelName, new Short(visibilityType));
            }
        }
        return map;
    }

    public static Map /*
                         * <String (model name) to Boolean (multi-source enabled flag)>
                         */modelMultiSourceMap(ModelVisibilityInfo[] visInf) {
        Map map = new HashMap();
        if (visInf != null) {
            for (int i = 0; i < visInf.length; i++) {
                String modelName = visInf[i].getModelName();
                Boolean sourceEnabled = new Boolean(visInf[i].isMultipleSourcesSelected());
                map.put(modelName, sourceEnabled);
            }
        }
        return map;
    }

    ImportFileSelectionPanel selectorPanel;
    VdbWizardEditConnBindPanel pnlEditConnBind = null;
    VdbWizardConfirmPanel pnlConfirm = null;
    VdbWizardIdPanel pnlID = null;
    VdbWizardEntitlementsPanel pnlEntitlements = null;
    VdbWizardEntitlementsSelectPanel pnlEntitlementsSelect = null;
    VDBWizardModelVisibilityPanel visPanel = null;
    private VdbWizardUserAndPasswordPanel upPanel = null;
    private VdbWizardSaveMaterializationFilesPanel savePanel = null;
    private VdbWizardWrittenMaterializationFilesPanel matFilesPanel = null;
    private boolean importing = true;
    private boolean creatingNewVDBVersion = false; // else creating new VDB
    private VirtualDatabase sourceVirtualDatabase;
    private MaterializationLoadScripts scripts = null;

    /**
     * When importing from a file, which extra pages have been added
     */
    private int importInitializedType = IMPORT_INITIALIZED_NONE;

    // ID vars
    String sVdbName = ""; //$NON-NLS-1$
    String sDescription = ""; //$NON-NLS-1$

    ModelVisibilityInfo[] visInfo = null;

    BasicVDBDefn vdbDefn = null;

    // Connector Binding vars
    Map /* <String model name to List of String connector binding UUIDs> */mapConnBind = null;
    Map /* <String binding UUID on import to BindingNameAndExistingUUID> */
    existingBindingsOnImport = null;
    CreateVDBPanelParent dlgParent = null;

    // result
    VirtualDatabase newVirtualDatabase = null;
    VDBDefn newVDBDefn = null;
    boolean nextFlag = true;

    // calculated
    short siStatus = 0;
    private EntitlementMigrationReport emrEntitlementReport = null;

    private ModelInfo materializationTableModel;
    private ConnectorBinding materializationConnectorBinding;
    private boolean attemptedCreatingVDB = false;
    private ConnectionInfo connection = null;
    
    /**Whether the vdb selection has changed on the first panel of the wizard*/
    private boolean vdbSelectionChanged = true;
    /**String version of the selected VDB - either the file path or the path in the repository.*/
    private String oldVDBPath = ""; //$NON-NLS-1$


    // If dtcTreeView is null
    // This is an import
    // Else
    // This is a new creation
    // If vdbSourceVdb is null
    // This is a new VDB
    // Else
    // This is a new VDB version
    // Endif
    // Endif

    public CreateVDBPanel(CreateVDBPanelParent dlgParent,
                          VirtualDatabase vdbSourceVdb,
                          ConnectionInfo connection) {
        super();
        this.dlgParent = dlgParent;
        this.sourceVirtualDatabase = vdbSourceVdb;
        this.connection = connection;
        init();
    }

    private RuntimeMetadataAdminAPI getRuntimeMetadataAdminAPI() {
        return ModelManager.getRuntimeMetadataAPI(connection);
    }

    private VdbManager getVdbManager() {
        return ModelManager.getVdbManager(connection);
    }

    private ConnectorManager getConnectorManager() {
        return ModelManager.getConnectorManager(connection);
    }

    private ConfigurationManager getConfigurationManager() {
        return ModelManager.getConfigurationManager(connection);
    }

    private RuntimeMgmtManager getRuntimeMgmtManager() {
        return ModelManager.getRuntimeMgmtManager(connection);
    }

    private void init() {
        // clear out the wizard panel
        int iPages = getPageCount();
        for (int x = 0; x < iPages; x++) {
            removePage(x);
        }
        String initialDirectory = (String)UserPreferences.getInstance()
                                                         .getValue(ConsoleConstants.CONSOLE_DIRECTORY_LOCATION_KEY);

        selectorPanel = new ImportFileSelectionPanel(this, initialDirectory, 1);
        addPage(selectorPanel);

        pnlID = new VdbWizardIdPanel(1, this);
        addPage(pnlID);

        // this is added so that when the wizard initially comes up,
        // the second panel will have a next button, not a
        // finish button. This panel will be removed
        // when the other panels are init'd

        VdbWizardIdPanel extra = new VdbWizardIdPanel(1, this);
        addPage(extra);
        renumberPages();
        setListeners();
    }

    /**
     * Initialize remaining pages for new VDB version, after user has specified a file and VDB name. Only called when importing a
     * VDB from a file (importing=true).
     * 
     * @since 4.2
     */
    private void initImportNewVersion() {

        if (importInitializedType != IMPORT_INITIALIZED_NEW_VERSION) {
            // clear out remaining pages if they're the wrong type
            int iPages = getPageCount();
            for (int x = iPages; x > ID_STEP_NUM; x--) {
                removePage(x - 1);
            }

            // add remaining pages if they don't exist
            visPanel = new VDBWizardModelVisibilityPanel(1, this);
            pnlEditConnBind = new VdbWizardEditConnBindPanel(1, sourceVirtualDatabase, this, connection);
            pnlEntitlementsSelect = new VdbWizardEntitlementsSelectPanel(sourceVirtualDatabase, connection, this, 1);
            pnlConfirm = new VdbWizardConfirmPanel(sourceVirtualDatabase, this, 1);

            pnlEntitlements = new VdbWizardEntitlementsPanel(sourceVirtualDatabase, this, connection, 1);

            addPage(visPanel);
            addPage(pnlEditConnBind);

            addPage(pnlEntitlementsSelect);

            addPage(pnlConfirm);

            pnlEntitlementsSelect.getViewEntitlementsReportCbx().addActionListener(new ActionListener() {

                public void actionPerformed(ActionEvent event) {
                    processViewEntitlementControl();
                }
            });
            renumberPages();
            this.getNextButton().setEnabled(false);
        }
        
        initImportCommon();

        importInitializedType = IMPORT_INITIALIZED_NEW_VERSION;
    }

    /**
     * Initialize remaining pages for new VDB, after user has specified a file and VDB name. Only called when importing a VDB from
     * a file (importing=true).
     * 
     * @since 4.2
     */
    private void initImportNewVDB() {

        if (importInitializedType != IMPORT_INITIALIZED_NEW_VDB) {
            // clear out remaining pages if they're the wrong type
            int iPages = getPageCount();
            for (int x = iPages; x > ID_STEP_NUM; x--) {
                removePage(x - 1);
            }

            // add remaining pages if they don't exist
            visPanel = new VDBWizardModelVisibilityPanel(1, this);
            addPage(visPanel);

            pnlEditConnBind = new VdbWizardEditConnBindPanel(1, this, connection);
            addPage(pnlEditConnBind);

        	boolean hasRoles = true;
        	if(this.vdbDefn!=null) {
        		hasRoles = this.vdbDefn.getDataRoles() != null;
        	}

        	pnlEntitlementsSelect = new VdbWizardEntitlementsSelectPanel(this, 1);
	        if(hasRoles) {
	        	addPage(pnlEntitlementsSelect);
        	}

            pnlConfirm = new VdbWizardConfirmPanel(this, 1);

            addPage(pnlConfirm);
            
            pnlEntitlements = new VdbWizardEntitlementsPanel(null, this, connection, 1);
            
            pnlEntitlementsSelect.getViewEntitlementsReportCbx().addActionListener(new ActionListener() {

                public void actionPerformed(ActionEvent event) {
                    processViewEntitlementControl();
                }
            });

            renumberPages();
            // First panel (pnlID) starts out with "next" button disabled. Must
            // enter non-blank in VDB field to enable.
            this.getNextButton().setEnabled(false);

        }
        
        initImportCommon();


        importInitializedType = IMPORT_INITIALIZED_NEW_VDB;
    }

    /**
     * Common code for initializing remaining pages after user has specified a file and VDB name. Called within specific
     * initialization code for new VDB or new version.
     * 
     * @since 4.2
     */
    private void initImportCommon() {
        ModelVisibilityInfo[] vInfo = convertModelTableRowsToModelVisibilityInfo(vdbDefn);
        
        if (vdbSelectionChanged) {
            visPanel.populateTable(vInfo);

            Collection mdls = vdbDefn.getModels();
            // put those models into pnlEditConnBind
            pnlEditConnBind.setVDB(vdbDefn);
            pnlEditConnBind.setModels(mdls);
            pnlEditConnBind.loadAdditionalBindings();
            
            mapConnBind = null;
        }
        
        // in the case that we are deploying a new vdb version
        // then use the original vdbs bindings.
        if (sourceVirtualDatabase != null) {
            pnlEditConnBind.useBindingsFromPreviousVDB();    
        }
        
        if(vdbDefn!=null && pnlEntitlementsSelect!=null) {
            pnlEntitlementsSelect.setShowVdbRolesImportOptionEnabled(vdbDefn.getDataRoles()!=null);
        }
    }

    private void setListeners() {
        getCancelButton().addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent event) {
                processCancelButton();
            }
        });

        getFinishButton().addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent event) {
                processFinishButton();
            }
        });
        if (pnlEntitlementsSelect != null) {
            pnlEntitlementsSelect.getViewEntitlementsReportCbx().addActionListener(new ActionListener() {

                public void actionPerformed(ActionEvent event) {
                    processViewEntitlementControl();
                }
            });
        }
    }

    private void renumberPages() {
        Component[] thePages = getPages();
        for (int i = 0; i < thePages.length; i++) {
            if (thePages[i] instanceof BasicWizardSubpanelContainer) {
                BasicWizardSubpanelContainer bwsc = (BasicWizardSubpanelContainer)thePages[i];
                bwsc.replaceStepNum(i + 1);
            }
        }
    }

    private void processViewEntitlementControl() {
        if (pnlEntitlementsSelect.isSelectedViewEntitlementsReport()) {
            // add the optional page to this WizardPanel
            // add it to the end:
            addPage(pnlEntitlements);
        } else {
            // remove the optional page from this WizardPanel
            removePage(pnlEntitlements);
        }
        renumberPages();
    }

    private void processCancelButton() {
        // do our local stuff

        // let dialog do its thing
        dlgParent.processCancelButton();
    }

    private void processFinishButton() {
        if (!attemptedCreatingVDB) {
            try {
                createVDB();
            } catch (Exception e) {
                LogManager.logError(LogContexts.VIRTUAL_DATABASE, e, "VDB creation failed"); //$NON-NLS-1$
                ExceptionUtility.showMessage("ERROR!  Failed while creating a VDB", e); //$NON-NLS-1$
            }
        }
        // let dialog do its thing
        dlgParent.processFinishButton();
    }

    public AbstractButton getWizardNextButton() {
        return getNextButton();
    }

    public void enableNextButton(boolean b) {
        ButtonWidget btnNext = null;

        btnNext = getNextButton();

        if (btnNext != null) {
            btnNext.setEnabled(b);
        } else {
            btnNext = getFinishButton();
            if (btnNext != null) {
                btnNext.setEnabled(b);
            }
        }
    }

    public VirtualDatabase getNewVdb() {
        return newVirtualDatabase;
    }


    private EntitlementMigrationReport getMigrationReport() {
        return emrEntitlementReport;
    }

       
   
    /**
     * Get list of new connector bindings.
     * @return List<ConnectorBinding>
     * @since 4.3
     */
    private List getNewConnectorBindings() {
        //create a copy of the map of all bindings
        Map bindings = new HashMap(newVDBDefn.getConnectorBindings());
                
        //remove existing bindings
        for (Iterator iter = existingBindingsOnImport.values().iterator(); iter.hasNext();) {
             BindingNameAndExistingUUID bnaeu = (BindingNameAndExistingUUID) iter.next();
             String name = bnaeu.getBindingName();
             bindings.remove(name);      
        }
        
        return new ArrayList(bindings.values());
        
    }
    
    
    
    private void createMaterializationFiles() {
        
        // Re-call this method. The returned connector binding may be null until the VDB
        // is actually created.
        try {
            validateConnBindForMaterialization();
        } catch (Exception ex) {
            // Ignore
        }
        // Create MaterializationLoadScripts object
        boolean proceeding = true;
        Throwable error = null;
        String connectionPropsFileName = null;
        String createScriptFileName = null;
        String loadScriptFileName = null;
        String swapScriptFileName = null;
        String truncateScriptFileName = null;
        String directory = null;
        try {
            String host = getVdbManager().getConnection().getHost();
            String port = getVdbManager().getConnection().getPort();
            String loginUserName = upPanel.getLoginUserName();
            String loginPassword = upPanel.getLoginPassword();
            String dataBaseUserName = upPanel.getDataBaseUserName();
            String dataBasePassword = upPanel.getDataBasePassword();
            if (newVirtualDatabase != null) {
                VirtualDatabaseID id = (VirtualDatabaseID)newVirtualDatabase.getID();
                String versStr = id.getVersion();
                vdbDefn.setVersion(versStr);
            }
            scripts = getVdbManager().getMaterializationScripts(materializationConnectorBinding,
                                                                vdbDefn,
                                                                host,
                                                                port,
                                                                dataBaseUserName,
                                                                dataBasePassword,
                                                                loginUserName,
                                                                loginPassword);
        } catch (Exception ex) {
            String msg = "Error obtaining materialization information"; //$NON-NLS-1$
            LogManager.logError(LogContexts.VIRTUAL_DATABASE, ex, msg);
            error = ex;
            proceeding = false;
            
        }
        if (proceeding) {
            // Create the files
            // Create the output files
            directory = savePanel.getDirectoryName();
            if (!directory.endsWith(File.separator)) {
                directory = directory + File.separator;
            }
            connectionPropsFileName = directory + scripts.getConnectionPropsFileName();
            InputStream connectionPropsInputStream = scripts.getConnectionPropsFileContents();
            File target = new File(connectionPropsFileName);
            proceeding = true;
            if (target.exists()) {
                proceeding = true;
            } else {
                try {
                    target.createNewFile();
                } catch (IOException ex) {
                    error = ex;
                    proceeding = false;
                }
            }
            if (proceeding) {
                boolean exported = false;
                if (!target.canWrite()) {
                    StaticUtilities.displayModalDialogWithOK("Cannot write to target file", //$NON-NLS-1$
                                                             "Unable to write to target file " + connectionPropsFileName + //$NON-NLS-1$
                                                                             "."); //$NON-NLS-1$
                    proceeding = false;
                }
                if (proceeding) {
                    try {
                        FileUtils.write(connectionPropsInputStream, target);
                        exported = true;
                    } catch (Exception ex) {
                        LogManager.logError(LogContexts.VIRTUAL_DATABASE, ex, "Error writing connection properties file."); //$NON-NLS-1$
                        error = ex;
                        proceeding = false;
                    }
                }
                if (exported) {
                    String directoryForModule = savePanel.getDirectoryName();
                    UserPreferences.getInstance().setValue(ConsoleConstants.SAVE_MATERIALIZATION_DIRECTORY_LOCATION_KEY,
                                                           directoryForModule);
                    UserPreferences.getInstance().saveChanges();
                }
            }
            if (proceeding) {
                createScriptFileName = directory + scripts.getCreateScriptFileName();
                InputStream createScriptInputStream = scripts.getCreateScriptFile();
                target = new File(createScriptFileName);
                if (target.exists()) {
                    proceeding = true;
                } else {
                    try {
                        target.createNewFile();
                    } catch (IOException ex) {
                        error = ex;
                        proceeding = false;
                    }
                }
                if (proceeding) {
                    if (!target.canWrite()) {
                        StaticUtilities.displayModalDialogWithOK("Cannot write to target file", //$NON-NLS-1$
                                                                 "Unable to write to target file " + createScriptFileName + //$NON-NLS-1$
                                                                                 "."); //$NON-NLS-1$
                        proceeding = false;
                    }
                    if (proceeding) {
                        try {
                            FileUtils.write(createScriptInputStream, target);
                        } catch (Exception ex) {
                            LogManager.logError(LogContexts.VIRTUAL_DATABASE, ex, "Error writing DDL Script file."); //$NON-NLS-1$
                            error = ex;
                            proceeding = false;
                        }
                    }
                }
                if (proceeding) {
                    loadScriptFileName = directory + scripts.getLoadScriptFileName();
                    InputStream loadScriptInputStream = scripts.getLoadScriptFile();
                    target = new File(loadScriptFileName);
                    if (target.exists()) {
                        } else {
                        try {
                            target.createNewFile();
                        } catch (IOException ex) {
                            error = ex;
                            proceeding = false;
                        }
                    }
                    if (proceeding) {
                        if (!target.canWrite()) {
                            StaticUtilities.displayModalDialogWithOK("Cannot write to target file", //$NON-NLS-1$
                                                                     "Unable to write to target file " + loadScriptFileName + //$NON-NLS-1$
                                                                                     "."); //$NON-NLS-1$
                            proceeding = false;
                        }
                        if (proceeding) {
                            try {
                                FileUtils.write(loadScriptInputStream, target);                                
                            } catch (Exception ex) {
                                LogManager.logError(LogContexts.VIRTUAL_DATABASE, ex, "Error writing Load Script file."); //$NON-NLS-1$
                                error = ex;
                                proceeding = false;
                            }
                        }
                    }
                    if (proceeding) {
                        swapScriptFileName = directory + scripts.getSwapScriptFileName();
                        InputStream swapScriptInputStream = scripts.getSwapScriptFile();
                        target = new File(swapScriptFileName);
                        if (target.exists()) {
                            proceeding = true;
                        } else {
                            try {
                                target.createNewFile();
                            } catch (IOException ex) {
                                StaticUtilities
                                               .displayModalDialogWithOK("Cannot create target file", //$NON-NLS-1$
                                                                         "Unable to create target file " + swapScriptFileName + //$NON-NLS-1$
                                                                                         ".  Must select another file name or cancel."); //$NON-NLS-1$
                                proceeding = false;
                            }
                        }
                        if (proceeding) {
                            if (!target.canWrite()) {
                                StaticUtilities
                                               .displayModalDialogWithOK("Cannot write to target file", //$NON-NLS-1$
                                                                         "Unable to write to target file " + swapScriptFileName + //$NON-NLS-1$
                                                                                         "."); //$NON-NLS-1$
                                proceeding = false;
                            }
                            if (proceeding) {
                                try {
                                    FileUtils.write(swapScriptInputStream, target);                                    
                                } catch (Exception ex) {
                                    LogManager.logError(LogContexts.VIRTUAL_DATABASE, ex, "Error writing Load Script file."); //$NON-NLS-1$
                                    error = ex;
                                    proceeding = false;
                                }
                            }
                        }
                        if (proceeding) {
                            truncateScriptFileName = directory + scripts.getTruncateScriptFileName();
                            InputStream truncateScriptInputStream = scripts.getTruncateScriptFile();
                            target = new File(truncateScriptFileName);
                            if (target.exists()) {
                                proceeding = true;
                            } else {
                                try {
                                    target.createNewFile();
                                } catch (IOException ex) {
                                    StaticUtilities
                                                   .displayModalDialogWithOK("Cannot create target file", //$NON-NLS-1$
                                                                             "Unable to create target file " + truncateScriptFileName + //$NON-NLS-1$
                                                                                             ".  Must select another file name or cancel."); //$NON-NLS-1$
                                    proceeding = false;
                                }
                            }
                            if (proceeding) {
                                if (!target.canWrite()) {
                                    StaticUtilities
                                                   .displayModalDialogWithOK("Cannot write to target file", //$NON-NLS-1$
                                                                             "Unable to write to target file " + truncateScriptFileName + //$NON-NLS-1$
                                                                                             "."); //$NON-NLS-1$
                                    proceeding = false;
                                }
                                if (proceeding) {
                                    try {
                                        FileUtils.write(truncateScriptInputStream, target);                                        
                                    } catch (Exception ex) {
                                        LogManager.logError(LogContexts.VIRTUAL_DATABASE,
                                                            ex,
                                                            "Error writing Truncate Script file."); //$NON-NLS-1$
                                        error = ex;
                                        proceeding = false;
                                    }
                                }
                            }
                        }
                    }
                }
            }

        }
        Object materializationFileCreationResult = null;
        if (error != null) {
            materializationFileCreationResult = error;
        } else {
            SingleMaterializationFileDisplayInfo[] files = new SingleMaterializationFileDisplayInfo[NUM_MATERIALIZATION_FILES];
            files[0] = new SingleMaterializationFileDisplayInfo(CONNECTION_PROPERTIES_FILE_DESC,
                                                                scripts.getConnectionPropsFileName());
            files[1] = new SingleMaterializationFileDisplayInfo(CREATE_SCRIPT_FILE_DESC,
                                                                scripts.getCreateScriptFileName());
            files[2] = new SingleMaterializationFileDisplayInfo(LOAD_SCRIPT_FILE_DESC, scripts.getLoadScriptFileName());
            files[3] = new SingleMaterializationFileDisplayInfo(SWAP_SCRIPT_FILE_DESC, scripts.getSwapScriptFileName());
            files[4] = new SingleMaterializationFileDisplayInfo(TRUNCATE_SCRIPT_FILE_DESC,
                                                                scripts.getTruncateScriptFileName());
            AllMaterializationFilesDisplayInfo allFiles = new AllMaterializationFilesDisplayInfo(directory, files);
            materializationFileCreationResult = allFiles;
        }
        matFilesPanel.setResults(materializationFileCreationResult);        
    }
    
   
    
    

    /**
     * @throws Exception
     * @since 4.3
     */
    private void createVDB() throws Exception {
        attemptedCreatingVDB = true;

        
        if (sVdbName == null || sVdbName.equals("")) { //$NON-NLS-1$
            ExceptionUtility.showMessage("ERROR!  could not create vdb because name is missing", //$NON-NLS-1$
                                         new Exception("VDB Name Must Be Specified")); //$NON-NLS-1$
        } else {


            Map visMap = CreateVDBPanel.modelVisibilityMap(visInfo);
            Map multiSourceMap = CreateVDBPanel.modelMultiSourceMap(visInfo);
            try {

                newVDBDefn = createDEF(vdbDefn,
                                         sVdbName,
                                         visMap,
                                         reviseMapForExistingUUIDSubstitution(mapConnBind, existingBindingsOnImport),
                                         multiSourceMap,
                                         pnlConfirm.getStatus(),
                                         this.getConnectorManager().getAllConnectorBindings());

                
                boolean importRoles = true;
                if(creatingNewVDBVersion) {
                    importRoles = pnlEntitlementsSelect.isSelectedImportEntitlementsFromVdb();
                }
                
                if(importing) {
                	importRoles = pnlEntitlementsSelect.isSelectedImportEntitlementsFromVdb();
                }

                VDBArchive vdbArchive = null;
                Object[] results = null;
                try {
                	vdbArchive = new VDBArchive(newVDBDefn.getVDBStream().getInputStream());
                	vdbArchive.updateConfigurationDef((BasicVDBDefn)newVDBDefn);
                	vdbArchive.setStatus(pnlConfirm.getStatus());
                	results = getVdbManager().importVDB(vdbArchive,importRoles);	
                } finally {
                	if (vdbArchive != null) {
                		vdbArchive.close();
                	}
                }
                
                newVirtualDatabase = (VirtualDatabase)results[0];
                emrEntitlementReport = (EntitlementMigrationReport)results[1];
                
                //if importing from a file, check that passwords are decryptable
                boolean decryptable = true;
                List bindings = getNewConnectorBindings();
                decryptable = getConfigurationManager().checkDecryptable(bindings);
                
                //synchronize
                try {
                    if (decryptable && pnlConfirm.isSyncActive()) {
                        getRuntimeMgmtManager().synchronizeServer();
                    }
                } catch (Exception e) {
                    LogManager.logError(LogContexts.VIRTUAL_DATABASE, e, "Synchronize failed"); //$NON-NLS-1$
                    ExceptionUtility.showMessage("ERROR!  Synchronize Failed", e); //$NON-NLS-1$
                }            
                
                
                if (creatingNewVDBVersion) {
                    try {
                        if (pnlEntitlementsSelect.isSelectedMigrateEntitlements()) {
                            emrEntitlementReport = getVdbManager().migrateEntitlements(sourceVirtualDatabase, newVirtualDatabase);
                            WorkspaceController.getInstance().handleUpdateNotification(getVdbManager().getConnection(),
                                                                                       new DataEntitlementChangeNotification());
                        }
                    } catch (Exception e) {
                        LogManager.logError(LogContexts.VIRTUAL_DATABASE, e, "Migrating roles failed"); //$NON-NLS-1$
                        ExceptionUtility.showMessage("ERROR!  Migrate Roles Failed", e); //$NON-NLS-1$
                    }
                    
                    newVirtualDatabase = getVdbManager().getVirtualDatabase((VirtualDatabaseID) newVirtualDatabase.getID());
                }

                // if we selected to import the roles updates the panel content.
                if (importRoles) {
                    WorkspaceController.getInstance().handleUpdateNotification(getVdbManager().getConnection(), new DataEntitlementChangeNotification());
                }

                getConfigurationManager().refresh();
            } catch (Exception e) {
                ExceptionUtility.showMessage("ERROR!  VDB Not created", e.getMessage(), e); //$NON-NLS-1$
            }
            
            
            if (materializationTableModel != null) {
                createMaterializationFiles();
            }
        }
    }
    
    private VDBDefn createDEF(BasicVDBDefn entry, String newName, Map visibilities, Map modelsToBindings, Map multiSourceEnabled, short status, Collection configbindings ) throws Exception {
        
        
        Assertion.isNotNull(entry, "RMCVersionEntry must not be null"); //$NON-NLS-1$
        Assertion.isNotNull(newName, "Name for the new VDB must not be null"); //$NON-NLS-1$
        Assertion.isNotNull(visibilities, "Visibilities for models in new VDB must not be null"); //$NON-NLS-1$
        
        //Hash the configuration's known connector bindings by UUID for use later
        Map uuidsToConfigBindings = new HashMap(configbindings.size());
        for (Iterator bit = configbindings.iterator(); bit.hasNext();) {
            ConnectorBinding cb = (ConnectorBinding) bit.next();
            uuidsToConfigBindings.put(cb.getRoutingUUID(), cb);
        }
        // set the visilibities for each model        
        Collection models = entry.getModels();
        for (Iterator it=models.iterator(); it.hasNext(); ) {
            BasicModelInfo mdefn = (BasicModelInfo) it.next();
            Object o = visibilities.get(mdefn.getName());
            if (o instanceof Short) {
                Short s = (Short) o;
                mdefn.setVisibility(s.shortValue());
            } else  {
                String s = (String) o;
                
                if (s.equalsIgnoreCase(ModelInfo.PUBLIC_VISIBILITY)) {
                     mdefn.setIsVisible(true);
                 } else {
                     mdefn.setIsVisible(false);
                 }
                
            }
            
            
            if (modelsToBindings != null && modelsToBindings.containsKey(mdefn.getName())) {
                List bindings = (List) modelsToBindings.get(mdefn.getName());
                List names = new ArrayList(bindings.size());
                for (Iterator iter=bindings.iterator(); iter.hasNext();) {
                    String uuid = (String) iter.next();
                    ConnectorBinding cb = (ConnectorBinding) uuidsToConfigBindings.get(uuid); 
                    if (cb == null) {
                        cb = entry.getConnectorBindingByRouting(uuid);
                    } 

                    if (cb != null) {
                        names.add(cb.getFullName());
                    }
                    
                }
                
                mdefn.setConnectorBindingNames(names);
           }     
            
           if (multiSourceEnabled != null && multiSourceEnabled.containsKey(mdefn.getName())) {
               Boolean enabled = (Boolean) multiSourceEnabled.get(mdefn.getName());
               mdefn.enableMutliSourceBindings(enabled.booleanValue());
           }
            
        }
        
        //remove any bindings that are not mapped to models
        entry.removeUnmappedBindings();
        
                
        entry.setModelInfos(models);   
        
        entry.setStatus(status);
        entry.setName(newName);

        return entry;
    }

    
    
    /** 
     * @see com.metamatrix.toolbox.ui.widget.WizardPanel#showNextPage()
     * @since 4.3
     */
    public void showNextPage() {
        try {
            StaticUtilities.startWait(dlgParent.getContentPane());
            JPanel pnlCurrPage = (JPanel)getCurrentPage();
            boolean proceeding = true;
            
            
            if (pnlCurrPage.equals(selectorPanel)) {
                proceeding = navigateFromSelectorPanel();
                
            } else if (pnlCurrPage.equals(pnlID)) {
                proceeding = navigateFromIDPanel();
                
            } else if (pnlCurrPage.equals(visPanel)) {
                proceeding = navigateFromVisibilityPanel();
                
            } else if (pnlCurrPage.equals(pnlEditConnBind)) {
                proceeding = navigateFromEditConnectorBindingsPanel();
                
            } else if (pnlCurrPage.equals(upPanel)) {      
                proceeding = navigateFromUserPasswordPanel();
                
            } else if (pnlCurrPage.equals(savePanel)) {
                proceeding = navigateFromSavePanel();
                
            } else if (pnlCurrPage.equals(pnlEntitlementsSelect)) {
                proceeding = navigateFromEntitlementsSelectPanel();
                
            } else if (pnlCurrPage.equals(pnlConfirm)) {
                proceeding = navigateFromConfirmPanel();
                
            }
            
            if (! proceeding) {
                return;
            }
            
        } catch (Exception e) {
            ExceptionUtility.showMessage("ERROR!  failed navigating the wizard", e); //$NON-NLS-1$
        } finally {
            StaticUtilities.endWait(dlgParent.getContentPane());
        }
        super.showNextPage();
    }

    
    
    /**
     * Called when navigating from selectorPanel to the next panel 
     * @since 4.3
     */
    private boolean navigateFromSelectorPanel() throws Exception {
        boolean result = checkSelectorPanelResults();
        if (result) {
            pnlID.setVdbName(vdbDefn.getName());
            pnlID.setDescription(vdbDefn.getDescription());
            pnlID.putDataIntoPanel();

        } else {
            return false;
        }
        boolean proceeding = true;
        
        proceeding = findExistingBindings(true);

        if (!proceeding) {
            processCancelButton();
        }
        return true;
    }
    
    private boolean findExistingBindings(boolean promptWhenDuplicates) {
        existingBindingsOnImport = null;
        try {
            existingBindingsOnImport = getExistingBindingUUIDsForVDB(vdbDefn);
        } catch (Exception ex) {
            String msg = "Error retrieving existing connector bindings."; //$NON-NLS-1$
            LogManager.logError(LogContexts.VIRTUAL_DATABASE, ex, msg);
            ExceptionUtility.showMessage(msg, ex);
        }
        if ((existingBindingsOnImport != null) && (existingBindingsOnImport.size() > 0)) {
            String[] bindingNames = new String[existingBindingsOnImport.size()];
            Iterator ib = existingBindingsOnImport.entrySet().iterator();
            for (int i = 0; ib.hasNext(); i++) {
                Map.Entry me = (Map.Entry)ib.next();
                BindingNameAndExistingUUID item = (BindingNameAndExistingUUID)me.getValue();
                bindingNames[i] = item.getBindingName();
            }
            
            if (promptWhenDuplicates) {
                BindingsAlreadyExistDlg dlg = new BindingsAlreadyExistDlg(ConsoleMainFrame.getInstance(), bindingNames);
                dlg.show();
                return dlg.proceeding();
            }
        }
        
        return true;
    }
    
    
    
    /**
     * Called when navigating from pnlID to the next panel 
     * @since 4.3
     */
    private boolean navigateFromIDPanel() {
        pnlID.getDataFromPanel();
        sVdbName = pnlID.getVdbName();
        sDescription = pnlID.getDescription();
        vdbDefn.setDescription(sDescription);
        if (importing) {
            sourceVirtualDatabase = getLatestVDB(sVdbName);
            creatingNewVDBVersion = (sourceVirtualDatabase != null);
            if (creatingNewVDBVersion) {
                initImportNewVersion();
            } else {
                initImportNewVDB();
            }
        } else {
            initImportCommon();
        }
        return true;
    }
    
    
    /**
     * Called when navigating from visPanel to the next panel 
     * @since 4.3
     */
    private boolean navigateFromVisibilityPanel() {
        visInfo = visPanel.getUpdatedVisibilityInfo();
        pnlEditConnBind.updateMultiSource(CreateVDBPanel.modelMultiSourceMap(visInfo));
        pnlEditConnBind.updateSelectionForModelsTable();
        return true;
    }
    
    /**
     * Called when navigating from editConnBindPanel to the next panel 
     * @since 4.3
     */
    private boolean navigateFromEditConnectorBindingsPanel() {
        boolean proceeding = true;
        materializationTableModel = getVdbManager().getMaterializationTableModel(vdbDefn);
        if (materializationTableModel != null) {
            String modelName = materializationTableModel.getName();
            if (!pnlEditConnBind.hasBindingAssigned(modelName)) {
                proceeding = false;
                String hdr = "Materialization model connector binding needed"; //$NON-NLS-1$
                String msg = "Must assign connector binding to " + //$NON-NLS-1$
                "materialization model " + modelName + //$NON-NLS-1$
                " before proceeding."; //$NON-NLS-1$
                StaticUtilities.displayModalDialogWithOK(hdr, msg, JOptionPane.WARNING_MESSAGE);
            }
        }
        if (proceeding) {
            // pull the conn bind stuff into a model/connbind map
            mapConnBind = pnlEditConnBind.getModelsToConnBindsMap();
            siStatus = determinePotentialVdbsStatus(mapConnBind);
            
            // put the name and status in the entitlements and confirm panels before
            // we show them:
            pnlConfirm.setVdbName(sVdbName);
            pnlConfirm.setStatus(siStatus);
            if (pnlEntitlementsSelect != null) {
                pnlEntitlementsSelect.setVdbName(sVdbName);
                pnlEntitlementsSelect.setStatus(siStatus);
                pnlEntitlementsSelect.putDataIntoPanel();
                
                pnlConfirm.setMigrateEntitlementsRequested(pnlEntitlementsSelect.isSelectedMigrateEntitlements());
            } else {
                pnlConfirm.setMigrateEntitlementsRequested(false);
            }
            pnlConfirm.putDataIntoPanel();
            if (materializationTableModel != null) {
                // 1) Validate connector binding capabilities for materialization
                try {
                    validateConnBindForMaterialization();
                } catch (Exception ex) {
                    LogManager.logError(LogContexts.VIRTUAL_DATABASE,
                                        ex,
                    "Error validating materialization connector binding."); //$NON-NLS-1$
                    ExceptionUtility.showMessage("Error validating materialization connector binding", ex); //$NON-NLS-1$
                    return false;
                }
                // 2) omitted
                // 3) omitted
                // 4) If not already done, create the new panels. Just put in ones
                // for step num, will replace below.
                if (upPanel == null) {
                    upPanel = new VdbWizardUserAndPasswordPanel(this, 1);
                }
                if (savePanel == null) {
                    String initialDirectory = (String)UserPreferences
                    .getInstance()
                    .getValue(ConsoleConstants.SAVE_MATERIALIZATION_DIRECTORY_LOCATION_KEY);
                    savePanel = new VdbWizardSaveMaterializationFilesPanel(this, 1, initialDirectory);
                }
                if (matFilesPanel == null) {
                    matFilesPanel = new VdbWizardWrittenMaterializationFilesPanel(this, 1, true);
                }
                // 5) Remove then reinsert the affected panels. Insert correct step
                // numbers.
                removePage(upPanel);
                removePage(savePanel);
                if (pnlEntitlementsSelect != null) {
                    removePage(pnlEntitlementsSelect);
                }
                removePage(pnlConfirm);
                removePage(matFilesPanel);
                addPage(upPanel);
                addPage(savePanel);
                if (pnlEntitlementsSelect != null) {
                    addPage(pnlEntitlementsSelect);
                }
                addPage(pnlConfirm);
                addPage(matFilesPanel);
                renumberPages();
                
                getForwardButton().setEnabled(false);
            } else {
                // Remove any unneeded pages that may have been inserted due to a different prior VDB
                // selection.
                if (upPanel != null) {
                    removePage(upPanel);
                }
                if (savePanel != null) {
                    removePage(savePanel);
                }
                if (matFilesPanel != null) {
                    removePage(matFilesPanel);
                }
                renumberPages();
            }
        }
        
        return proceeding;        
    }
        

    /**
     * Called when navigating from upPanel to the next panel 
     * @since 4.3
     */
    private boolean navigateFromUserPasswordPanel() {
        // TODO-- set forward button enabling- how?
        return true;
    }

    
    /**
     * Called when navigating from savePanel to the next panel 
     * @since 4.3
     */
    private boolean navigateFromSavePanel() {
        getForwardButton().setEnabled(false);
        return true;
    }
    
    /**
     * Called when navigating from entitlementsSelectPanel to the next panel 
     * @since 4.3
     */
    private boolean navigateFromEntitlementsSelectPanel() {
        pnlConfirm.setVdbName(sVdbName);
        pnlConfirm.setStatus(siStatus);
        pnlConfirm.setMigrateEntitlementsRequested(pnlEntitlementsSelect.isSelectedMigrateEntitlements());
        pnlConfirm.putDataIntoPanel();
        return true;
    }
    
    
    /**
     * Called when navigating from pnlConfirm to the next panel 
     * @since 4.3
     */
    private boolean navigateFromConfirmPanel() {         
        try {
            createVDB();
        } catch (Exception e) {
            LogManager.logError(LogContexts.VIRTUAL_DATABASE, e, "VDB creation failed"); //$NON-NLS-1$
            ExceptionUtility.showMessage("ERROR!  Failed while creating a VDB", e); //$NON-NLS-1$
        }
        
        if (pnlEntitlements != null) {
            pnlEntitlements.setNewVdb(getNewVdb());
            
            pnlEntitlements.setEntitlementMigrationReport(getMigrationReport());
        }
        getBackButton().setVisible(false);
        getCancelButton().setVisible(false);
        return true;
    }
    
    
    
    
    /** 
     * @see com.metamatrix.toolbox.ui.widget.WizardPanel#showPreviousPage()
     * @since 4.3
     */
    public void showPreviousPage() {
        super.showPreviousPage();
        enableNextButton(true);
    }

    private Map /* <String binding UUID on import to BindingNameAndExistingUUID> */
    getExistingBindingUUIDsForVDB(VDBDefn rve) throws Exception {
        Map existingBindingsInVDB = new HashMap();
        Iterator it = rve.getConnectorBindings().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry me = (Map.Entry)it.next();
            String bindingName = (String)me.getKey();
            ConnectorBinding cb = getConnectorManager().getConnectorBindingByName(bindingName);
            if (cb != null) {
                String uuid = cb.getRoutingUUID();
                ConnectorBinding cbFromImport = (ConnectorBinding)me.getValue();
                String uuidOnImport = cbFromImport.getRoutingUUID();
                BindingNameAndExistingUUID item = new BindingNameAndExistingUUID(bindingName, uuid);
                existingBindingsInVDB.put(uuidOnImport, item);
            }
        }
        return existingBindingsInVDB;
    }

    private Map /* <String model name to List of String UUIDs> */
    reviseMapForExistingUUIDSubstitution(Map /* <String binding name to List of String UUIDs on import> */fromImport,
                                         Map /* <String UUID on import to BindingNameAndExistingUUID> */existing) {
        Map outputMap;
        if ((existing == null) || (existing.size() == 0)) {
            outputMap = fromImport;
        } else {
            outputMap = new HashMap();
            Iterator it = fromImport.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry me = (Map.Entry)it.next();
                String modelName = (String)me.getKey();
                List /* <String> */bindingUUIDsOnImport = (List)me.getValue();
                List newList = new ArrayList(bindingUUIDsOnImport.size());
                Iterator it2 = bindingUUIDsOnImport.iterator();
                while (it2.hasNext()) {
                    String uuidFromImport = (String)it2.next();
                    String uuidToUse = null;
                    BindingNameAndExistingUUID replacement = (BindingNameAndExistingUUID)existing.get(uuidFromImport);
                    if (replacement == null) {
                        uuidToUse = uuidFromImport;
                    } else {
                        uuidToUse = replacement.getExistingUUID();
                    }
                    newList.add(uuidToUse);
                }
                outputMap.put(modelName, newList);
            }
        }
        return outputMap;
    }

    public boolean checkSelectorPanelResults() {
        boolean goingToNextPage = true;
        String fileName = selectorPanel.getSelectedFileFullName();
        
        vdbSelectionChanged = (!fileName.equals(oldVDBPath));
        oldVDBPath = fileName;
        
        
        File file = new File(fileName);

        if (!file.exists()) {
            StaticUtilities
                           .displayModalDialogWithOK("Unable to open file", //$NON-NLS-1$
                                                     "Unable to open file " + fileName + ".  Must select " + "a different file or cancel."); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            goingToNextPage = false;
        } else if (!file.canRead()) {
            StaticUtilities
                           .displayModalDialogWithOK("Unable to read file", //$NON-NLS-1$
                                                     "Unable to open file " + fileName + ".  Must select " + "a different file or cancel."); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            goingToNextPage = false;
        } else {
            try {

                String vdbDirectoryName = selectorPanel.getDirectoryName();
                String vdbFileName = selectorPanel.getSelectedFileName();

                UserPreferences.getInstance().setValue(ConsoleConstants.CONSOLE_DIRECTORY_LOCATION_KEY, vdbDirectoryName);
                UserPreferences.getInstance().saveChanges();

                String vdbF = vdbFileName.toLowerCase();

                if (vdbF.endsWith(VDB_IMPORT_EXTENSIONS[1])) {
                    goingToNextPage = importVDB(file, vdbFileName);
                } else if (vdbF.endsWith(VDB_IMPORT_EXTENSIONS[0])) {
                    // definition file
                    goingToNextPage = importDefinition(vdbFileName, vdbDirectoryName);
                } else {
                    StaticUtilities.displayModalDialogWithOK("Selection Error", "File selected must be of " + VDB_TYPE_FILE_DESC); //$NON-NLS-1$ //$NON-NLS-2$
                    goingToNextPage = false;
                }
            } catch (VirtualDatabaseException vex) {
                StaticUtilities.displayModalDialogWithOK("VDB Exceptions", //$NON-NLS-1$
                                                         vex.getMessage());

                String msg1 = "Unable to import VDB."; //$NON-NLS-1$
                String msg2 = "File " //$NON-NLS-1$
                              + fileName + " may not contain a VDB in the correct format, " //$NON-NLS-1$
                              + "or VDB definition file may be missing.  See details."; //$NON-NLS-1$
                ExceptionUtility.showMessage(msg1, msg2, vex);
                goingToNextPage = false;
            } catch (Exception ex) {
                String msg1 = "Unable to complete VDB Creation."; //$NON-NLS-1$
                String msg2 = "File " //$NON-NLS-1$
                              + fileName + " may not contain a VDB in the correct format, " //$NON-NLS-1$
                              + "or VDB definition file may be missing.  See details."; //$NON-NLS-1$
                ExceptionUtility.showMessage(msg1, msg2, ex);
                goingToNextPage = false;
            }
        }
        return goingToNextPage;

    }

    private boolean validateConnBindForMaterialization() throws Exception {
        String modelName = materializationTableModel.getName();
        Map map = pnlEditConnBind.getModelsToConnBindsMap();
        materializationConnectorBinding = null;
        List /* <String> */uuids = (List)map.get(modelName);
        if ((uuids != null) && (uuids.size() > 0)) {
            String uuid = (String)uuids.get(0);
            String connBindingName = (String)getConnectorManager().getUUIDConnectorBindingsMap(false).get(uuid);
            materializationConnectorBinding = getConnectorManager().getConnectorBindingByName(connBindingName);
        }
        return getRuntimeMetadataAdminAPI().validateConnectorBindingForMaterialization(materializationConnectorBinding);
    }

    private VirtualDatabase getLatestVDB(String vdbName) {

        try {
            // if a vdb is found then
            VirtualDatabase vdb = getVdbManager().getLatestVDBVersion(vdbName);
            return vdb;

        } catch (VirtualDatabaseDoesNotExistException dne) {
            return null;
        } catch (Exception ex) {
            String msg = "An error occurred retrieving VDB version information"; //$NON-NLS-1$
            LogManager.logError("Information Retrieval Error", ex, msg); //$NON-NLS-1$
            ExceptionUtility.showMessage(msg, ex);
            return null;
        }
    }

    protected boolean importVDB(File file, String newVDBName) throws Exception {
        
    	VDBArchive vdb = null;

    	try {
			vdb = new VDBArchive(file);
			vdb.setName(newVDBName);

			vdbDefn = vdb.getConfigurationDef();
			vdbDefn.setVDBStream(new VDBStreamImpl(file));
			if (vdb.getVDBValidityErrors() != null) {
			    StaticUtilities.displayModalDialogWithOK("VDB.DEF Processing Error", "VDB " + vdb.getName() + " is at a nondeployable severity state of " +  VDBStatus.VDB_STATUS_NAMES[vdb.getStatus()]); 
			    return false;
			}
			return true;
			
		} finally {
			if (vdb != null) {
				vdb.close();
			}
		}
    }

    protected boolean importDefinition(String fileName, String directory) throws Exception {
        if (fileName == null || directory == null) {
            StaticUtilities.displayModalDialogWithOK("VDB.DEF Processing Error", "VDB File name or the directory was not able to be obtained from selected file."); //$NON-NLS-1$
            return false;
        }
        vdbDefn = VDBArchive.readFromDef(new FileInputStream(new File(directory, fileName)));
        if (vdbDefn.doesVDBHaveValidityError()) {
            StaticUtilities.displayModalDialogWithOK("VDB.DEF Processing Error", //$NON-NLS-1$
                                      "VDB " + vdbDefn.getName() + " is at a nondeployable severity state of " +  //$NON-NLS-1$//$NON-NLS-2$
                                      VDBStatus.VDB_STATUS_NAMES[vdbDefn.getStatus()]);
            return false;
                                     
        }
        return true;
    }

    private ModelVisibilityInfo[] convertModelTableRowsToModelVisibilityInfo(VDBInfo info) {
        ModelVisibilityInfo[] visInfo = null;
        Collection mdls = info.getModels();
        int numRows = mdls.size();
        visInfo = new ModelVisibilityInfo[numRows];
        int i = 0;
        for (Iterator it = mdls.iterator(); it.hasNext();) {
            ModelInfo me = (ModelInfo)it.next();
            int numBindings = me.getConnectorBindingNames().size();
            boolean multipleSourceEditable = (me.supportsMultiSourceBindings() && (numBindings <= 1));
            visInfo[i] = new ModelVisibilityInfo(me.getName(),me.getModelTypeName(), me.isVisible(),
                                                 me.supportsMultiSourceBindings(), multipleSourceEditable,
                                                 me.isMultiSourceBindingEnabled());
            ++i;

        }

        return visInfo;
    }


    //
    private short determinePotentialVdbsStatus(Map mapConnBind) {
        // examine the map looking for any models that do
        // not have a connector binding specified.
        // if any missing, return:
        // MetadataConstants.VDB_STATUS.INCOMPLETE
        // if all are present, return
        // MetadataConstants.VDB_STATUS.INACTIVE
        // how about this... just look for values that are ""?
        short siStatus = 0;
        boolean bHasMissingValues = false;
        boolean bHasNoBindings = false;
        boolean requiresBindings = false;

        // verify that at least one model requires a binding
        Iterator mit = vdbDefn.getModels().iterator();
        while (mit.hasNext()) {
            ModelInfo mi = (ModelInfo)mit.next();
            if (mi.requiresConnectorBinding()) {
                requiresBindings = true;
                break;
            }
        }

        // if no binding is required, then the vdb can
        // be considered for active status
        if (!requiresBindings) {
            return VDBStatus.INACTIVE;
        }

        int connBind = 0;
        Iterator it = mapConnBind.values().iterator();

        // Count the number of bindings
        while (it.hasNext()) {
            List list = (List)it.next();
            if ((list == null) || (list.size() == 0)) {
                bHasMissingValues = true;
            } else {
                connBind += list.size();
            }
        }

        if (connBind == 0) {
            bHasNoBindings = true;
        }

        if (bHasNoBindings || bHasMissingValues) {
            siStatus = VDBStatus.INCOMPLETE;
        } else {
            siStatus = VDBStatus.INACTIVE;
        }
        return siStatus;
    }

    
}// end CreateVDBPanel

class ImportFileSelectionPanel extends BasicWizardSubpanelContainer implements
                                                                   MDCPOpenStateListener {

    private ModifiedDirectoryChooserPanel chooser;

    public ImportFileSelectionPanel(WizardInterface wizardInterface,
                                    String initialDirectory,
                                    int stepNum) {
        super(wizardInterface);

        FileSystemView view = new FileSystemView();
        if ((initialDirectory != null) && (initialDirectory.length() > 0)) {
            try {
                DirectoryEntry dirEntry = view.lookup(initialDirectory);
                view.setHome(dirEntry);
            } catch (Exception ex) {
                // Any exception that may occur on setting the initial view is
                // inconsequential. This is merely a convenience to the user.
            }
        }
        chooser = new ModifiedDirectoryChooserPanel(view, DirectoryChooserPanel.TYPE_OPEN, getFileFilters(view), this);
        chooser.setShowAcceptButton(false);
        chooser.setShowCancelButton(false);
        chooser.setShowDetailsButton(false);
        chooser.setShowFilterComboBox(false);
        chooser.setShowNewFolderButton(false);
        chooser.setShowPassThruFilter(false);
        super.setStepText(stepNum, "Select VDB Archive (.VDB) or Definition (.DEF) file to be imported"); //$NON-NLS-1$
        super.setMainContent(chooser);
    }

    public void fileSelectionIsValid(boolean flag) {
        AbstractButton forwardButton = getWizardInterface().getForwardButton();
        forwardButton.setEnabled(flag);
    }

    public String getSelectedFileFullName() {
        String name = null;
        FileSystemEntry fse = (FileSystemEntry)chooser.getSelectedTreeNode();
        if (fse != null) {
            name = fse.getFullName();
        }
        return name;
    }

    public String getDirectoryName() {
        String fullName = getSelectedFileFullName();
        String directoryName = StaticUtilities.getDirectoryName(fullName);
        return directoryName;
    }

    public String getSelectedFileName() {
        String fullName = getSelectedFileFullName();
        String fileName = StaticUtilities.getFileName(fullName);
        return fileName;
    }

    private FileSystemFilter[] getFileFilters(FileSystemView fsv) {
        FileSystemFilter[] filters = null;
        FileSystemFilter filter = new FileSystemFilter(fsv, CreateVDBPanel.VDB_IMPORT_EXTENSIONS,
                                                       CreateVDBPanel.VDB_TYPE_FILE_DESC);

        filters = new FileSystemFilter[] {
            filter
        };

        return filters;
    }

}// end ImportFileSelectionPanel

class BindingNameAndExistingUUID {

    private String bindingName;
    private String existingUUID;

    public BindingNameAndExistingUUID(String name,
                                      String uuid) {
        super();
        this.bindingName = name;
        this.existingUUID = uuid;
    }

    public String getBindingName() {
        return bindingName;
    }

    public String getExistingUUID() {
        return existingUUID;
    }
}// end BindingNameAndExistingUUID
