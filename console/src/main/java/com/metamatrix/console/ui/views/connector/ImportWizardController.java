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

package com.metamatrix.console.ui.views.connector;

import java.awt.Component;
import java.awt.Point;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import javax.swing.JDialog;

import com.metamatrix.admin.api.objects.ConnectorBinding;
import com.metamatrix.admin.api.server.ServerAdmin;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorArchive;
import com.metamatrix.common.config.api.ConnectorBindingType;
import com.metamatrix.common.config.api.ExtensionModule;
import com.metamatrix.common.config.util.ConfigurationImportExportUtility;
import com.metamatrix.common.config.util.ConfigurationPropertyNames;
import com.metamatrix.common.config.util.InvalidConfigurationElementException;
import com.metamatrix.common.config.xml.XMLConfigurationImportExportUtility;
import com.metamatrix.common.tree.directory.DirectoryEntry;
import com.metamatrix.common.xml.XmlUtil;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConfigurationManager;
import com.metamatrix.console.models.ConnectorManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.security.UserCapabilities;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.util.WizardInterfaceImpl;
import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.StaticProperties;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.ObjectConverterUtil;
import com.metamatrix.platform.admin.api.ConfigurationAdminAPI;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;


public class ImportWizardController extends WizardInterfaceImpl implements ImportWizardControllerInterface {
  	public final static int CALLED_FOR_CONNECTOR_TYPE = 1;
  	public final static int CALLED_FOR_CONNECTOR_BINDING = 2;
  	
    protected final static int FILE_SELECTOR_PAGE_NUM = 0;
    protected final static int CONNECTOR_ID_PAGE_NUM = 1;
    protected final static int DUPLICATE_PAGE_NUM = 2;
    protected final static int CONFIRMATION_PAGE_NUM = 3;

    protected ImportWizardConnectorIDPanel itemIDPanel;
    protected ImportWizardFileSelectorPanel fileSelectorPanel;
    protected ImportWizardConfirmationPanel confirmPanel;
    protected ImportWizardDuplicatesPanel duplicatesPanel;

    protected JDialog dialog;
    protected int currentPage = -1;
    private ButtonWidget nextButton;
    private ButtonWidget cancelButton;
    private ButtonWidget finishButton;
    
    private TreeMap connectorTypesToImport;

    private ConfigurationImportExportUtility cieuImportUtil;
    protected DirectoryEntry directoryEntry = null;
    protected boolean bFinishPressed = false;

	private ConfigurationAdminAPI api;
	protected  ConfigurationManager manager;
	protected ConnectorManager connectorManager;
    private ConnectionInfo conn;
    
    protected String dialogTitle;
    
    private ExtensionModule[] extModulesToImport;
	
    public ImportWizardController(ConnectionInfo conn) {
        super();
        this.conn = conn;
        api = ModelManager.getConfigurationAPI(conn);
        manager = ModelManager.getConfigurationManager(conn);
        connectorManager = ModelManager.getConnectorManager(conn);       
    }

    public boolean runWizard() {
        loadWizard();

        dialog = new JDialog(ConsoleMainFrame.getInstance(), dialogTitle);
        dialog.setModal(true);
        cancelButton = getCancelButton();
        cancelButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                cancelPressed();
            }
        });

        nextButton = getNextButton();

        finishButton = getFinishButton();
        finishButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                finishPressed();
            }
        });
        dialog.getContentPane().add(this);
        dialog.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent ev) {
                cancelPressed();
            }
        });
        dialog.pack();


        setLocationOn(dialog);
        dialog.show();
        return bFinishPressed;
    }
    
    protected void loadWizard() {
        fileSelectorPanel = new ImportWizardFileSelectorPanel(this, this,CALLED_FOR_CONNECTOR_TYPE);
        addPage(fileSelectorPanel);

        itemIDPanel = new ImportWizardConnectorIDPanel(this, CALLED_FOR_CONNECTOR_TYPE);
        addPage(itemIDPanel);

        duplicatesPanel = new ImportWizardDuplicatesPanel(this, this);
        addPage(duplicatesPanel);
        
        confirmPanel = new ImportWizardConfirmationPanel(this, this, CALLED_FOR_CONNECTOR_TYPE);
        addPage(confirmPanel);
        
        dialogTitle = "Import Connector Type Wizard"; //$NON-NLS-1$
        currentPage = 0;      
    }

    public JDialog getDialog() {
        return dialog;
    }

    private void cancelPressed() {
        dialog.dispose();
    }

    public void importFileSelected() {
        nextButton.setEnabled(true);
    }

    protected void finishPressed() {
        bFinishPressed      = true;
        if (saveNewItemToServer()) {
            if (this.connectorTypesToImport != null && !this.connectorTypesToImport.isEmpty()) {
                for (Iterator i=this.connectorTypesToImport.keySet().iterator(); i.hasNext();) {
                    connectorManager.reportConnectorDetails((ComponentType)this.connectorTypesToImport.get(i.next()));
                }
            }
        }
        dialog.dispose();
    }

    public void showNextPage() {
        int numPagesForward = 0;
        try {
            StaticUtilities.startWait(getDialog().getContentPane());
            numPagesForward = goingToNextPage();
        } catch (Exception e) {

        	String msgText = "Failed testing new connector name for uniqueness"; //$NON-NLS-1$
			ExceptionUtility.showMessage(msgText, e);
        } finally {
           StaticUtilities.endWait(getDialog().getContentPane());
        }

        for (int i=0; i<numPagesForward; i++) {
            super.showNextPage();
        }

    }

    public void showPreviousPage() {
        int numPagesBackward = goingToPreviousPage();
        
        for (int i=0; i<numPagesBackward; i++) {
            super.showPreviousPage();
        }
    }

    public int goingToNextPage() throws Exception {
        int numPagesForward = 0;
        switch (currentPage) {

            case FILE_SELECTOR_PAGE_NUM:
                // save the selected DirectoryEntry
                directoryEntry = fileSelectorPanel.getSelection();                                
                if (isImportingCDK()) {
                    //.cdk/.xml file      
                    loadCDK(directoryEntry);
                } 
                else {
                    //.caf file
                    loadCAF(directoryEntry);     
                }
                itemIDPanel.setExtensionModules(this.extModulesToImport);
                itemIDPanel.setConnectorTypes(this.directoryEntry, this.connectorTypesToImport);                
                numPagesForward = 1;
                break;
                
            case CONNECTOR_ID_PAGE_NUM:
                // get the modified names
                this.connectorTypesToImport = itemIDPanel.getConnectorTypes();
                
                ServerAdmin admin = conn.getServerAdmin(); 
                
                String[] duplicateConnectorTypes = checkForExistingConnectorTypes(admin, this.connectorTypesToImport);
                String[] duplicateExtensionModules = checkForExistingExtensionModules(admin, this.extModulesToImport);
                            
                String errorWhy = checkVDBAssosiations(admin, duplicateConnectorTypes);
                
                // show the duplicates panel
                duplicatesPanel.setDuplicateConnectorTypes(duplicateConnectorTypes);
                duplicatesPanel.setDuplicateExtensionModules(duplicateExtensionModules);
                duplicatesPanel.setErrorText(errorWhy);
                duplicatesPanel.setStepNumber(3);

                numPagesForward = 1;                
                break;
                
            case DUPLICATE_PAGE_NUM:
                this.confirmPanel.setConnectorTypes(this.connectorTypesToImport);
                this.confirmPanel.setExtensionModules(this.extModulesToImport);
                confirmPanel.setStepNumber(4);

                numPagesForward = 1;
                break;
                
            case CONFIRMATION_PAGE_NUM:
                //this represents the last page, but we take no action here
                //because the final work is done in 'finishPressed()'.
                break;
        }
        
        //hack
        currentPage += numPagesForward;
        return numPagesForward;
    }

    /** 
     * @param admin
     * @param duplicateConnectorTypes
     * @return
     */
    private String checkVDBAssosiations(ServerAdmin admin, String[] duplicateConnectorTypes) throws Exception {
        if (duplicateConnectorTypes != null && duplicateConnectorTypes.length > 0) {
            StringBuffer text = new StringBuffer();            
            // Get the bindings in the VDB
            Collection bindings = admin.getConnectorBindings("*");
            for (Iterator bindingsIter=bindings.iterator(); bindingsIter.hasNext();) {
                ConnectorBinding binding = (ConnectorBinding)bindingsIter.next();
                
                // Now Check if the connector type name in the duplicates List
                for (int i = 0; i < duplicateConnectorTypes.length; i++) {
                    if (duplicateConnectorTypes[i].equalsIgnoreCase(binding.getConnectorTypeName())) {
                        text.append("\n{");
                        text.append(duplicateConnectorTypes[i]);
                        text.append("} in use by : ");
                        text.append(binding.getIdentifier());
                        text.append("\n");
                    }
                }
            }
            return text.toString();
        }
        return null;
    }

    public int goingToPreviousPage() {
        int numPagesBackward = 1;
        currentPage -= numPagesBackward;
        return numPagesBackward;
    }

    protected ConfigurationImportExportUtility getImportExportUtility() {
        if (cieuImportUtil == null) {
            try {
                cieuImportUtil = new XMLConfigurationImportExportUtility();
            } catch (Exception e) {
                ExceptionUtility.showMessage("Failed to get import utility", e); //$NON-NLS-1$
            }

        }
        return cieuImportUtil;
    }

    /**
     * <p>Package-level utility method</p>
     *
     * <p>Connector names cannot have a period in them, so this will
     * take the substring to the left of the first "." which in
     * almost all cases will mean that it simply strip off any
     * file extension</p>
     *
     * <p>Used by this class, and also
     * {@link ImportWizardConnectorIDPanel ImportWizardConnectorIDPanel}.</p>
     */
   public static String stripFileExtensionFromName(String sName) {
        String period = "."; //$NON-NLS-1$
        int iMarkerPos = sName.indexOf(period);
        if (iMarkerPos > -1) {
            return sName.substring(0, iMarkerPos);
        }
        return sName;
    }
    
	
    protected ConfigurationObjectEditor getNewItemEditor() throws MetaMatrixRuntimeException {
        try {
            return api.createEditor();
        } catch (Exception e) {
        	String msg = "Import Error: Failed to create editor"; //$NON-NLS-1$
            ExceptionUtility.showMessage(msg, e);
            throw new MetaMatrixRuntimeException(msg);
        }        
    }
    
       
    private  void loadCDK(DirectoryEntry deFile) {
        try {
            ConfigurationObjectEditor editor = getNewItemEditor();
            Collection cTypes = getImportExportUtility().importComponentTypes(deFile.getInputStream(), editor);
            editor.getDestination().popActions();
            
            // mark that there are no extension modules.
            this.extModulesToImport = null;

            // load the connector types.
            this.connectorTypesToImport = new TreeMap(new HashMap(cTypes.size()));
            for (Iterator it=cTypes.iterator(); it.hasNext();) {
                ConnectorBindingType t = (ConnectorBindingType) it.next();
                this.connectorTypesToImport.put(t.getFullName(), t);
            }        
                                    
        }  catch (IOException ioe) {
            String msg = "Failed while importing a connector type(s)"; //$NON-NLS-1$
            String comment = "Error importing connector type(s), verify the file is a valid CDK or CAF file."; //$NON-NLS-1$
            ExceptionUtility.showMessage(msg, comment, ioe);
            
        } catch(InvalidConfigurationElementException ice) {
            String msg = "Failed while importing a connector type(s)"; //$NON-NLS-1$
            String comment = "Error importing connector type(s), verify the file is a valid CDK or CAF file."; //$NON-NLS-1$
            ExceptionUtility.showMessage(msg, comment, ice);

        } catch (Exception e) {
            String msg = "Failed while importing a connector type."; //$NON-NLS-1$
            ExceptionUtility.showMessage(msg, e.getMessage(), e);
        } 
    }
    
    private void loadCAF(DirectoryEntry deFile) {
        try {
            ConfigurationObjectEditor editor = getNewItemEditor();
            ConnectorArchive newArchive = getImportExportUtility().importConnectorArchive(deFile.getInputStream(), editor);
            editor.getDestination().popActions();
            
            // mark the extension modules
            this.extModulesToImport = getAllExtensionModules(newArchive);            

            // load the connector types into map
            this.connectorTypesToImport = new TreeMap();        
            ConnectorBindingType[] types = newArchive.getConnectorTypes();
            for (int i = 0; i < types.length; i++) {
                ConnectorBindingType type = types[i];
                this.connectorTypesToImport.put(type.getFullName(), type);
            }
                                    
        } catch (IOException ioe) {
            String msg = "Failed while importing a connector type(s)"; //$NON-NLS-1$
            String comment = "Error importing connector type(s), verify the file is a valid CDK or CAF file."; //$NON-NLS-1$
            ExceptionUtility.showMessage(msg, comment, ioe);
            
        } catch(InvalidConfigurationElementException ice) {
            String msg = "Failed while importing a connector type(s)"; //$NON-NLS-1$
            String comment = "Error importing connector type(s), verify the file is a valid CDK or CAF file."; //$NON-NLS-1$
            ExceptionUtility.showMessage(msg, comment, ice);

        } catch (Exception e) {
            String msg = "Failed while importing a connector type."; //$NON-NLS-1$
            ExceptionUtility.showMessage(msg, e.getMessage(), e);
        } 
    }
    
    // Extract all the extension modules used by the all the connectors
    private ExtensionModule[] getAllExtensionModules(ConnectorArchive archive) {
        HashSet set = new HashSet();
        ConnectorBindingType[] types = archive.getConnectorTypes();
        for (int i = 0; i < types.length; i++) {
            ExtensionModule[] modules = archive.getExtensionModules(types[i]);
            for (int m = 0; m < modules.length; m++) {
                set.add(modules[m]);
            }
        }        
        return (ExtensionModule[])set.toArray(new ExtensionModule[set.size()]);
    }
    
    private String[] checkForExistingExtensionModules(ServerAdmin admin, ExtensionModule[] modules) throws Exception {
        if (modules != null && modules.length > 0) {
            ArrayList duplicateModuleNames = new ArrayList();
            for (int i=0; i<modules.length; i++) {
                ExtensionModule module = modules[i];
                Collection results = admin.getExtensionModules(module.getFullName());
                if (! results.isEmpty()) {
                    duplicateModuleNames.add(module.getFullName());
                } 
            }        
            return (String[])duplicateModuleNames.toArray(new String[duplicateModuleNames.size()]);
        }
        return null;
    }
    
    private String[] checkForExistingConnectorTypes(ServerAdmin admin, Map connectorTypesMap) throws Exception {
        if (connectorTypesMap != null && !connectorTypesMap.isEmpty()) {
            ArrayList duplicateConnectorTypes = new ArrayList();
            for (Iterator i = connectorTypesMap.keySet().iterator(); i.hasNext();) {
                String typename = (String)i.next();
                Collection results = admin.getConnectorTypes(typename);
                if (results != null && !results.isEmpty()) {
                    duplicateConnectorTypes.add(typename);
                } 
            }         
            return (String[])duplicateConnectorTypes.toArray(new String[duplicateConnectorTypes.size()]);
        }
        return null;
    }    
    

    protected boolean saveNewItemToServer() {
    	
        // ** Add the new Connector Types **
    	// At this point, we have established that there are no conflicts, and
    	// the user must agree to overwrite the type if it is a duplicate
        try {
            addConnectorTypes();
        } catch (Exception e) {
            ExceptionUtility.showMessage("Failed attempting to save connector types.", e); //$NON-NLS-1$
            return false;
        }        
        
        // ** Add the extension modules **
        // Use the user selection on whether to overwrite the duplicate jars.
        boolean overwriteExtJars = this.duplicatesPanel.isOverwriteExtJarsPressed();
        try {
            addExtensionModules(overwriteExtJars);
        } catch (Exception e) {
            ExceptionUtility.showMessage("Failed attempting to save extension modules.", e); //$NON-NLS-1$
            return false;
        }
        
        manager.refresh();
        return true;
        
    }

    private void addConnectorTypes() throws Exception {
        if (this.connectorTypesToImport != null) {
            
            ServerAdmin admin = conn.getServerAdmin();
            
            for (Iterator i = this.connectorTypesToImport.keySet().iterator(); i.hasNext();) {
                String name = (String)i.next(); 
                ConnectorBindingType type = (ConnectorBindingType)this.connectorTypesToImport.get(name);
                
                // since we do not have a way to add the conector type with object model, and we
                // may have come from CAF format, we do not have char[] to import; So we export this
                // first; then import. 
                Collection types = admin.getConnectorTypes(name);
                if (types != null && !types.isEmpty()) {
                    admin.deleteConnectorType(name);
                }
                
                // add the new connector type
                ByteArrayOutputStream bos = new ByteArrayOutputStream(4096);
                getImportExportUtility().exportConnector(bos, type, getExportProperties());
                
                char[] contents = new String(bos.toByteArray()).toCharArray();
                admin.addConnectorType(name, contents);
            }
        }
        
    }
    
    private Properties getExportProperties() throws Exception {
        String userName = UserCapabilities.getLoggedInUser(this.conn).getName();
        String version = StaticProperties.getVersion();
        Properties props = new Properties();
        props.put(ConfigurationPropertyNames.APPLICATION_CREATED_BY,DeployPkgUtils.getString("dmp.console.name")); //$NON-NLS-1$
        props.put(ConfigurationPropertyNames.APPLICATION_VERSION_CREATED_BY,version);
        props.put(ConfigurationPropertyNames.USER_CREATED_BY, userName);
        return props;
        
    }

    private void addExtensionModules(boolean overwriteDuplicateExtJars) throws Exception {
        if (this.extModulesToImport != null) {
        	// Get list of duplicates
        	List duplicateExtensionJars = this.duplicatesPanel.getDuplicateExtensionModules();
            ServerAdmin admin = conn.getServerAdmin();
            for (int i=0; i<extModulesToImport.length; i++) {
                ExtensionModule module = extModulesToImport[i];
                String moduleName = module.getFullName();
                // If this is duplicate, use overwrite value to determine overwrite
                if(duplicateExtensionJars.contains(moduleName)) {
                	if(overwriteDuplicateExtJars) {
                    	admin.deleteExtensionModule(moduleName);
                    	admin.addExtensionModule(module.getModuleType(), moduleName, module.getFileContents(), module.getDescription());
                	}
                } else {
                	admin.deleteExtensionModule(moduleName);
                	admin.addExtensionModule(module.getModuleType(), moduleName, module.getFileContents(), module.getDescription());
                }
            }
        }
    }
    

    public static void setLocationOn(Component comp) {
        Point p = StaticUtilities.centerFrame(comp.getSize());
        comp.setLocation(p.x, p.y);
    }
    
    private boolean isImportingCDK() {
        InputStream is = null;
        try {
            is = directoryEntry.getInputStream();
            String contents = ObjectConverterUtil.convertToString(is);
            return (XmlUtil.containsValidCharacters(contents) == null);
        } catch (Exception e) {
            return false;
        } finally {        
            try {
                is.close();
            } catch (Exception e) {
            }
        }
    }
    
    TreeMap getLoadedTypes() {
        return this.connectorTypesToImport;
    }
}
