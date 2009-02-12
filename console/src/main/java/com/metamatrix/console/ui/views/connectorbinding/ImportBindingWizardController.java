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

import java.awt.Component;
import java.awt.Point;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.swing.JDialog;

import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ProductServiceConfig;
import com.metamatrix.common.config.util.ConfigurationImportExportUtility;
import com.metamatrix.common.config.util.InvalidConfigurationElementException;
import com.metamatrix.common.config.xml.XMLConfigurationImportExportUtility;
import com.metamatrix.common.tree.directory.DirectoryEntry;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConfigurationManager;
import com.metamatrix.console.models.ConnectorManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.util.WizardInterfaceImpl;
import com.metamatrix.console.ui.views.connector.ImportWizardControllerInterface;
import com.metamatrix.console.ui.views.connector.ImportWizardFileSelectorPanel;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.platform.admin.api.ConfigurationAdminAPI;
import com.metamatrix.platform.admin.api.ExtensionSourceAdminAPI;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;


public class ImportBindingWizardController extends WizardInterfaceImpl implements ImportWizardControllerInterface {
	
    private final static String CONNECTOR_CLASSPATH = "ConnectorClassPath";
    
    protected final static int FILE_SELECTOR_PAGE     = 0;
    protected final static int CONNECTOR_ID_PAGE      = 1;
    protected final static int PSC_ASSIGNMENT_PAGE      = 2;    
    protected final static int CONFIRMATION_PAGE      = 3;
    
    protected final static int CALLED_FOR_CONNECTOR_TYPE = 1;
    protected final static int CALLED_FOR_CONNECTOR_BINDING = 2;
   
    
    protected ImportWizardFileSelectorPanel fileSelectorPanel;
    protected ImportBindingWizardPSCEnablePanel pscEnablePanel;
    private ImportBindingWizardRenamePanel itemRenamePanel;
    
    private ButtonWidget nextButton;
    private ButtonWidget cancelButton;
    private ButtonWidget finishButton;
    
    protected JDialog dialog;
   
    protected int currentPage = -1;
    
    private String sXMLFileName;
    // bindings loaded from the imported file, but not yet created to the server
    private Map loadedConnBindings;
    private Map componentTypes;
    // imported connector types that need to be created 
    private Collection connTypesToCreate;    
    // a map between the imported binding name and the new name (or existing name if not changed)
    private Map renameMapping;
    
    protected DirectoryEntry deFile = null;
    protected boolean bFinishPressed = false;
    
    protected String dialogTitle;    
    

    private ConfigurationAdminAPI api;
    protected ConfigurationManager manager;
    protected ConnectorManager connectorManager;
    protected ExtensionSourceAdminAPI extensionAPI;

    private ConfigurationObjectEditor newComponentEditor;
    private ConfigurationImportExportUtility cieuImportUtil;
    

    
    public ImportBindingWizardController(ConnectionInfo conn) {
        super();
        api = ModelManager.getConfigurationAPI(conn);
        manager = ModelManager.getConfigurationManager(conn);
        connectorManager = ModelManager.getConnectorManager(conn);
        extensionAPI = ModelManager.getExtensionSourceAPI(conn);
        
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
//        backButton = 
            getBackButton();

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
    
    public static void setLocationOn(Component comp) {
        Point p = StaticUtilities.centerFrame(comp.getSize());
        comp.setLocation(p.x, p.y);
    }    
    
    private void cancelPressed() {
        dialog.dispose();
    }    
    public Collection getConnectorBindings() {
        return new ArrayList(loadedConnBindings.values());
    }
    
    public void importFileSelected() {
        nextButton.setEnabled(true);
    }    
    
    public JDialog getDialog() {
        return dialog;
    }    

    protected void loadWizard() {
             
            //new ImportWizardConnectorIDPanel(this, CALLED_FOR_CONNECTOR_BINDING);
        fileSelectorPanel = new ImportWizardFileSelectorPanel(this, this,
                                                              CALLED_FOR_CONNECTOR_BINDING);
        itemRenamePanel = new ImportBindingWizardRenamePanel(this, connectorManager); 

        //        confirmPanel = new ImportWizardConfirmationPanel(this, this, CALLED_FOR_CONNECTOR_BINDING);
        pscEnablePanel = new ImportBindingWizardPSCEnablePanel(this, connectorManager);
            //new NewBindingWizardPSCEnablePanel(this, connectorManager);
       
        addPage(fileSelectorPanel);
        addPage(itemRenamePanel);
        addPage(pscEnablePanel);        
//        addPage(confirmPanel);
        
        dialogTitle = "Import Connector Binding(s) Wizard"; //$NON-NLS-1$
        
        currentPage = 0;            
       
    }
    
    
    
    
    
    
    
    
    
    public void showPreviousPage() {
        goingToPreviousPage();
        super.showPreviousPage();
    }
    public void showNextPage() {
        boolean bContinue = false;
        try {
            StaticUtilities.startWait(getDialog().getContentPane());
            bContinue = goingToNextPage();
        } catch (Exception e) {
        	String msgText = "Failed testing new connector binding name for uniqueness"; //$NON-NLS-1$

			ExceptionUtility.showMessage(msgText, e);
        } finally {
           StaticUtilities.endWait(getDialog().getContentPane());
        }

        if (bContinue)
        {
            super.showNextPage();
        }

    }
    
    
    

    public void goingToPreviousPage() {
        currentPage -= 1;
    }
    public boolean goingToNextPage() {
        boolean bContinue       = true;
        switch (currentPage) {
            case FILE_SELECTOR_PAGE:
                // save the selected DirectoryEntry
                deFile = fileSelectorPanel.getSelection();
                sXMLFileName = deFile.getName();
                if (deFile != null) {
                    loadConnectorBindings(deFile);
                    bContinue=doConnectorTypesExists() & doConnectorBindingsExist();
                    if (bContinue) {
                        itemRenamePanel.setConnectorBindings(sXMLFileName, getConnectorBindingNames(loadedConnBindings));
                    }
                } else {
                	bContinue = false;
                }
                break;
            case CONNECTOR_ID_PAGE:
                renameMapping = itemRenamePanel.getConnectorBindingMapping();
                if (renameMapping == null || renameMapping.isEmpty()) {
                    // nothing to import
                    bContinue = false;
                } else {
                    pscEnablePanel.setNewConnectorBindingInfo(renameMapping.values());
                }

                break;
//            case CONFIRMATION_PAGE:
//                //this represents the last page, but we take no action here
//                //because the final work is done in 'finishPressed()'.
//                break;
                
                
            case PSC_ASSIGNMENT_PAGE:
//                pscEnablePanel.get
//                    bContinue = createNewItem(deFile);
//                    if (bContinue) {
//                        String itemName = getNewName();
//
//                        confirmPanel.setItemName(itemName);
//                                                
//                    } else {
//                        break;
//                    }

                
                
                break;
        }
        if (bContinue) {
            currentPage += 1;
        }
        return bContinue;
    }
    
    protected void finishPressed() {

        bFinishPressed      = true;
        
        Collection newBindings = createNewBindings();
        
        ProductServiceConfig[] enabledConfigs = 
            pscEnablePanel.getEnabledConfigs();
        
        if (saveNewItemToServer(newBindings, enabledConfigs)) {
            validateExtensions(newBindings);
            dialog.dispose();
        }
        
    }    
    
    private void validateExtensions(Collection bindings) {
        List extensions = null;
        try {
            extensions = this.extensionAPI.getSourceNames();
        } catch (Exception err) {
            return; // can't get the resources so just assume they are there.
        }
        StringBuffer sb = new StringBuffer();
        Iterator iter = bindings.iterator();
        while (iter.hasNext()) {
            boolean err = false;
            ConnectorBinding cb = (ConnectorBinding) iter.next();
            List extensionList = getExtensionListForBinding(cb);
            Iterator extensionIter = extensionList.iterator();
            while (extensionIter.hasNext()) {
                String extName = (String) extensionIter.next();
                if (!extensions.contains(extName)) {
                    if (!err) {
                        sb.append("\nThe following extensions are missing for the " + cb.getName() + " connector."); //$NON-NLS-1$ //$NON-NLS-2$
                        err = true;
                    }
                    sb.append("\n    " + extName); //$NON-NLS-1$
                }
            }
        }
        if (sb.length() > 0) {
            String hdr = "Warning: Missing extension modules"; //$NON-NLS-1$
            String msg = "Some required extension modules are missing, connectors may not start properly.\n";  //$NON-NLS-1$
            StaticUtilities.displayModalDialogWithOK(hdr, msg + sb.toString());
        }
    }

   private List getExtensionListForBinding(ConnectorBinding cb) {
       String cp = cb.getProperty(CONNECTOR_CLASSPATH);
       List l = StringUtil.getTokens(cp, ";"); //$NON-NLS-1$
       List nl = new ArrayList();
       for (int i = 0; i < l.size(); i++) {
           String extension = (String) l.get(i);
           int index = extension.indexOf(":"); //$NON-NLS-1$
           nl.add(extension.substring(index+1));
       }
       return nl;
   }
    
    
    private void loadConnectorBindings(DirectoryEntry deFile) {
        ConfigurationObjectEditor coe = getNewItemEditor();
        try {
           
            // if no types are found in the xml file, that is ok, but the types
            // must already exist in the configuration.
            loadConnectorTypes(deFile, false);
            
            Collection bindings = getImportExportUtility().importConnectorBindings(
                    deFile.getInputStream(), coe);
            coe.getDestination().popActions();
            
            loadedConnBindings = new HashMap(bindings.size());
            for (Iterator it=bindings.iterator(); it.hasNext();) {
                ConnectorBinding t = (ConnectorBinding) it.next();
                loadedConnBindings.put(t.getFullName(), t);
            }
            
           

        } catch (IOException ioe) {
            String msg;
            msg = "Failed while importing a connector binding(s)"; //$NON-NLS-1$
            String comment = "Error importing connector binding(s), verify the xml file is well formatted."; //$NON-NLS-1$

            ExceptionUtility.showMessage(msg, comment, ioe);
             
        } catch (Exception e) {
            String msg;

            msg = "Failed while importing a connector binding."; //$NON-NLS-1$

            ExceptionUtility.showMessage(msg, e.getMessage(), e);
        } 
        
       
        
    }
    
    
    private Collection getConnectorBindingNames(Map connbindings) {
        Collection names = null;
        names = new ArrayList(connbindings.size());
        for (Iterator it=connbindings.keySet().iterator(); it.hasNext();) {
            String t = (String) it.next();
            names.add(t);
        }
 
        return names;         
        
    }
    
    protected Collection loadConnectorTypes(DirectoryEntry deFile, boolean errorIfNoneFound) {
        ConfigurationObjectEditor coe = getNewItemEditor();
        Collection cTypes = null;

        try {
            cTypes = getImportExportUtility().importComponentTypes(
                                        deFile.getInputStream(),
                                        coe);
            coe.getDestination().popActions();

            componentTypes = new HashMap(cTypes.size());
            for (Iterator it=cTypes.iterator(); it.hasNext();) {
                ComponentType t = (ComponentType) it.next();
                componentTypes.put(t.getFullName(), t);
            }
            
            
        } catch (IOException ioe) {
            String msg;
            
            msg = "Failed while importing a connector type(s)"; //$NON-NLS-1$

            String comment = "Error importing connector type(s), verify the xml file is well formatted."; //$NON-NLS-1$

            ExceptionUtility.showMessage(msg, comment, ioe);
            
        } catch(InvalidConfigurationElementException ice) {
            // this exception is thrown because there is no element in the file
            // for connector type
            if (errorIfNoneFound) {
                String msg;
                msg = "Failed while importing a connector type(s)"; //$NON-NLS-1$
                String comment = "No connector type(s) found in the file, verify the xml file is well formatted."; //$NON-NLS-1$

                ExceptionUtility.showMessage(msg, comment, ice);
                
            } else {
                componentTypes = null;
            }

        } catch (Exception e) {
            String msg;
            msg = "Failed while importing a connector type."; //$NON-NLS-1$

            ExceptionUtility.showMessage(msg, e.getMessage(), e);
        } 
        return cTypes;
        
    }
    
    
    protected Collection getConnectorTypeNames() {
        Collection names = null;

            names = new ArrayList(componentTypes.size());
            for (Iterator it=componentTypes.keySet().iterator(); it.hasNext();) {
                String t = (String) it.next();
                names.add(t);
            }
        
        return names;           
     }
    
    private boolean doConnectorBindingsExist() {
        if (loadedConnBindings.isEmpty()) {
            String hdr = "File contains no bindings"; //$NON-NLS-1$
            String msg = "The selected file contains no connector binding definitions, either select a different file to import or go to the connector types panel to import connector types.";  //$NON-NLS-1$
            StaticUtilities.displayModalDialogWithOK(hdr, msg);
            return false;
        }
        return true;
    }
    
    private boolean doConnectorTypesExists() {
        connTypesToCreate = new ArrayList(loadedConnBindings.size());
         try {
        
            for (Iterator it=loadedConnBindings.keySet().iterator(); it.hasNext();) {
                String name = (String) it.next();
                final ConnectorBinding importedBinding = (ConnectorBinding) loadedConnBindings.get(name);
                // determine if the connector type is in the file,
                // if so, see if it already exist, 
                // if type does not exist, then import it           
                ComponentType importedtype = getConnectorType(importedBinding.getComponentTypeID().getFullName());
                boolean typeExists = connectorManager.connectorTypeNameAlreadyExists(importedBinding.getComponentTypeID().getFullName()) ;
                 
                // if the type doesnt exist and is not imported, then error
                 if (!typeExists) {
                     if (importedtype == null) {
                     
                         String hdr = "Connector Not Found"; //$NON-NLS-1$
                         String msg = "The connector  " + importedBinding.getComponentTypeID().getFullName() +  //$NON-NLS-1$
                             " does not exist and is not in the imported file " + sXMLFileName + ".  Please import the connector before importing the binding."; //$NON-NLS-1$ //$NON-NLS-2$
                         StaticUtilities.displayModalDialogWithOK(hdr, msg);
                         return false;
                     }
                     
                     // save for creation later
                     connTypesToCreate.add(importedtype);
                 }                    
                
            }
        
        } catch (Exception e) {
            String msg;
                msg = "Failed while verifying connector types for each importer connector binding."; //$NON-NLS-1$
            ExceptionUtility.showMessage(msg, e);
            return false;
        }
        
        
        return true;
    }
    
    protected ComponentType getConnectorType(String typeName) {
        return (ComponentType) componentTypes.get(typeName);
    }    
    

    private Collection createNewBindings() {
        ConfigurationObjectEditor newComponentEditor = getNewItemEditor();
        
        Configuration config = manager.getConfig(Configuration.NEXT_STARTUP_ID);
        List newBindings = new ArrayList(renameMapping.size());

        if (config == null) {
            Exception e = new Exception("Configuation was not found in order to add the connector binding."); //$NON-NLS-1$
            String msg;
            msg = "Failed while creating new connector binding on import."; //$NON-NLS-1$
            ExceptionUtility.showMessage(msg, e);
            return newBindings;

        }

        try {
            
            // create the new types that are from the imported file
            for (Iterator typeIt=connTypesToCreate.iterator(); typeIt.hasNext();) {
                final ComponentType t = (ComponentType) typeIt.next();
                createNewItem(t);
            }
            
            for (Iterator it=renameMapping.keySet().iterator(); it.hasNext();) {
                String name=(String)it.next();
                String newName=(String)renameMapping.get(name);
                
                ConnectorBinding importedBinding = (ConnectorBinding)loadedConnBindings.get(name);
                ConnectorBinding newBinding = newComponentEditor.createConnectorComponent(Configuration.NEXT_STARTUP_ID, importedBinding, newName, null);
                
                newBindings.add(newBinding);
                
                                                               
            }
            
            manager.checkDecryptable(newBindings);
        } catch (Exception e) {
            String msg;
                msg = "Failed while creating new connector binding on import."; //$NON-NLS-1$
            ExceptionUtility.showMessage(msg, e);
        }
        
       return newBindings;
    }


    
    protected boolean createNewItem(ComponentType compType) {
        try {
            newComponentEditor = getNewItemEditor();
                
            newComponentEditor.createComponentType(compType, null);
             
        } catch (Exception e) {
            String msg;
                msg = "Failed while creating new connector type on import."; //$NON-NLS-1$
             ExceptionUtility.showMessage(msg, e);
        }
        
       return true;
    }     
    
    protected ConfigurationObjectEditor getNewItemEditor() {
        if (newComponentEditor == null) {
            try {
                newComponentEditor = api.createEditor();
            } catch (Exception e) {
                String msg;
                    msg = "Import Error: Failed to create editor"; //$NON-NLS-1$
                ExceptionUtility.showMessage(msg, e);
            }
        }
        return newComponentEditor;
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
    
    protected boolean saveNewItemToServer(Collection bindings, ProductServiceConfig[] pscs) {
        try {
            
            ConfigurationObjectEditor newComponentEditor = getNewItemEditor();
            
            connectorManager.createConnectorBinding(bindings, newComponentEditor, pscs);
        } catch (Exception e) {
            String msg;
                msg = "Failed attempting to save changes on server."; //$NON-NLS-1$
            ExceptionUtility.showMessage(msg, e);
            return false;
        }
        manager.setRefreshNeeded();
        return true;
        
    }
    

 }
