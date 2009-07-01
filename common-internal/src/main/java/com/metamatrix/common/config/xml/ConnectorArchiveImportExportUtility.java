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

package com.metamatrix.common.config.xml;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.output.XMLOutputter;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorArchive;
import com.metamatrix.common.config.api.ConnectorBindingType;
import com.metamatrix.common.config.api.ExtensionModule;
import com.metamatrix.common.config.model.BasicConnectorArchive;
import com.metamatrix.common.config.model.BasicExtensionModule;
import com.metamatrix.common.config.util.ConfigObjectsNotResolvableException;
import com.metamatrix.common.config.util.InvalidConfigurationElementException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.ByteArrayHelper;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.internal.core.xml.JdomHelper;


/**
 * This is utility class to import and export the Connector Archive file
 * The methods in this class only to be called through  XMLConfigurationImportExportUtility
 * class, they are separated due to readability only.
 * 
 * @since 4.3.2
 */
class ConnectorArchiveImportExportUtility {
    
    private static final String DEPLOYMENT_MANIFEST = "DeploymentManifest"; //$NON-NLS-1$
    private static final String EXTENSION_MODULE = "ExtensionModule"; //$NON-NLS-1$
    private static final String EXTENSION_MODULES = "ExtensionModules"; //$NON-NLS-1$
    private static final String TYPE = "Type"; //$NON-NLS-1$
    private static final String FILE = "File"; //$NON-NLS-1$
    private static final String NAME = "Name"; //$NON-NLS-1$
    private static final String CONNECTOR_TYPE = "ConnectorType"; //$NON-NLS-1$
    private static final String CONNECTOR_TYPES = "ConnectorTypes"; //$NON-NLS-1$
    private static final String SHARED = "Shared"; //$NON-NLS-1$
    private static final String MANIFEST_XML = "Manifest.xml"; //$NON-NLS-1$
    private static final String FWD_SLASH= "/"; //$NON-NLS-1$
    
    /**
     * The Zip file stream format is look like this.Also onlu used to 
     * import one connector type (it not current requirement to have multiple
     * connector types)
     * 
     *  Manifest.xml
     *  /ConnectorTypes
     *      /ConnectorTypeName
     *           ConnectorTypeName.xml
     *           extension1.jar
     *           extension2.jar
     *      /shared
     *          extension3.jar
     *  
     * @see com.metamatrix.common.config.util.ConfigurationImportExportUtility#importConnectorArchive(java.io.InputStream, com.metamatrix.common.config.api.ConfigurationObjectEditor, java.lang.String)
     * @since 4.3.2
     */
    public ConnectorArchive importConnectorArchive(InputStream stream, ConfigurationObjectEditor editor, XMLConfigurationImportExportUtility util) 
        throws IOException, InvalidConfigurationElementException {
        
        BasicConnectorArchive archive = new BasicConnectorArchive();
        
        File cafFile = File.createTempFile("ConnectorArchive", ".zip"); //$NON-NLS-1$ //$NON-NLS-2$            
        try {
            FileUtils.write(stream, cafFile);
            
            // First extract all the Connector Types in the zip file
            extractConnectorTypes(cafFile, editor, util, archive);
            
            // Now for each connector type extracted get allt eh extension modules. 
            ConnectorBindingType[] types = archive.getConnectorTypes();
            for (int i = 0; i < types.length; i++) {
                extractExtensionModules(cafFile, types[i], archive);
            }

            // finally extract the manifest file
            extractManifest(cafFile, archive);
        }finally {
            cafFile.delete();
        }        
        
        // before we return validate the archive
        validateArchive(archive);
        
        return archive;
    }

    /**
     * Extract all the Connector Types First from the configuration File 
     */
    void extractConnectorTypes(File cafFile, ConfigurationObjectEditor editor, XMLConfigurationImportExportUtility util, BasicConnectorArchive archive) 
        throws IOException, InvalidConfigurationElementException {

        // now open up the zip file for analysis
        ZipFile zip = new ZipFile(cafFile);
        Enumeration e = zip.entries();
        while(e.hasMoreElements()) {
            
            ZipEntry entry = (ZipEntry)e.nextElement();
            String entryName = entry.getName();                
            String fileName = entryName.substring(entryName.lastIndexOf('/')+1);
            
            if (!entry.isDirectory()) { 
                if (entryName.matches("^ConnectorTypes\\/.*\\/.*\\.cdk$")) { //$NON-NLS-1$
                    // load this as Connectory Type
                    InputStream in = zip.getInputStream(entry);
                    String cbName = fileName.substring(0, fileName.length()-4);
                    ComponentType type = util.importComponentType(in, editor, cbName);
                    archive.addConnectorType((ConnectorBindingType)type);
                }
            }                
        }
        
        // close the zip file
        zip.close();        
    }
    
    /**
     * Extract all the Extension modules from the archibe that are needed for the
     * given connector type. 
     * @param cafFile
     * @param type
     */
    void extractExtensionModules(File cafFile, ConnectorBindingType type, BasicConnectorArchive archive) 
        throws IOException{
        
        // get a list of extension module names.
        String[] extModuleNames = type.getExtensionModules();
        
        // now open up the zip file for analysis
        ZipFile zip = new ZipFile(cafFile);
        Enumeration e = zip.entries();
        while(e.hasMoreElements()) {
            ZipEntry entry = (ZipEntry)e.nextElement();
            String entryName = entry.getName();                
            String fileName = entryName.substring(entryName.lastIndexOf('/')+1);
            
            if (!entry.isDirectory()) { 
                
                for (int i = 0; i < extModuleNames.length; i++) {
                    if (entryName.equals(CONNECTOR_TYPES+FWD_SLASH+type.getName()+FWD_SLASH+extModuleNames[i]) 
                        || entryName.equals(CONNECTOR_TYPES+FWD_SLASH+SHARED+FWD_SLASH+extModuleNames[i]) ) {    
                        
                        InputStream in = zip.getInputStream(entry);
                        BasicExtensionModule module = new BasicExtensionModule(fileName, entry.getComment(),ByteArrayHelper.toByteArray(in));
                        archive.addExtensionModule(type, module);                        
                    }
                }                
            }
        }
        // close the zip file
        zip.close();                
    }
    
    /**
     * Extract the Manifest files contents. 
     * @param cafFile
     * @param archive
     * @throws IOException
     */
    void extractManifest(File cafFile, BasicConnectorArchive archive) 
        throws IOException {

        // now open up the zip file for analysis
        ZipFile zip = new ZipFile(cafFile);
        Enumeration e = zip.entries();
        while(e.hasMoreElements()) {
            
            ZipEntry entry = (ZipEntry)e.nextElement();
            String entryName = entry.getName();                
            
            if (!entry.isDirectory()) { 
                if (entryName.matches(MANIFEST_XML)) { 
                    // load this as Connectory Type
                    InputStream in = zip.getInputStream(entry);
                    archive.addMainfestContents(ByteArrayHelper.toByteArray(in));
                }
            }                
        }
        
        // close the zip file
        zip.close();        
    }    
    
    /** 
     * @see com.metamatrix.common.config.util.ConfigurationImportExportUtility#exportConnectorArchive(java.io.OutputStream, com.metamatrix.common.config.api.ConnectorArchive)
     * @since 4.3
     */
    public void exportConnectorArchive(OutputStream stream, ConnectorArchive archive, Properties properties, XMLConfigurationImportExportUtility util) 
        throws IOException, ConfigObjectsNotResolvableException {
        
        // Validate the archive submitted is correct one
        validateArchive(archive);
        
        // first create the manifest object, this will decide which modules 
        // are shared which modules are not. 
        Manifest mainfest = createManifest(archive);
        
        // create the output zip stream
        ZipOutputStream zipstream = new ZipOutputStream(stream);
        zipstream.putNextEntry(new ZipEntry(CONNECTOR_TYPES+FWD_SLASH));
        zipstream.closeEntry();
        
        HashSet writtenSharedFiles = new HashSet();
        
        // Now walk the each connector type and first write all the private extension modules
        ConnectorBindingType[] types = archive.getConnectorTypes();
        for (int typeIdx = 0; typeIdx < types.length; typeIdx++) {
            ConnectorBindingType type = types[typeIdx];
                                
            zipstream.putNextEntry(new ZipEntry(CONNECTOR_TYPES+FWD_SLASH+type.getName()+FWD_SLASH)); 
            zipstream.closeEntry();
            
            // First save the connector type xml file
            ZipEntry entry = new ZipEntry(getConnectorTypeFileName(type)); 
            zipstream.putNextEntry(entry);
            ByteArrayOutputStream baos = new ByteArrayOutputStream(10*1024);
            util.exportComponentType(baos, type, properties);
            zipstream.write(baos.toByteArray());
            zipstream.closeEntry();
            baos.close();
            
            // Now save the extension modules.
            ExtensionModule[] extModules = archive.getExtensionModules(type);
            for(int i = 0; i < extModules.length; i++) {
                // if this not shread then write to private area
                if (!mainfest.isShared(extModules[i].getFullName())) {
                    entry = new ZipEntry(getExtensionModuleFileName(type, extModules[i])); 
                    zipstream.putNextEntry(entry);
                    zipstream.write(extModules[i].getFileContents());
                    zipstream.closeEntry();
                }
                else {       
                    // if it shread not already written then write to shared directory
                    if (!writtenSharedFiles.contains(extModules[i].getFullName())) {
                        entry = new ZipEntry(getSharedExtensionModuleFileName(extModules[i])); 
                        zipstream.putNextEntry(entry);
                        zipstream.write(extModules[i].getFileContents());
                        zipstream.closeEntry();
                        writtenSharedFiles.add(extModules[i].getFullName());
                    }
                }
            }
        }
        
        // write manifest file
        byte[] manifest = createManifestFile(archive, mainfest);
        if (manifest != null) {
            ZipEntry entry = new ZipEntry(MANIFEST_XML);  
            zipstream.putNextEntry(entry);
            zipstream.write(manifest);
            zipstream.closeEntry();            
        }
        zipstream.finish();
    }
    
    /**
     * Contains matrix of types and their use of extension modules 
     */
    static class Manifest {
        // map between ext module --> list of shared connector types
        HashMap sharedList = new HashMap();
        // map between connector type --> list of private ext modules
        HashMap connectorList = new HashMap();
        
        void addShared(String extName, String type1, String type2) {
            HashSet list = (HashSet)sharedList.get(extName);
            if (list == null) {
                list = new HashSet();
            }
            list.add(type1);
            list.add(type2);
            sharedList.put(extName, list);
        }
        
        boolean isShared (String extName) {
            return sharedList.containsKey(extName);
        }
        
        void addPrivate(String type, String extName) {
            HashSet list = (HashSet)connectorList.get(type);
            if (list == null) {
                list = new HashSet();
            }
            list.add(extName);
            connectorList.put(type, list);
        }
    }
    
    /**
     * Create the Mainifest object for the Connector Archive Given 
     * @param archive
     * @return
     */
    Manifest createManifest(ConnectorArchive archive) {
        Manifest manifest = new Manifest();
        ConnectorBindingType[] types = archive.getConnectorTypes();

        // walk the types
        for (int currentType = 0; currentType < types.length; currentType++) {
            ExtensionModule[] currentModules = archive.getExtensionModules(types[currentType]);
        
            // walk the extension modules on given type
            for (int currentModule = 0; currentModule < currentModules.length; currentModule++) {
             
                boolean shared = false;
                
                // now match aginst other type's modules
                for (int nextType = currentType+1; nextType < types.length; nextType++) {
                    ExtensionModule[] nextModules = archive.getExtensionModules(types[nextType]);
                                 
                    for (int nextModule = 0; nextModule < nextModules.length; nextModule++) {
                        // if the names of the current and next maych then it is shared module
                        // otherwise they are separate
                        if (nextModules[nextModule].getFullName().equals(currentModules[currentModule].getFullName())) {
                            manifest.addShared(currentModules[currentModule].getFullName(), types[currentType].getName(), types[nextType].getName());
                            shared = true;
                        }
                    }
                }
                
                // the Current Module not shared then add under type 
                if (!shared) {
                    if (!manifest.isShared(currentModules[currentModule].getFullName())) {
                        manifest.addPrivate(types[currentType].getName(), currentModules[currentModule].getFullName());
                    }
                }
            }
        }        
        return manifest;
    }
    
      
    /**
     * Create a mainfest file for the Connector Archive 
     * @param archive
     * @return
     * @since 4.3
     */
    byte[] createManifestFile(ConnectorArchive archive, Manifest manifest) throws IOException {
        Element root = new Element(DEPLOYMENT_MANIFEST); 

        Element connectorTypesElement = new Element(CONNECTOR_TYPES); 
        
        // Loop through all the connector types and add them to the profile
        ConnectorBindingType[] types = archive.getConnectorTypes();
        for (int typeIdx = 0; typeIdx < types.length; typeIdx++) {
            ConnectorBindingType type = types[typeIdx];
                
            // Add connector type
            Element typeElement = new Element(CONNECTOR_TYPE); 
            typeElement.setAttribute(NAME, type.getName()); 
            typeElement.setAttribute(FILE, getConnectorTypeFileName(type)); 
        
            // Add extension modules
            Element extensionModulesElement = new Element(EXTENSION_MODULES); 
            ExtensionModule[] extModules = archive.getExtensionModules(type);
            for(int i = 0; i < extModules.length; i++) {
                Element extensionModuleElement = new Element(EXTENSION_MODULE); 
                extensionModuleElement.setAttribute(TYPE, extModules[i].getModuleType()); 
                if (manifest.isShared(extModules[i].getFullName())) {
                    extensionModuleElement.setAttribute(FILE, getSharedExtensionModuleFileName(extModules[i])); 
                }
                else {
                    extensionModuleElement.setAttribute(FILE, getExtensionModuleFileName(type, extModules[i])); 
                }
                extensionModulesElement.addContent(extensionModuleElement);
            }
            typeElement.addContent(extensionModulesElement);
            connectorTypesElement.addContent(typeElement);
        }

        // Add the types to the root
        root.addContent(connectorTypesElement);        
        
        // Now write the document..
        Document doc = new Document(root);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        XMLOutputter outputter = new XMLOutputter(JdomHelper.getFormat("    ", true)); //$NON-NLS-1$
        outputter.output(doc, bos);
        bos.flush();
        bos.close();        
        return bos.toByteArray();
    }
    
    private String getConnectorTypeFileName(ConnectorBindingType type) {
        return CONNECTOR_TYPES+FWD_SLASH+type.getName()+FWD_SLASH+type.getName()+".cdk"; //$NON-NLS-1$ 
    }

    private String getExtensionModuleFileName(ConnectorBindingType type, ExtensionModule module) {
        return CONNECTOR_TYPES+FWD_SLASH+type.getName()+FWD_SLASH+module.getFullName(); 
    }
    
    private String getSharedExtensionModuleFileName(ExtensionModule module) {
        return CONNECTOR_TYPES+FWD_SLASH+SHARED+FWD_SLASH+module.getFullName(); 
    }    
 
    /**
     * Validate that given archive has all the required extension modules, for the given types 
     * @param archive
     */
    boolean validateArchive(ConnectorArchive archive) {
        
        ConnectorBindingType[] types = archive.getConnectorTypes();        
        for (int typeIdx = 0; typeIdx < types.length; typeIdx++) {
            ConnectorBindingType type = types[typeIdx];
            String[] expectedModules = type.getExtensionModules();
            
            // extension modules 
            ExtensionModule[] foundModules = archive.getExtensionModules(type);
            
            for(int i = 0; i < expectedModules.length; i++) {
                boolean found = false;
                String moduleName = expectedModules[i];                
                for (int j = 0; j < foundModules.length; j++) {
                    ExtensionModule foundModule = foundModules[j];
                    if (foundModule.getFullName().equals(moduleName)) {
                        found = true;
                    }                    
                }
             
                // If an extension module not found then throw an exception
                if (!found) {
                    //throw new ConfigObjectsNotResolvableException("Connector Archive is incomplete \""+moduleName+"\" not found for Connector Type \""+type.getName()+"\"", moduleName); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                    LogManager.logWarning(LogConstants.CTX_CONFIG, CommonPlugin.Util.getString("ConnectorArchiveImportExportUtility.Invalid_archive_missing_jars", new Object[] {moduleName, type.getName()}));   //$NON-NLS-1$                 
                }
            }
        }
                
        return true;
    }
}
