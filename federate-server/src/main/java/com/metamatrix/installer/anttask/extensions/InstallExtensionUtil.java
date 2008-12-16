/*
 * Copyright © 2000-2005 MetaMatrix, Inc.  All rights reserved.
 */
package com.metamatrix.installer.anttask.extensions;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.extensionmodule.ExtensionModuleDescriptor;
import com.metamatrix.common.extensionmodule.ExtensionModuleInstallUtil;
import com.metamatrix.common.extensionmodule.ExtensionModulePropertyNames;
import com.metamatrix.common.extensionmodule.ExtensionModuleTypes;
import com.metamatrix.common.pooling.api.ResourcePool;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.StringUtil;

public class InstallExtensionUtil {

    private static final String PRINCIPAL = "InstallExtensionFiles";//$NON-NLS-1$

    public static final String ALL = "all";

    public static final String EXTENSION_XML_FILE = "extensiondescriptor";
    public static final String DEFAULT_EXTENSION_XML_FILE = "extensiondescriptor.xml";

    public static ExtensionModuleInstallUtil createExtensionModuleInstallUtil(Properties properties) throws MetaMatrixException {
        Properties newProps = new Properties();
        newProps.putAll(properties);
        newProps
                .setProperty(ExtensionModulePropertyNames.CONNECTION_FACTORY,
                             com.metamatrix.common.extensionmodule.spi.jdbc.JDBCExtensionModuleTransactionFactory.class.getName());

        if (!newProps.containsKey(ResourcePool.RESOURCE_POOL)) {
            newProps.setProperty(ResourcePool.RESOURCE_POOL, ResourcePool.JDBC_SHARED_CONNECTION_POOL);
        }

        return new ExtensionModuleInstallUtil(newProps);
    }

    public static List importExtensionModulesFromDescriptor(ExtensionModuleInstallUtil util,
                                                            String extPath,
                                                            String extDescriptorFile) throws MetaMatrixException {
        ExtensionXMLDescriptor xmlDesc = new ExtensionXMLDescriptor();
        Assertion.isNotNull(extDescriptorFile, EXTENSION_XML_FILE + " property must be specified."); //$NON-NLS-1$

        List modulesAdded = new ArrayList();
        Map extMap = xmlDesc.read(extDescriptorFile);

        if (extMap != null) {
            Iterator mit = extMap.keySet().iterator();
            while (mit.hasNext()) {
                String filename = (String)mit.next();
                final ExtensionModuleDescriptor emd = (ExtensionModuleDescriptor)extMap.get(filename);

                final String fullpath = FileUtils.buildDirectoryPath(new String[] {
                                extPath, filename
                });

                importExtensionModule(util, emd.getName(), emd.getType(), emd.getDescription(), fullpath);
                modulesAdded.add(emd.getName());
            }
        }
        
        
        return modulesAdded;

    }

    public static void importExtensionModule(ExtensionModuleInstallUtil util,
                                             String extensionname,
                                             String extensiontype,
                                             String extensiondescription,
                                             String extensionfullpath) throws MetaMatrixException {

        Assertion.isNotNull(extensiontype, "No file type was provided"); //$NON-NLS-1$
        Assertion.isNotNull(extensionname, "No source name was provided"); //$NON-NLS-1$
        Assertion.isNotNull(extensionfullpath, "No full path was provided"); //$NON-NLS-1$
        if (extensiondescription == null) {
            extensiondescription = extensionname;
        }

        util.installExtensionModule(extensionfullpath, extensiontype, PRINCIPAL, extensiondescription, extensionname);
    }

    public static void exportExtensionFile(ExtensionModuleInstallUtil util,
                                           Properties props) throws MetaMatrixException {
        String jarDir = props.getProperty(ExtensionConstants.EXTENSION_DIR_PATH);
        String jarName = props.getProperty(ExtensionConstants.EXTENSION_NAME);

        Assertion.isNotNull(jarDir, ExtensionConstants.EXTENSION_DIR_PATH + " property must be specified."); //$NON-NLS-1$
        Assertion.isNotNull(jarName, ExtensionConstants.EXTENSION_NAME + " must be specified"); //$NON-NLS-1$

        String extDescriptorFile = props.getProperty(EXTENSION_XML_FILE);
        String extType = props.getProperty(ExtensionConstants.EXTENSION_FILE_TYPE);

        
        exportExtensionFile(util, jarDir, jarName, extType, extDescriptorFile);
        
        
    }
    
    public static void exportExtensionFile(ExtensionModuleInstallUtil util,
                                                String extensionpath, // jardir
                                                String extensionname,
                                                String extensiontype, // optional
                                                String extensiondescriptorFile // optional
                                                ) throws MetaMatrixException {

        ExtensionXMLDescriptor xmlDesc = new ExtensionXMLDescriptor();

        String dir = extensionpath;
        File f = new File(extensionpath);
        if (f.getName().indexOf(".") > 0) {
            dir = f.getParent();
        }

        if ( (extensiontype != null && extensiontype.length() > 0) || (extensionname != null && extensionname.equalsIgnoreCase(ALL)) ) {
            // incase the full path with the filename was accidentally passed in, then obtain the directory

            if (extensiondescriptorFile == null || extensiondescriptorFile.length() == 0) {
                extensiondescriptorFile = DEFAULT_EXTENSION_XML_FILE;
            }

            xmlDesc.initForExport(extensiondescriptorFile);

            if (extensionname != null && extensionname.equalsIgnoreCase(ALL)) {
                // only export these types
                // configuration and vdb's should be done on their own, within the context
                // of specfically requesting those items

                List exts = util.exportExtensionModulesOfType(ExtensionModuleTypes.JAR_FILE_TYPE, dir);
                addToDescriptor(xmlDesc, exts);

                exts = util.exportExtensionModulesOfType(ExtensionModuleTypes.FUNCTION_DEFINITION_TYPE, dir);
                addToDescriptor(xmlDesc, exts);
                exts = util.exportExtensionModulesOfType(ExtensionModuleTypes.MISC_FILE_TYPE, dir);
                addToDescriptor(xmlDesc, exts);
                exts = util.exportExtensionModulesOfType(ExtensionModuleTypes.METADATA_KEYWORD_TYPE, dir);
                addToDescriptor(xmlDesc, exts);
            } else {
                List types = null;
                if (extensiontype.indexOf(",") > 0) {
                    types = StringUtil.getTokens(extensiontype, ",");
                } else {
                    types = new ArrayList(1);
                    types.add(extensiontype);
                }

                for (Iterator it = types.iterator(); it.hasNext();) {
                    String t = (String)it.next();
                    List exts = util.exportExtensionModulesOfType(t.trim(), dir);

                    addToDescriptor(xmlDesc, exts);
                }


            }

            xmlDesc.write();

        } else {
            ExtensionModuleDescriptor emd = util.exportExtensionModule(extensionpath, extensionname);
            
            if (extensiondescriptorFile != null && extensiondescriptorFile.length() > 0) {
                xmlDesc.initForExport( extensiondescriptorFile);

                if (emd != null) {
                    xmlDesc.addForExport(emd);
                    xmlDesc.write();
                }
            }

        }

    }

    private static void addToDescriptor(ExtensionXMLDescriptor xmlDesc,
                                        List extensions) {
        for (Iterator it = extensions.iterator(); it.hasNext();) {
            ExtensionModuleDescriptor emd = (ExtensionModuleDescriptor)it.next();
            xmlDesc.addForExport(emd);
        }
    }
}
