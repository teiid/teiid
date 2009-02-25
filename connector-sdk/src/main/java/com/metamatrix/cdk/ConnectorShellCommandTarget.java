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

package com.metamatrix.cdk;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.teiid.connector.api.Connector;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.language.IQuery;

import com.metamatrix.cdk.api.ConnectorHost;
import com.metamatrix.cdk.file.ConfigReaderWriter;
import com.metamatrix.cdk.file.XMLConfigReaderWriter;
import com.metamatrix.common.config.api.ConnectorArchive;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ConnectorBindingType;
import com.metamatrix.common.config.api.ExtensionModule;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.config.model.BasicConnectorArchive;
import com.metamatrix.common.config.model.BasicExtensionModule;
import com.metamatrix.common.config.xml.XMLConfigurationImportExportUtility;
import com.metamatrix.common.util.ByteArrayHelper;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.commandshell.ConnectorResultUtility;
import com.metamatrix.core.factory.ComponentLoader;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.StringUtilities;


/**
 * Implements the commands supported by the ConnectorShell.  Delegates to a ConnectorHost to execute queries.
 */
public class ConnectorShellCommandTarget extends QueryCommandTarget {
    private static final String MM_JAR_PROTOCOL = "extensionjar"; //$NON-NLS-1$   
    private static final String CONNECTOR_CLASSPATH = "ConnectorClassPath"; //$NON-NLS-1$
    private static final String CONNECTOR_CLASS_NAME = "ConnectorClass"; //$NON-NLS-1$
    
    private IConnectorHost connectorHost;
    private Properties connectorProperties = null;
    private Connector connector;
    private String vdbFileName;
    private String connectorClassName = null;
    private URLClassLoader connectorClassLoader = null;
    public ConnectorShellCommandTarget() {
    }
    
    public ConnectorShellCommandTarget(IConnectorHost connectorHost) {
        this.connectorHost = connectorHost;
    }
    
    protected String execute(String query) {
        try {
            ICommand command = getConnectorHost().getCommand(query);
            String[] columnNames = null;
            if(command instanceof IQuery) {
                IQuery iquery = (IQuery) command;
                columnNames = iquery.getColumnNames();
            } else if (!(command instanceof IProcedure)){
                columnNames = new String[] {"count"}; //$NON-NLS-1$
            }
            List results = getConnectorHost().executeCommand(command);
            return ConnectorResultUtility.resultsToString(results, columnNames);
        } catch (ConnectorException e) {
            throw new MetaMatrixRuntimeException(e);
        }
    }

    protected Set getMethodsToIgnore() {
        Set result = new HashSet();
        result.addAll(Arrays.asList( new String[] { "runRep", "setUsePreparedStatement"} )); //$NON-NLS-1$ //$NON-NLS-2$
        return result;
    }

    /**
     * Executes the provided script and expects the script to produce a ConnectorHost for executing queries.
     * @param configurationScriptFileName Resource name of the script file to be loaded from the class path.
     */
    public void loadFromScript(String configurationScriptFileName) {
        ComponentLoader loader = new ComponentLoader(this.getClass().getClassLoader(), configurationScriptFileName);       
        connectorHost = (IConnectorHost) loader.load("ConnectorHost"); //$NON-NLS-1$
    }
    
    public void load(String connectorClassName, String vdbFileName) throws IllegalAccessException,
        InstantiationException, ClassNotFoundException {
        if (connectorClassLoader == null) {
            Class.forName(connectorClassName);	// Just make sure we load the class
            this.connectorProperties = new Properties();
        }
        else {
            connectorClassLoader.loadClass(connectorClassName).newInstance();
        }        
        this.vdbFileName = vdbFileName;
        this.connectorClassName = connectorClassName; 
        connectorHost = null;
    }
    

    public void start() throws IllegalAccessException,
    	InstantiationException, ClassNotFoundException {
        if (connectorHost == null) {
            if (vdbFileName == null) {
                throw new RuntimeException(CdkPlugin.Util.getString("ConnectorShellCommandTarget.Connector_must_be_loaded_before_it_can_be_used._1")); //$NON-NLS-1$
            }
            if (connectorClassLoader == null) {
                connector = (Connector) Class.forName(connectorClassName).newInstance();
            }
            else {
                connector = (Connector) connectorClassLoader.loadClass(connectorClassName).newInstance();
            }
            connectorHost = new ConnectorHost(connector, connectorProperties, shell.expandFileName(vdbFileName));     
        }
    }
    
    public void stop() {
        if (connector != null) {
            connector.stop();
            connector = null;
        } 
        connectorHost = null;
    }
    
    public void loadProperties(String propertyFileName) {
        File propertyFile = new File(propertyFileName);
        if(!propertyFile.exists()
        || !propertyFile.isFile()
        || !propertyFile.canRead()) {
            throw new RuntimeException(CdkPlugin.Util.getString("ConnectorShellCommandTarget.Cannot_read_from_file__{0}_1", propertyFile)); //$NON-NLS-1$
        }
        try {
            connectorProperties = loadFromXMLConfig(propertyFile);
            return;
        } catch (Exception e) {
            // Assume this is a properties file, not an XML file.
        }
        connectorProperties = loadFromPropertiesFile(propertyFile);
    }
    
    private Properties loadFromXMLConfig(File propertyFile) {
        ConfigReaderWriter xmlConfig = new XMLConfigReaderWriter();
        InputStream in = null;
        try {
            in = new FileInputStream(propertyFile);
        } catch (IOException e) {
            throw new RuntimeException(CdkPlugin.Util.getString("ConnectorShellCommandTarget.Cannot_read_from_file__{0}_1", propertyFile)); //$NON-NLS-1$
        }
        
        try {
            ConnectorBinding binding = (ConnectorBinding) xmlConfig.loadConnectorBinding(in)[1];
            return binding.getProperties();
        } catch (Exception e) {
            throw new RuntimeException(CdkPlugin.Util.getString("ConnectorShellCommandTarget.Could_not_load_XML_configuration_from_file__{0}_1", propertyFile)); //$NON-NLS-1$
        } finally {
            try {
                in.close();
            } catch (Exception e) {
            }
        }
    }
    
    private Properties loadFromPropertiesFile(File propertyFile) {
        Properties props = new Properties();
        InputStream in = null;
        try {
            in = new FileInputStream(propertyFile);
            props.load(in);
            return props;
        } catch (IOException e) {
            throw new RuntimeException(CdkPlugin.Util.getString("ConnectorShellCommandTarget.Cannot_read_from_file__{0}_1", propertyFile)); //$NON-NLS-1$
        } finally {
            try {
                in.close();
            } catch (Exception e) {
            }
        }
    }
    
    public void setFailOnError(boolean failOnError) {
        if (failOnError) {
            shell.turnOffExceptionHandling();
        } else {
            shell.turnOnExceptionHandling();
        }        
    }
    
    public void setPrintStackOnError(boolean printStackOnError) {
        shell.setPrintStackTraceOnException(printStackOnError);
    }
    
    public void setSecurityContext(String vdbName, String vdbVersion, String userName) {
        getConnectorHost().setSecurityContext(vdbName, vdbVersion, userName, null, null);
    }
    
    public void setBatchSize(int batchSize) {
    }

    public void setProperty(String propertyName, String propertyValue) {
        if (connectorHost == null) {
            if (connectorProperties == null) {
                connectorProperties = new Properties();
            }
            connectorProperties.put(propertyName, propertyValue);
        } else {
            throw new RuntimeException(CdkPlugin.Util.getString("ConnectorShellCommandTarget.Cannot_set_connector_properties_after_the_connector_is_started._1")); //$NON-NLS-1$
        }
    }
    
    private IConnectorHost getConnectorHost() {
        return connectorHost;
    }
    
    public String getProperties() {
        StringBuffer props = new StringBuffer();
        IConnectorHost host = getConnectorHost();
        if (host != null) {
            Properties properties = host.getConnectorEnvironmentProperties();
            stringifyProperties(properties, props);
        } else if (connectorProperties != null) {
            stringifyProperties(connectorProperties, props);
        }
        return props.toString();
    }
    
    public void createTemplate(String filename) {
        File file = new File(filename);
        if (file.exists()) {
            if (!file.canWrite()) {
                throw new RuntimeException(CdkPlugin.Util.getString("ConnectorShellCommandTarget.Cannot_write_to_file__{0}_3", file)); //$NON-NLS-1$
            }
        } else if (file.getParentFile() == null || file.getParentFile().exists()) {
            try {
                if (!file.createNewFile()) {
                    throw new RuntimeException(CdkPlugin.Util.getString("ConnectorShellCommandTarget.Cannot_create_file__{0}_4", file)); //$NON-NLS-1$
                }
            } catch (IOException e) {
                throw new RuntimeException(CdkPlugin.Util.getString("ConnectorShellCommandTarget.Cannot_create_file__{0}_4", file)); //$NON-NLS-1$
            }
        } else {
            throw new RuntimeException(CdkPlugin.Util.getString("ConnectorShellCommandTarget.Cannot_create_file_{0}_because_directory_{1}_does_not_exist._6", file, file.getParentFile())); //$NON-NLS-1$
        }
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(CdkPlugin.Util.getString("ConnectorShellCommandTarget.Cannot_write_to_file__{0}_3", file)); //$NON-NLS-1$
        }
        
        InputStream template = getClass().getResourceAsStream("Template.cdk"); //$NON-NLS-1$
        int readByte = -1;
        try {
            while((readByte = template.read()) != -1) {
                out.write(readByte);
            }
        } catch (IOException e) {
            throw new RuntimeException(CdkPlugin.Util.getString("ConnectorShellCommandTarget.Cannot_write_to_file__{0}_3", file)); //$NON-NLS-1$
        } finally {
            try {
                template.close();
            } catch (Exception e) {
            }
            try {
                out.close();
            } catch (Exception e) {
            }
        }
    }
    
    /**
     * Build the connector archive file. ( This is duplicate code from the 
     * ConnectorArchiveImportExport class, due to dependency reasons I had to
     * write)
     *  
     * @param filename
     * @param cdkName
     * @param extDirName
     * @since 4.3.2
     */
    public void createArchive(String fileName, String cdkName, String extDirName) {
        
        String lcaseCdkName = cdkName.toLowerCase();
        
        File out = new File(fileName);
        if (out.exists()) {
            throw new RuntimeException(CdkPlugin.Util.getString("ConnectorShellCommandTarget.archive_file_exists", fileName)); //$NON-NLS-1$            
        }
        
        File cdk = new File(cdkName);
        if (!cdk.exists() || !lcaseCdkName.endsWith(".cdk")) { //$NON-NLS-1$
            throw new RuntimeException(CdkPlugin.Util.getString("ConnectorShellCommandTarget.bad_cdk_file", cdkName)); //$NON-NLS-1$
        }
        
        File extDir = new File(extDirName);
        if (!extDir.exists() || !extDir.isDirectory()) {
            throw new RuntimeException(CdkPlugin.Util.getString("ConnectorShellCommandTarget.bad_extdir", extDirName)); //$NON-NLS-1$
        }
        
        File[] extModules = extDir.listFiles();        
        if (extModules.length == 0) {
            throw new RuntimeException(CdkPlugin.Util.getString("ConnectorShellCommandTarget.no_ext_modules", extDirName)); //$NON-NLS-1$            
        }
                
        try {
            BasicConnectorArchive archive = new BasicConnectorArchive();
            XMLConfigurationImportExportUtility util = new XMLConfigurationImportExportUtility();
            List types = new ArrayList(util.importComponentTypes(new FileInputStream(cdk), new BasicConfigurationObjectEditor()));
            ConnectorBindingType type = (ConnectorBindingType)types.get(0);
            archive.addConnectorType(type);
            
            // Look at the connector type and find all the needextension modules
            List neededExtensionModules = getExtensionJarNames(type);            
            for (final Iterator i = neededExtensionModules.iterator(); i.hasNext();) {
                final String extName = (String)i.next();
                File extModule = new File(extDirName, extName);
                BasicExtensionModule ext = new BasicExtensionModule(extModule.getName(), ExtensionModule.JAR_FILE_TYPE, "JAR File", ByteArrayHelper.toByteArray(extModule)); //$NON-NLS-1$
                archive.addExtensionModule(type, ext);                
            }
            
            // write the archive file
            FileOutputStream fout = new FileOutputStream(out);
            util.exportConnectorArchive(fout, archive, null);
            fout.close();
            
        } catch (Exception e) {
            throw new RuntimeException(CdkPlugin.Util.getString("ConnectorShellCommandTarget.failed_create_archive", e.getMessage())); //$NON-NLS-1$
        }
    }
        
    /**
     * Load the archive file, so that the connector can be started 
     * @param fileName
     * @param typeName Name to give the new connector type.
     * @since 4.3.2
     */
    public void loadArchive(String fileName, String typeName) {
        XMLConfigurationImportExportUtility util = new XMLConfigurationImportExportUtility();
        
        File archiveFile = new File(fileName);
        if(!archiveFile.exists()) {
            throw new RuntimeException(CdkPlugin.Util.getString("ConnectorShellCommandTarget.bad_archive_file", fileName)); //$NON-NLS-1$
        }
        
        try {
            FileInputStream in = new FileInputStream(archiveFile);
            ConnectorArchive archive = util.importConnectorArchive(in, new BasicConfigurationObjectEditor());
            in.close();
            
            ConnectorBindingType type = archive.getConnectorTypes()[0];
            ExtensionModule[] extModules = archive.getExtensionModules(type);

            // write the needed extension jar files to disk
            List extModuleUrls = new ArrayList();
            List neededExtensionModules = getExtensionJarNames(type);
            for (final Iterator i = neededExtensionModules.iterator(); i.hasNext();) {
                final String extName = (String)i.next();                
                for (int j = 0; j < extModules.length; j++) {
                    if (extModules[j].getFullName().equalsIgnoreCase(extName)) {
                        File targetFile = new File("extensions/"+extModules[j].getFullName()); //$NON-NLS-1$
                        FileUtils.write(extModules[j].getFileContents(), targetFile.getCanonicalFile()); 
                        extModuleUrls.add(targetFile.toURL());
                    }
                }                
            } // for
            
            connectorClassName = type.getDefaultValue(CONNECTOR_CLASS_NAME);
            URL[] urls = (URL[])extModuleUrls.toArray(new URL[extModuleUrls.size()]);
            connectorClassLoader = new URLClassLoader(urls, this.getClass().getClassLoader());
            this.connectorProperties = new Properties();
            
            // Now set the rest of the properties
            Properties props = type.getDefaultPropertyValues();
            Iterator keys = props.keySet().iterator();
            while(keys.hasNext()) {
                String key = (String)keys.next();
                if (!key.equals(CONNECTOR_CLASS_NAME) && !key.equals(CONNECTOR_CLASSPATH)) {
                    String value = type.getDefaultValue(key);
                    if (value != null) {
                        setProperty(key, type.getDefaultValue(key));
                    }
                }
            }
                
        } catch (Exception e) {
            throw new RuntimeException(CdkPlugin.Util.getString("ConnectorShellCommandTarget.failed_load_archive", e.getMessage())); //$NON-NLS-1$            
        }
    }
    
    /**
     * Gets the extension modules needed by this connector type 
     * @param type
     * @return
     * @since 4.3
     */
    private List getExtensionJarNames(ConnectorBindingType type) {
        List modules = new ArrayList();
        String classPath = type.getDefaultValue(CONNECTOR_CLASSPATH); 
        StringTokenizer st = new StringTokenizer(classPath, ";"); //$NON-NLS-1$
        while (st.hasMoreTokens()) {
            String path = st.nextToken();
            int idx = path.indexOf(MM_JAR_PROTOCOL);
            if (idx != -1) {
                String jarFile = path.substring(idx + MM_JAR_PROTOCOL.length() + 1);
                modules.add(jarFile);
            }                                        
        }
        return modules;
    }
    
    private void stringifyProperties(Properties props, StringBuffer buf) {
        // Properties.list() does not list the complete value of a property,
        // so we iterate through the property names and add them to the result.
        
        // Sort the names in a natural ordering
        SortedSet sortedNames = new TreeSet(props.keySet());
        String propertyName = null;
        for(Iterator i = sortedNames.iterator(); i.hasNext();) {
            propertyName = (String)i.next();
            buf.append(propertyName)
               .append("=") //$NON-NLS-1$
               .append(props.getProperty(propertyName))
               .append(StringUtilities.LINE_SEPARATOR);
        }
    }
}
