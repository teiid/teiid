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

package org.teiid.adminapi;

import java.util.Properties;

import com.metamatrix.admin.RolesAllowed;


/**
 * This interface describes the methods to configure MetaMatrix.
 *
 * <p>As a <i>core</i> interface,
 * this administration is common to both the MetaMatrix server and MM Query.</p>
 * @since 4.3
 */
@RolesAllowed(value=AdminRoles.RoleName.ADMIN_SYSTEM)
public interface ConfigurationAdmin {

    /**
     * Set system-wide property.  This will be written to config_ns.xml
     *
     * @param propertyName
     *            Name of the System Property
     * @param propertyValue
     *            Value of the System Property
     * @throws AdminException
     *             if there's a system error or if there's a user input error.
     * @since 4.3
     */
    void setSystemProperty(String propertyName,
                           String propertyValue) throws AdminException;
    
    
    /**
     * Set several system-wide properties.  These will be written to config_ns.xml
     * Any existing properties not specified will not be changed.
     *
     * @param properties
     *            Properties to set.
     * @throws AdminException
     *             if there's a system error or if there's a user input error.
     * @since 4.3
     */
    void updateSystemProperties(Properties properties) throws AdminException;
    

    /**
     * Assign a {@link ConnectorBinding} to a {@link VDB}'s Model
     *
     * @param connectorBindingName
     *            Name of the ConnectorBinding
     * @param vdbName
     *            Name of the VDB
     * @param vdbVersion
     *            Version of the VDB
     * @param modelName
     *            Name of the Model to map Connector Binding
     * @throws AdminException
     *             if there's a system error or if there's a user input error.
     * @since 4.3
     */
    void assignBindingToModel(String connectorBindingName,
                              String vdbName,
                              String vdbVersion,
                              String modelName) throws AdminException;
    
    void assignBindingsToModel(String[] connectorBindingName,
            String vdbName,
            String vdbVersion,
            String modelName) throws AdminException;    

    /**
     * Set a Property for an AdminObject
     * 
     * @param identifier
     *            The unique identifier for for an {@link AdminObject}.
     * @param className
     *            The class name of the sub-interface of {@link AdminObject} you are setting the property for.
     *            All of these sub-interfaces are in package <code>com.metamatrix.admin.api.objects</code>.
     *            You may specify either the fully-qualified or unqualified classname. 
     *            For example "ConnectorBinding" or "com.metamatrix.admin.api.objects.ConnectorBinding".
     * @param propertyName
     *            String Property key
     * @param propertyValue
     *            String value to update
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    void setProperty(String identifier,
                     String className,
                     String propertyName,
                     String propertyValue) throws AdminException;
    
    
    /**
     * Set several properties for an AdminObject. Any existing properties not specified will not be changed.
     * 
     * @param identifier
     *            The unique identifier for for an {@link AdminObject}.
     * @param className
     *            The class name of the sub-interface of {@link AdminObject} you are setting the property for.
     *            All of these sub-interfaces are in package <code>com.metamatrix.admin.api.objects</code>.
     *            You may specify either the fully-qualified or unqualified classname. 
     *            For example "ConnectorBinding" or "com.metamatrix.admin.api.objects.ConnectorBinding".
     * @param properties
     *            Properties to set.
     * @throws AdminException
     *             if there's a system error or if there's a user input error.
     * @since 4.3
     */
    void updateProperties(String identifier,
                          String className,
                          Properties properties) throws AdminException;
    

    /**
     * Add Connector Type, will import Connector Type from a file
     *
     * @param name
     *            of the Connector Type to add
     * @param cdkFile
     *            contents of File from Client
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    void addConnectorType(String name, char[] cdkFile) throws AdminException;

    /**
     * Add Connector Type and all the required extension modules required by the
     * this connector type into the system from the given file byte stream which is
     * encoded inthe Connector Archive format.
     *
     * @param archiveContents contents of File 
     * @param options resolution option in case of conflict in the connector type 
     * @throws AdminException if there's a system error.
     * @since 4.3.2
     */
    void addConnectorArchive(byte[] archiveContents, AdminOptions options ) throws AdminException;
    
    /**
     * Delete Connector Type from Next Configuration
     *
     * @param name String name of the Connector Type to delete
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    void deleteConnectorType(String name) throws AdminException;

    /**
     * Deploy a {@link ConnectorBinding} to Configuration
     *
     * @param name
     *            is the Connector Binding name that will be added to Configuration
     * @param connectorTypeIdentifier
     *            Name of the Connector Type
     * @param properties
     *            Name & Value pair need to deploy the Connector Binding
     * @param options The perferred options when executing this method. There are choices about
     * what to do when a connector binding with the given identifier already exists in the system.
     * See the interface {@link AdminOptions.OnConflict} for details.
     * <p>
     * Another option is to ignore a binding connection password decrypt error, when adding a connector
     * binding whose password was encrypted with a different keystore, so that the new password property
     * can be set after the connector binding has been added.</p>
     * @throws AdminException
     *             if there's a system error.
     * @return the {@link ConnectorBinding} representing the current property values and runtime state.
     * Note that if this is a system with multiple Processes, this method may actually create multiple deployed 
     * Connector Bindings (one for each process).  This method will return one of them, arbitrarily.
     * @since 4.3
     */
    ConnectorBinding addConnectorBinding(String name,
                             String connectorTypeIdentifier,
                             Properties properties, AdminOptions options) throws AdminException;

    /**
     * Import a {@link ConnectorBinding} into the Configuration.
     *
     * @param name
     *            is the Connector Binding name that will be added to Configuration
     * @param xmlFile
     *            contents of XML file that will be sent to the server.
     * @param options The perferred options when executing this method. There are choices about
     * what to do when a connector binding with the given identifier already exists in the system.
     * See the interface {@link AdminOptions.OnConflict} for details.
     * <p>
     * Another option is to ignore a binding connection password decrypt error, when adding a connector
     * binding whose password was encrypted with a different keystore, so that the new password property
     * can be set after the connector binding has been added.</p>
     * @throws AdminException
     *             if there's a system error.
     * @return the {@link ConnectorBinding} representing the current property values and runtime state.
     * Note that if this is a system with multiple Processes, this method may actually create multiple deployed 
     * Connector Bindings (one for each process).  This method will return one of them, arbitrarily.
     * @since 4.3
     */
    ConnectorBinding addConnectorBinding(String name,
                             char[] xmlFile, AdminOptions options) throws AdminException;

    /**
     * Delete the {@link ConnectorBinding} from the Configuration
     *
     * @param connectorBindingIdentifier
     * @throws AdminException
     *             if there's a system error.
      * @since 4.3
     */
    void deleteConnectorBinding(String connectorBindingIdentifier) throws AdminException;

    /**
     * Import a {@link VDB} file.
     * <br>A VDB file with internal definitions. Thise is the default VDB export configuration
     * begining with MetaMatrix version 4.3.</br>
     *
     * @param name
     *            VDB Name
     * @param vdbFile
     *            byte array of the VDB Archive
     * @param options The perferred options when executing this method. There are choices about
     * what to do when a connector binding with the given identifier already exists in the system.
     * @throws AdminException
     *             if there's a system error.
     * @return the {@link VDB} representing the current property values and runtime state.
     * @since 4.3
     */
    VDB addVDB(String name,
                byte[] vdbFile, AdminOptions options) throws AdminException;

    /**
     * Get the {@link LogConfiguration}
     *
     * @return LogConfiguration object
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    @RolesAllowed(value=AdminRoles.RoleName.ADMIN_READONLY)
    LogConfiguration getLogConfiguration() throws AdminException;

    /**
     * Set the {@link LogConfiguration} in the MetaMatrix Server
     *
     * @param config
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    void setLogConfiguration(LogConfiguration config) throws AdminException;

    /**
     * Adds an {@link ExtensionModule} to the end of the list of modules.
     * <br><i>All caches (of Class objects) are cleared.</i></br>
     *
     * @param type
     *            one of the known types of extension file
     * @param sourceName
     *            name (e.g. filename) of extension module
     * @param source
     *            actual contents of module
     * @param description
     *            (optional) description of the extension module - may be null
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    void addExtensionModule(String type,
                            String sourceName,
                            byte[] source,
                            String description) throws AdminException;

    /**
     * Deletes an {@link ExtensionModule} from the list of modules.
     * <br><i>All caches (of Class objects) are cleared.</i></br>
     *
     * @param sourceName
     *            name (e.g. filename) of extension module
     * @throws AdminException
     *             if there's a system error.
     */
    void deleteExtensionModule(String sourceName) throws AdminException;

    /**
     * Export an {@link ExtensionModule} to byte array
     *
     * @param sourceName unique identifier for the {@link ExtensionModule}.
     * @return byte array of the extension module
     * @throws AdminException
     * @since 4.3
     */
    byte[] exportExtensionModule(String sourceName) throws AdminException;

    /**
     * Export Configuration to character Array in XML format
     *
     * @return character array of Configuration
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    char[] exportConfiguration() throws AdminException;

    /**
     * Export a {@link ConnectorBinding} to character Array in XML format
     *
     * @param connectorBindingIdentifier the unique identifier for a {@link ConnectorBinding}.
     * @return character Array in XML format
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    char[] exportConnectorBinding(String connectorBindingIdentifier) throws AdminException;

    /**
     * Export Connector Type to character array
     *
     * @param connectorTypeIdentifier the unique identifier for for a {@link ConnectorType}
     * @return character Array in XML format
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    char[] exportConnectorType(String connectorTypeIdentifier) throws AdminException;

    /**
     * Export Connector Archive, which is bundled connector type with its xml
     * properties file and all the extension modules required by the this connector type
     * 
     * @param connectorTypeIdentifier the unique identifier for for a {@link ConnectorType}
     * @return byte array of the connector archive.
     * @throws AdminException if there's a system error.
     * @since 4.3.2
     */
    byte[] exportConnectorArchive(String connectorTypeIdentifier) throws AdminException;
    
    /**
     * Export VDB to byte array
     *
     * @param name identifier of the {@link VDB}
     * @param version {@link VDB} version
     * @return byte array of the MetaMatrix VDB Archive
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    byte[] exportVDB(String name, String version) throws AdminException;


    /**
     * Add User Defined Function model to the system. If one is already deployed before this 
     * will replace the previous, otherwise add this as the new UDF model. Once the UDF is added
     * the new UDF model is loaded.  
     * @param modelFileContents - UDF contents
     * @param classpath - classpath for the UDF
     * @throws AdminException
     */
    void addUDF(byte[] modelFileContents, String classpath) throws AdminException;
    
    /**
     * Delete the User Defined Function model. Note that this will not delete any supporting
     * extension jar files added, those need to be deleted separately.
     * @throws AdminException  
     */
    void deleteUDF() throws AdminException;
    
    /**
     * Indicates that an extension module has changed 
     * @throws AdminException
     * @since 6.1.0
     */
    void extensionModuleModified(String name) throws AdminException;    
}
