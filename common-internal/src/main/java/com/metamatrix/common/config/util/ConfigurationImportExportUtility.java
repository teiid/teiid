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

package com.metamatrix.common.config.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Properties;

import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorArchive;
import com.metamatrix.common.config.api.ConnectorBinding;

/**
* This interface can be used to import and export configuration objects to/from
* InputStream instances (usually representing a File of some sort).
*/
public interface ConfigurationImportExportUtility {

    
    /**
    * <p>This method will write to the passed in InputStream instance a 
    * complete representation of the Collection of Configuration objects that
    * are passed into it.  The failsafe way to build this
    * Collection of objects is to call the getConfigurationAndDependents() method
    * on the AdminAPI of the MetaMatrix Server.  This method will retreive the
    * Configuration and all of its dependent objects in their entirety.</p>
    *
    * <p>In order to export an entire Configuration, the Collection passed into this method
    * should have all of the following object references to be able to resolve
    * the relationships between all objects referenced by a Configuration
    * object.</p>
    *
    * <pre>
    * 1. Configuration object
    * 2. all ComponentTypes that ComponentObjects reference in the
    * Configuration object including the Configuration object's Component Type.
    * (this includes ProductTypes) 
    * 3. all ProductTypes that ProductServiceConfig objects reference in the
    * Configuration object
    * 4. all Host objects that are referenced by DeployedComponents in the
    * Configuration object
    * </pre>
    * 
    *
    * <p> All of the above object references must be in the collection passed
    * into this method.</p>
    *
    * <p> The properties object that is passed into this method may contain
    * the following properties as defined by the ConfigurationPropertyNames class.
    * These properties will define the values for the header of the output of 
    * this method.</p>
    *
    * <pre>
    * ConfigurationPropertyNames.APPLICATION_CREATED_BY
    * ConfigurationPropertyNames.APPLICATION_VERSION_CREATED_BY
    * ConfigurationPropertyNames.USER_CREATED_BY
    * <pre>
    *
    * <p>Any of these properties that are not included in the properties object
    * will not be included in the header Element.
    *
    * @param stream the output stream to write the Configuration Object
    * representation to
    * @param configurationObjects a Collection of configuration objects that
    * represents an entire logical Configuration.
    * @param props the properties object that contains the values for the Header
    * @throws IOException if there is an error writing to the InputStream
    * @throws ConfigObjectsNotResolvableException if there are references
    * to configuration objects not included in the Collection of configuration objects
    * that cannot be resolved to other configuration objects in the passed in 
    * Collection
    */
    public void exportConfiguration(OutputStream stream, Collection<?> configurationObjects, Properties props)throws IOException, ConfigObjectsNotResolvableException;
    
    
    
    /**
    * <p>This method will be used to import a Collection of Configuration objects
    * given an InputStream.  If the InputStream resource does not contain enough
    * data to recombine all of the configuration objects in the Input Stream,
    * then a ConfigurationObjectsNotResolvableException
    * will be thrown.</p>
    *
    * <p>This method also allows you to rename the imported Configuration object
    * possibly to avoid name conflicts with other Configurations already in the server.</p>
    *
    * <p>If the name parameter submitted is null, the name of the configuration
    * object as it exists in the InputStream will be used as the name
    * of the resulting Configuration object in the returned collection of
    * configuration objects.</p>
    *
    *
    * @param editor the ConfigurationObjectEditor to use to create the Configuration
    * objects in the InputStream resource.
    * @param stream the input stream to read the configuration object
    * representations from
    * @param name the name for the Configuration object to be created. Can
    * be null if the name specified in the input stream is to be used.
    * @return the configuration objects that were represented as data in the 
    * InputStream resource
    * @throws ConfigObjectsNotResolvableException if the data representing
    * the Configuration to be imported is incomplete.
    * @throws IOException if there is an error reading from the InputStream
    * @throws InvalidConfigurationElementException if there is a problem with
    * the representation of the configuration element as it exists in the 
    * InputStream resource, usually some type of formatting problem.
    */
    public Collection<?> importConfigurationObjects(InputStream stream, ConfigurationObjectEditor editor, String newName)throws IOException, ConfigObjectsNotResolvableException, InvalidConfigurationElementException;

    /**
    * <p>This method will be used to import a ComponentType Object from given a
    * InputStream instance.</p>
    * 
    * <p>This method also allows you to rename the imported ComponentType object
    * possibly to avoid name conflicts with other objects already in the server.</p>
    *
    * <p>If the name parameter submitted is null, the name of the configuration
    * object as it exists in the InputStream will be used.</p>
    *
    * @param editor the ConfigurationObjectEditor to use to create the Configuration
    * objects in the InputStream resource.
    * @param stream the input stream to read the configuration object
    * representation from
    * @return the configuration object that was represented as data in the 
    * InputStream resource
    * @param name the name for the ComponentType object to be created.
    * @throws IOException if there is an error reading from the InputStream
    * @throws InvalidConfigurationElementException if there is a problem with
    * the representation of the configuration element as it exists in the 
    * InputStream resource, usually some type of formatting problem.
    */
    public ComponentType importComponentType(InputStream stream, ConfigurationObjectEditor editor, String newName)throws IOException, InvalidConfigurationElementException;

    
    /**
     * <p>This method will be used to import 1 or more a ComponentType Objects.</p>
     *
     * @param editor the ConfigurationObjectEditor to use to create the Configuration
     * objects.
     * @param stream the input stream to read the configuration object
     * representation from
     * @return Collection of objects of type <code>ComponentType</code>
     * @throws IOException if there is an error reading from the DirectoryEntry
     * @throws InvalidConfigurationElementException if there is a problem with
     * the representation of the configuration element as it exists in the
     * DirectoryEntry resource, usually some type of formatting problem.
     */    
    public Collection<?> importComponentTypes(InputStream stream,
                                           ConfigurationObjectEditor editor)
                                           throws IOException, InvalidConfigurationElementException;
    
    /**
    * <p>This method will generally be used to create a file representation of a
    * Connector.  It will write to the InputStream
    * the representation of the ComponentType that is passed in.</p>
    *
    * <p>We have made the assumption here that the Super and Parent Component
    * types of Connector ComponentType objects will already be loaded in
    * the configuration of the server.  Thus we do not require that the Super
    * and Parent ComponentType be written to the resource. This will always be
    * the case as of the 2.0 server.</p>
    *
    * <p> The properties object that is passed into this method may contain
    * the following properties as defined by the ConfigurationPropertyNames class.</p>
    *
    * <pre>
    * ConfigurationPropertyNames.APPLICATION_CREATED_BY
    * ConfigurationPropertyNames.APPLICATION_VERSION_CREATED_BY
    * ConfigurationPropertyNames.USER_CREATED_BY
    * <pre>
    *
    * <p>Any of these properties that are not included in the properties object
    * will not be included in the header Element. 
    *
    * @param stream the output stream to write the Configuration Object
    * representation to
    * @param type the ComponentType to be written to the InputStream
    * @param props the properties object that contains the values for the Header
    * @throws IOException if there is an error writing to the InputStream
    */
    public void exportConnector(OutputStream stream, ComponentType type, Properties props) throws IOException;
 
    /**
    * <p>This method will generally be used to create a file representation of a 
    * Connector Binding.  It will write to the InputStream the representation
    * of the ServiceComponentDefn object that is passed in.</p>
    *
    * <p>Multiple ServiceComponentDefns can be written to the same InputStream
    * by passing the same InputStream instance to this method multiple times.</p>
    *
    * <p> The properties object that is passed into this method may contain
    * the following properties as defined by the ConfigurationPropertyNames class.</p>
    *
    * <pre>
    * ConfigurationPropertyNames.APPLICATION_CREATED_BY
    * ConfigurationPropertyNames.APPLICATION_VERSION_CREATED_BY
    * ConfigurationPropertyNames.USER_CREATED_BY
    * <pre>
    *
    * <p>Any of these properties that are not included in the properties object
    * will not be included in the header Element.
    *
    * @param stream the output stream to write the Configuration Object
    * representation to
    * @param type the ComponentType of the ServiceComponentDefn to be written
    * to the InputStream resource.
    * @param defn the ServiceComponentDefn instance to write to the InputStream.
    * @param props the properties object that contains the values for the Header
    * @throws IOException if there is an error writing to the InputStream
    * @throws ConfigObjectsNotResolvableException if the passed in
    * ComponentType is not the type referenced by the passed in ServiceComponentDefn.
    */
    public void exportConnectorBinding(OutputStream stream, ConnectorBinding defn, ComponentType type, Properties props) throws IOException, ConfigObjectsNotResolvableException ;

    /**
     * <p>This method will generally be used to create a file representation of 
     * one or more Connector Bindings.  It will write to the InputStream the representation
     * of the ConnectorBind object that is passed in.  The bindings and types are 
     * a match pair when resolving is done on the binding.  Therefore, these
     * should be matching order when passed in.</p>
     *
     * <p> The properties object that is passed into this method may contain
     * the following properties as defined by the ConfigurationPropertyNames class.</p>
     *
     * <pre>
     * ConfigurationPropertyNames.APPLICATION_CREATED_BY
     * ConfigurationPropertyNames.APPLICATION_VERSION_CREATED_BY
     * ConfigurationPropertyNames.USER_CREATED_BY
     * <pre>
     *
     * <p>Any of these properties that are not included in the properties object
     * will not be included in the header Element.
     *
     * @param stream the output stream to write the Configuration Object
     * representation to
     * @param bindings is an array of type <code>ConnectorBinding</code> to be written
     * to the InputStream resource.
     * @param types is an array of type <code>ComponentType</code> to be written
     * to the InputStream resource.
     * @param props the properties object that contains the values for the Header
     * @throws IOException if there is an error writing to the InputStream
     * @throws ConfigObjectsNotResolvableException if the passed in
     * ComponentType is not the type referenced by the passed in ServiceComponentDefn.
     */
    public void exportConnectorBindings(OutputStream stream, ConnectorBinding[] bindings, ComponentType[] types, Properties props) throws IOException, ConfigObjectsNotResolvableException ;
    
    /**
    * <p>This method will be used to import a group of Connector Bindings 
    * Objects given a ImputStream.  If the ImputStream resource does not contain enough
    * data to recombine a complete Connector, then a ConfigurationObjectsNotResolvableException
    * will be thrown.</p>
    *
    * <p>This method returns a collection of objects which represent one or more
    * Connector Bindings.
    *
    *
    * <p>The user of this method must either commit the Connector ComponentType of this
    * Connector Binding's  or make sure that it already exists in the server
    * configuration database before attempting to commit the
    * Connector Binding ServiceComponentDefn object.  This is because every Connector Binding ServiceComponentDefn
    * has a reference to a corresponding Connector ComponentType</p> 
    *
    * @param stream the input stream to read the configuration object
    * representation from    * 
    * @param editor the ConfigurationObjectEditor to use to create the Configuration
    * objects in the InputStream resource.
    * @return the configuration objects that are represented as data in the 
    * InputStream resource. see javadoc heading for details.
    * @throws ConfigObjectsNotResolvableException if the
    * ServiceComponentDefn does not have a reference to a ComponentType object
    * for which there is data to recombine in the InputStream resource.
    * @throws IOException if there is an error reading from the InputStream
    * @throws InvalidConfigurationElementException if there is a problem with
    * the representation of the configuration element as it exists in the 
    * InputStream resource, usually some type of formatting problem.
    */
    public Collection<ConnectorBinding> importConnectorBindings(InputStream stream, ConfigurationObjectEditor editor)throws IOException, ConfigObjectsNotResolvableException, InvalidConfigurationElementException;


    /**
    * <p>This method will be used to import a Connector Binding
    * Object given a ImputStream.  If the ImputStream resource does not contain enough
    * data to recombine a complete Connector, then a ConfigurationObjectsNotResolvableException
    * will be thrown.</p>
    *
    * <p>This method returns an object which represent one Connector Bindings.
    *
    * <p>The user of this method must either commit the Connector ComponentType of this
    * Connector Binding  or make sure that it already exists in the server
    * configuration database before attempting to commit the
    * Connector Binding ServiceComponentDefn object.  This is because every Connector Binding ServiceComponentDefn
    * has a reference to a corresponding Connector ComponentType</p> 
    *
    * @param stream the input stream to read the configuration object
    * representation from  
    * @param editor the ConfigurationObjectEditor to use to create the Configuration
    * object in the InputStream resource.
    * @return the configuration object that represent the data in the 
    * InputStream resource. see javadoc heading for details.
    * @throws ConfigObjectsNotResolvableException if the
    * ServiceComponentDefn does not have a reference to a ComponentType object
    * for which there is data to recombine in the InputStream resource.
    * @throws IOException if there is an error reading from the InputStream
    * @throws InvalidConfigurationElementException if there is a problem with
    * the representation of the configuration element as it exists in the 
    * InputStream resource, usually some type of formatting problem.
    */
	public ConnectorBinding importConnectorBinding(InputStream stream, ConfigurationObjectEditor editor, String newName)throws IOException, ConfigObjectsNotResolvableException, InvalidConfigurationElementException ;


    /**
    * <p>This method will be used to import a Connector Binding and its related Component Type at the
    * same time from the given ImputStream.  If the ImputStream resource does not contain enough
    * data to recombine a complete Connector, then a ConfigurationObjectsNotResolvableException
    * will be thrown.</p>
    * 
    * <p>This method also allows you to rename the imported Connector object
    * possibly to avoid name conflicts with other objects already in the server.</p>
    *
    * <p>If the name parameter submitted is null, the name of the confiuguration
    * object as it exists in the InputStream will be used.</p>
    *
    * <p>This method returns an array of objects which represent a
    * Connector Binding ServiceComponentDefn and its corresponding
    * Connector ComponentType.  The index of 
    * each is defined by the following static variables:</p>
    *
    * <pre>
    * ConfigurationImportExportUtility.COMPONENT_TYPE_INDEX
    * ConfigurationImportExportUtility.SERVICE_COMPONENT_DEFN_INDEX
    * </pre>
    *
    * <p>These array indices are also used to override the Connector ComponentType name
    * and Connector Binding ServiceComponentDefn name with the passed in name[] String array.
    * If either or both of these String names are null, the name of the returned
    * configuration object will be as it exists in the InputStream resource.</p>
    *
    * <p>The user of this method must either commit the Connector ComponentType of this
    * Connector Binding's ServiceComponentDefn or make sure that it already exists in the server
    * configuration database before attempting to commit the
    * Connector Binding ServiceComponentDefn object.  This is because every Connector Binding ServiceComponentDefn
    * has a reference to a corresponding Connector ComponentType</p> 
    *
    * @param editor the ConfigurationObjectEditor to use to create the Configuration
    * objects in the InputStream resource.
    * @param stream the input stream to read the configuration object
    * representation from
    * @param newNames the name for the ServiceComponentDefn and ComponentType
    * object to be created.
    * @return the configuration objects that are represented as data in the 
    * InputStream resource. see javadoc heading for details.
    * @throws ConfigObjectsNotResolvableException if the
    * ServiceComponentDefn does not have a reference to a ComponentType object
    * for which there is data to recombine in the InputStream resource.
    * @throws IOException if there is an error reading from the InputStream
    * @throws InvalidConfigurationElementException if there is a problem with
    * the representation of the configuration element as it exists in the 
    * InputStream resource, usually some type of formatting problem.
    */
    public Object[] importConnectorBindingAndType(InputStream stream, ConfigurationObjectEditor editor, String[] newName)throws IOException, ConfigObjectsNotResolvableException, InvalidConfigurationElementException;



    /**
     * Import a connector archive (connector type and extension modules) from the given
     * stream of zip file.
     * @param stream - zip file stream
     * @param editor - Configuration object to use
     * @param newName the new name for the ConnectorType.  If null, the name is derived from the file contents.
     * @return - loaded {@link com.metamatrix.common.config.api.ConnectorArchive}
     * @throws IOException
     * @throws InvalidConfigurationElementException
     * @since 4.3.2
     */
    public ConnectorArchive importConnectorArchive(InputStream stream, ConfigurationObjectEditor editor) 
        throws IOException, InvalidConfigurationElementException;
    
    
    /**
     * Export the given archive into zip format and write to the the stream provided 
     * @param stream - stream to written into
     * @param archive - archive to be exported
     * @param properties the properties object that contains the values for the Header
     * @throws IOException
     * @since 4.3.2
     */
    public void exportConnectorArchive(OutputStream stream, ConnectorArchive archive, Properties properties) 
        throws IOException, ConfigObjectsNotResolvableException;    
}


