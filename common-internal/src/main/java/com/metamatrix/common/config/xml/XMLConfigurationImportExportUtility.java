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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Properties;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorArchive;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.util.ConfigObjectsNotResolvableException;
import com.metamatrix.common.config.util.ConfigurationImportExportUtility;
import com.metamatrix.common.config.util.InvalidConfigurationElementException;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.common.xml.XMLReaderWriter;
import com.metamatrix.common.xml.XMLReaderWriterImpl;
import com.metamatrix.core.util.Assertion;

/**
* This implementation is used to import/export configuration objects to/from
* XML files.  The structure of the XML file(s) that can be generated/read in
* is defined in the XMLElementNames class.
*
*
* **************************************************************************************
*         * * * * * * *      W A R N I N G     * * * * * * *
* **************************************************************************************
*
*   The importer process cannot have any calls to I18NLogManager or LogManager because the
*  			bootstrapping of CurrentConfiguration
*           uses this class and the CurrentConfiguration has to come up before
*           logging is available.
*
*/
public class XMLConfigurationImportExportUtility implements ConfigurationImportExportUtility{    
    private XMLReaderWriter readerWriter;


    /**
    * <p>This method will write to the passed in DirectoryEntry instance a
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
    * @throws IOException if there is an error writing to the DirectoryEntry
    * @throws ConfigObjectsNotResolvableException if there are references
    * to configuration objects not included in the Collection of configuration objects
    * that cannot be resolved to other configuration objects in the passed in
    * Collection
    */
    public void exportConfiguration(OutputStream stream,
                     Collection configurationObjects, Properties props)
                     throws IOException, ConfigObjectsNotResolvableException {
        ConfigurationImportExport util = new ConfigurationImportExport();
        util.exportConfiguration(stream, configurationObjects, props);
    }


    /**
    * <p>This method will generally be used to create a file representation of a
    * ConnectorType that is defined in the CDK.  It will write to the outputstream
    * the representation of the ComponentType that is passed in.</p>
    *
    * <p>We have made the assumption here that the Super and Parent Component
    * types of all exportable ComponentType objects will already be loaded in
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
    * @param type the ComponentType to be written to the DirectoryEntry
    * @param props the properties object that contains the values for the Header
    * @throws IOException if there is an error writing to the DirectoryEntry
    */
    public void exportComponentType(OutputStream stream, ComponentType type,
                     Properties props) throws IOException {

        
        ConfigurationImportExport util = new ConfigurationImportExport();
        util.exportComponentType(stream, type, props);
    }
    

    /**
    * <p>This method will generally be used to create a file representation 
    * containing one or more connector types.  It will write to the outputstream
    * the representation of the ComponentType that is passed in.</p>
    *
    * <p>We have made the assumption here that the Super and Parent Component
    * types of all exportable ComponentType objects will already be loaded in
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
    * @param type the ComponentType to be written to the DirectoryEntry
    * @param props the properties object that contains the values for the Header
    * @throws IOException if there is an error writing to the DirectoryEntry
    */
    public void exportComponentTypes(OutputStream stream, ComponentType[] types,
                     Properties props) throws IOException {

        
        ConfigurationImportExport util = new ConfigurationImportExport();
        util.exportComponentTypes(stream, types, props);
    }    


    /**
    * <p>This method will generally be used to create a file representation of a
    * Connector Binding.  It will write to the DirectoryEntry the representation
    * of the ServiceComponentDefn object that is passed in.</p>
    *
    * <p>Multiple ServiceComponentDefns can be written to the same DirectoryEntry
    * by passing the same DirectoryEntry instance to this method multiple times.</p>
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
    * to the DirectoryEntry resource.
    * @param defn the ServiceComponentDefn instance to write to the DirectoryEntry.
    * @param props the properties object that contains the values for the Header
    * @throws IOException if there is an error writing to the DirectoryEntry
    * @throws ConfigObjectsNotResolvableException if the passed in
    * ComponentType is not the type referenced by the passed in ServiceComponentDefn.
    */
//    public void exportServiceComponentDefn(OutputStream stream,
//                 ServiceComponentDefn defn, ComponentType type, Properties props)
//                 throws IOException, ConfigObjectsNotResolvableException {
//        Assertion.isNotNull(defn);
//        Assertion.isNotNull(type);
//        Assertion.isNotNull(stream);
//
////        LogManager.logDetail(LogCommonConstants.CTX_CONFIG, "Exporting a ServiceDefinition: " + defn.getName() + ".");
//
//        List configurationObjects = new ArrayList(2);
//
//        configurationObjects.add(defn);
//        configurationObjects.add(type);
//
//        // here we need to make sure that the serviceComponentDefn references
//        // the passed in ComponentType instance.  If not, this will throw
//        // the ConfigObjectsNotResolvableException
//        resolveConfigurationObjects(configurationObjects);
////        LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Configuration objects to export resolved properly.");
//
//        XMLHelper helper = getXMLHelper();
//
//        Element root = helper.createRootConfigurationDocumentElement();
//
//        // create a new Document with a root element
//        Document doc = new Document(root);
//
//        // add the header element
//        root.addContent(helper.createHeaderElement(createHeaderProperties(props)));
//
//        Element componentTypesElement = helper.createComponentTypesElement();
//        root.addContent(componentTypesElement);
//        componentTypesElement.addContent(helper.createComponentTypeElement(type));
//
//        Element serviceComponentDefnsElement = helper.createServiceComponentDefnsElement();
//        root.addContent(serviceComponentDefnsElement);
//        serviceComponentDefnsElement.addContent(helper.createServiceComponentDefnElement(defn));
//
//        getXMLReaderWriter().writeDocument(doc, stream);
////        I18nLogManager.logInfo(LogCommonConstants.CTX_CONFIG, LogMessageKeys.CONFIG_MSG_0005, defn.getName());
//    }
//
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
    public Collection importConfigurationObjects(InputStream stream,
                     ConfigurationObjectEditor editor, String name)
                     throws IOException, ConfigObjectsNotResolvableException,
                     InvalidConfigurationElementException {
        Assertion.isNotNull(stream);
        Assertion.isNotNull(editor);

//***        LogManager.logDetail(LogCommonConstants.CTX_CONFIG, "Importing a Configuration...");

        Document doc = null;

        try {
            doc = getXMLReaderWriter().readDocument(stream);
        }catch(JDOMException e) {
			e.printStackTrace();
            throw new IOException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0002));
        }


        Element root = doc.getRootElement();

             ConfigurationImportExport util = new ConfigurationImportExport();
            return util.importConfigurationObjects(root, editor, name);

    }
    
    
    protected XMLReaderWriter getXMLReaderWriter() {
        if (readerWriter == null) {
            readerWriter = new XMLReaderWriterImpl();
        }
        return readerWriter;
    }


    /**
    * <p>This method will be used to import a ComponentType Object</p>
    *
    * <p>This method also allows you to rename the imported ComponentType object
    * possibly to avoid name conflicts with other objects already in the server.</p>
    *
    * <p>If the name parameter submitted is null, the name of the configuration
    * object as it exists in the DirectoryEntry will be used.</p>
    *
    * @param editor the ConfigurationObjectEditor to use to create the Configuration
    * objects in the DirectoryEntry resource.
    * @param stream the input stream to read the configuration object
    * representation from
    * @return the configuration object that was represented as data in the
    * DirectoryEntry resource
    * @param name the name for the ComponentType object to be created.
    * @throws IOException if there is an error reading from the DirectoryEntry
    * @throws InvalidConfigurationElementException if there is a problem with
    * the representation of the configuration element as it exists in the
    * DirectoryEntry resource, usually some type of formatting problem.
    */
    public ComponentType importComponentType(InputStream stream,
                 ConfigurationObjectEditor editor, String name)
                 throws IOException, InvalidConfigurationElementException {

        Document doc = null;

        try {
            doc = getXMLReaderWriter().readDocument(stream);
        }catch(JDOMException e) {
            e.printStackTrace();
            throw new IOException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0002));
        }


        Element root = doc.getRootElement();

//        boolean is42Compatible = XMLHelperUtil.is42ConfigurationCompatible(root); 
        
        //vah 7/22/04 the format of the configuration changed
        // in version 4.2, any version prior to this will use
        // the 3.0 version utility
            ConfigurationImportExport util = new ConfigurationImportExport();
            return util.importComponentType(root, editor, name);

        
        
//        ConfigurationImportExport util = new ConfigurationImportExport();
//        return util.importComponentType(stream, editor, name);
    }
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

    public Collection importComponentTypes(InputStream stream,
                 ConfigurationObjectEditor editor)
                 throws IOException, InvalidConfigurationElementException {

        Document doc = null;

        try {
            doc = getXMLReaderWriter().readDocument(stream);
        }catch(JDOMException e) {
            e.printStackTrace();
            throw new IOException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0002));
        }


        Element root = doc.getRootElement();
        
        return importComponentTypes(root, editor);

    }

    /**
     * <p>This method will be used to import 1 or more a ComponentType Objects.</p>
     * The ComponentTypes element must be a child of the root element passed in.
     *
     * @param editor the ConfigurationObjectEditor to use to create the Configuration
     * objects.
     * @param root Element contains the ComponentTypes to import
     * @return Collection of objects of type <code>ComponentType</code>
     * @throws IOException if there is an error reading from the DirectoryEntry
     * @throws InvalidConfigurationElementException if there is a problem with
     * the representation of the configuration element as it exists in the
     * DirectoryEntry resource, usually some type of formatting problem.
     */

     public Collection importComponentTypes(Element root,
                  ConfigurationObjectEditor editor)
                  throws IOException, InvalidConfigurationElementException {

         ConfigurationImportExport util = new ConfigurationImportExport();
         return util.importComponentTypes(root, editor);        
         
     }

    
    /**
    * <p>This method will be used to import a ServiceComponentDefn Object given a Directory
    * entry instance.  If the DirectoryEntry resource does not contain enough
    * data to recombine a complete ServiceComponentDefn, then a ConfigurationObjectsNotResolvableException
    * will be thrown.</p>
    *
    * <p>This method also allows you to rename the imported ServiceComponentDefn object
    * possibly to avoid name conflicts with other objects already in the server.</p>
    *
    * <p>If the name parameter submitted is null, the name of the confiuguration
    * object as it exists in the DirectoryEntry will be used.</p>
    *
    * <p>This method returns an array of objects which represent a
    * ServiceComponentDefn and its corresponding ComponentType.  The index of
    * each is defined by the following static variables:</p>
    *
    * <pre>
    * ConfigurationImportExport.COMPONENT_TYPE_INDEX
    * ConfigurationImportExport.SERVICE_COMPONENT_DEFN_INDEX
    * </pre>
    *
    * <p>These array indices are also used to override the ComponentType name
    * and ServiceComponentDefn name with the passed in name[] String array.
    * If either or both of these String names are null, the name of the returned
    * configuration object will be as it exists in the DirectoryEntry resource.</p>
    *
    * <p>The user of this method must either commit the ComponentType of this
    * ServiceComponentDefn or make sure that it already exists in the server
    * configuration database before attempting to commit the
    * ServiceComponentDefn object.  This is because every ServiceComponentDefn
    * has a reference to a corresponding ComponentType</p>
    *
    * @param editor the ConfigurationObjectEditor to use to create the Configuration
    * objects in the DirectoryEntry resource.
    * @param stream the input stream to read the configuration object
    * representation from
    * @param name the name for the ServiceComponentDefn and ComponentType
    * object to be created.
    * @return the configuration objects that are represented as data in the
    * DirectoryEntry resource. see javadoc heading for details.
    * @throws ConfigObjectsNotResolvableException if the
    * ServiceComponentDefn does not have a reference to a ComponentType object
    * for which there is data to recombine in the DirectoryEntry resource.
    * @throws IOException if there is an error reading from the DirectoryEntry
    * @throws InvalidConfigurationElementException if there is a problem with
    * the representation of the configuration element as it exists in the
    * DirectoryEntry resource, usually some type of formatting problem.
    */
//    public Object[] importServiceComponentDefn(InputStream stream,
//                 Configuration config, ConfigurationObjectEditor editor,
//                 String[] name)throws IOException,
//                 ConfigObjectsNotResolvableException,
//                 InvalidConfigurationElementException {
//
//        Assertion.isNotNull(stream);
//        Assertion.isNotNull(editor);
//
////***        LogManager.logDetail(LogCommonConstants.CTX_CONFIG, "Importing a ServiceComponentDefn object.");
//
//        Document doc = null;
//
//
//        try {
//            doc = getXMLReaderWriter().readDocument(stream);
//        }catch(JDOMException e) {
//     		e.printStackTrace();
//            throw new IOException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0006));
//        }
//
//        XMLHelper helper  = getXMLHelper();
//
//        Element root = doc.getRootElement();
//
//        ComponentType type = createComponentType(doc, editor, name[COMPONENT_TYPE_INDEX]);
//
//        Element serviceComponentDefnsElement = root.getChild(XMLElementNames.Configurations.Configuration.ServiceComponentDefns.ELEMENT);
//
//        if (serviceComponentDefnsElement == null) {
//            throw new InvalidConfigurationElementException(ErrorMessageKeys.CONFIG_ERR_0008, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0008, XMLElementNames.Configurations.Configuration.ServiceComponentDefns.ELEMENT));
//        }
//
//        Element serviceComponentDefnElement = serviceComponentDefnsElement.getChild(XMLElementNames.Configurations.Configuration.ServiceComponentDefns.ServiceComponentDefn.ELEMENT);
//
//    	ComponentDefn cd = helper.createServiceComponentDefn(serviceComponentDefnElement, config, editor, name[SERVICE_COMPONENT_DEFN_INDEX]);
//        Object[] object  = {type, cd};
//
//
////***        I18nLogManager.logInfo(LogCommonConstants.CTX_CONFIG, LogMessageKeys.CONFIG_MSG_0008, cd.getName());
//
//        return object;
//    }


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
    public void exportConnector(OutputStream stream, ComponentType type, Properties props) throws IOException {
        // no resolving issues with this implementation...
        exportComponentType(stream, type, props);
    }

    /**
    * <p>This method will generally be used to create a file representation of a
    * Connector Binding.  It will write to the InputStream the representation
    * of the ServiceComponentDefn object that is passed in.</p>
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
    public void exportConnectorBinding(OutputStream stream, ConnectorBinding defn, ComponentType type, Properties props) throws IOException, ConfigObjectsNotResolvableException  {
        ConfigurationImportExport util = new ConfigurationImportExport();
      
      util.exportConnectorBinding(stream, defn, type, props);
    }
    
    
//    public void exportConnectorBindings(ConnectorBinding[] bindings,
//                                        ComponentType[] types,
//                                        Element root) {
//        ConfigurationImportExport util = new ConfigurationImportExport();
//        
//        util.exportConnectorBindings(bindings, types, root);
//        
//    }
    
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
    
    public void exportConnectorBindings(OutputStream stream, ConnectorBinding[] bindings, ComponentType[] types, Properties props) throws IOException, ConfigObjectsNotResolvableException  {       
        ConfigurationImportExport util = new ConfigurationImportExport();
        util.exportConnectorBindings(stream, bindings, types, props);
    }


    public ConnectorBinding importConnectorBinding(InputStream stream, ConfigurationObjectEditor editor, String newName)throws IOException, ConfigObjectsNotResolvableException, InvalidConfigurationElementException {

        ConfigurationImportExport util = new ConfigurationImportExport();
        return util.importConnectorBinding(stream, editor, newName);
}

    public Collection importExistingConnectorBindings(Element root, ConfigurationObjectEditor editor, boolean importExistingBinding)throws IOException, ConfigObjectsNotResolvableException, InvalidConfigurationElementException {

        ConfigurationImportExport util = new ConfigurationImportExport();

        return util.importConnectorBindings(root, editor, importExistingBinding);
    }
    
    public Collection importConnectorBindings(Element root, ConfigurationObjectEditor editor)throws IOException, ConfigObjectsNotResolvableException, InvalidConfigurationElementException {

        ConfigurationImportExport util = new ConfigurationImportExport();

        return util.importConnectorBindings(root, editor, false);
    }
    
    public Collection importConnectorBindings(InputStream stream, ConfigurationObjectEditor editor)throws IOException, ConfigObjectsNotResolvableException, InvalidConfigurationElementException {

        ConfigurationImportExport util = new ConfigurationImportExport();

        return util.importConnectorBindings(stream, editor, false);
    }
    
    public Collection importExistingConnectorBindings(InputStream stream, ConfigurationObjectEditor editor, boolean useExistingBinding)throws IOException, ConfigObjectsNotResolvableException, InvalidConfigurationElementException {

        ConfigurationImportExport util = new ConfigurationImportExport();

        return util.importConnectorBindings(stream, editor, useExistingBinding);
    }

    public Object[] importConnectorBindingAndType(InputStream stream, ConfigurationObjectEditor editor, String[] newName)throws IOException, ConfigObjectsNotResolvableException, InvalidConfigurationElementException {

        ConfigurationImportExport util = new ConfigurationImportExport();
        return util.importConnectorBindingAndType(stream, editor, newName);
    }


    /**
    * <p>This method will resolve that none of the configuration objects in the
    * collection of configuration objects refers to a configuration object that
    * is not in the collection.  Any set of configuration obejcts can be passed
    * to this method.  </p>
    *
    * <p>Any number of actual Configuration object instances may be passed in
    * in the Collection.</p>
    *
    * @param collection the collection of configuration objects to be resolved
    * @throws ConfigObjectsNotResolvableException if the collection of objects
    * passed in are not self containing.
    */
    public void resolveConfigurationObjects(Collection collection) throws ConfigObjectsNotResolvableException{

        ConfigurationImportExport util = new ConfigurationImportExport();
        
        util.resolveConfigurationObjects(collection);

    }


    /**
     * The Zip file stream format is look like this.
     * 
     *  /ConnectorTypes
     *      /ConnectorTypeName
     *           ConnectorTypeName.xml
     *           extension1.jar
     *           extension2.jar
     *  
     * @see com.metamatrix.common.config.util.ConfigurationImportExport#importConnectorArchive(java.io.InputStream, com.metamatrix.common.config.api.ConfigurationObjectEditor)
     * @since 4.3.2
     */
    public ConnectorArchive importConnectorArchive(InputStream stream, ConfigurationObjectEditor editor) 
        throws IOException, InvalidConfigurationElementException {
        ConnectorArchiveImportExportUtility util = new ConnectorArchiveImportExportUtility();
        return util.importConnectorArchive(stream, editor, this);
    }
    

    /** 
     * @see com.metamatrix.common.config.util.ConfigurationImportExportUtility#exportConnectorArchive(java.io.OutputStream, com.metamatrix.common.config.api.ConnectorArchive)
     * @since 4.3
     */
    public void exportConnectorArchive(OutputStream stream, ConnectorArchive archive, Properties properties) 
        throws IOException, ConfigObjectsNotResolvableException {        
        ConnectorArchiveImportExportUtility util = new ConnectorArchiveImportExportUtility();
        util.exportConnectorArchive(stream, archive, properties, this);
    }
}
