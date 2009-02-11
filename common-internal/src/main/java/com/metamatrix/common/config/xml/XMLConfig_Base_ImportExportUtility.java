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

package com.metamatrix.common.config.xml;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentObject;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.ProductServiceConfig;
import com.metamatrix.common.config.api.ProductType;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.util.ConfigObjectsNotResolvableException;
import com.metamatrix.common.config.util.ConfigurationImportExportUtility;
import com.metamatrix.common.config.util.ConfigurationPropertyNames;
import com.metamatrix.common.config.util.InvalidConfigurationElementException;
import com.metamatrix.common.namedobject.BaseObject;
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
public abstract class XMLConfig_Base_ImportExportUtility implements ConfigurationPropertyNames {

    private XMLReaderWriter readerWriter;
//    private XMLHelper helper = null;

    /**
    * These static variables define the constants that will be used to
    * create the header for every document that is produced using this concrete
    * utility.
    */
    public static final String DEFAULT_USER_CREATED_BY = "Unknown"; //$NON-NLS-1$

    /**
    * These indices are the indices used to retreive the Lists from the array
    * of lists from the segregateConfigurationObjects method.
    */
    static final int CONFIGURATIONS_INDEX = 0;
    static final int PRODUCT_TYPES_INDEX = 1;
    static final int HOSTS_INDEX = 2;
    static final int DEPLOYED_COMPONENTS_INDEX = 3;
    static final int SERVICE_COMPONENT_DEFNS_INDEX = 4;
    static final int VM_COMPONENT_DEFNS_INDEX = 5;
    static final int COMPONENT_TYPES_INDEX = 6;
    static final int PRODUCT_SERVICE_CONFIGS_INDEX = 7;
    static final int CONNECTION_POOL_CONFIGS_INDEX = 16;
    static final int RESOURCES_INDEX = 18;
    static final int CONNECTORS_INDEX = 19;


    static final int CONFIGURATION_IDS_INDEX = 8;
    static final int PRODUCT_TYPE_IDS_INDEX = 9;
    static final int HOST_IDS_INDEX = 10;
    static final int DEPLOYED_COMPONENT_IDS_INDEX = 11;
    static final int SERVICE_COMPONENT_DEFN_IDS_INDEX = 12;
    static final int VM_COMPONENT_DEFN_IDS_INDEX = 13;
    static final int COMPONENT_TYPE_IDS_INDEX = 14;
    static final int PRODUCT_SERVICE_CONFIG_IDS_INDEX = 15;
//    private static final int CONNECTION_POOL_CONFIG_IDS_INDEX = 17;
    static final int CONNECTORS_IDS_INDEX = 20;


    static final int NUMBER_OF_LISTS = 21;

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
//    public void exportConfiguration(OutputStream stream,
//                     Collection configurationObjects, Properties props)
//                     throws IOException, ConfigObjectsNotResolvableException {
//
//
//        Assertion.isNotNull(configurationObjects);
//        Assertion.isNotNull(stream);
//
//        // this will throw ConfigObjectsNotResolvableException if the collection
//        // of configuration objects are not self containing.
//        resolveConfigurationObjects(configurationObjects);
//
//        // this will divide the configuration objects by their type
//        // so that we can create an XML doc and put the objects in their proper
//        // categories.
//        List[] lists = segregateConfigurationObjects(configurationObjects);
//
//        List configurations = lists[CONFIGURATIONS_INDEX];
//        List productTypes = lists[PRODUCT_TYPES_INDEX];
//        List hosts = lists[HOSTS_INDEX];
//        List componentTypes = lists[COMPONENT_TYPES_INDEX];
//        List resources = lists[RESOURCES_INDEX];
//        List connectors = lists[CONNECTORS_INDEX];
//
//
// 
//        // this helper class contains all of the code to convert configuration
//        // objects into JDOM Elements
//        XMLHelper helper = getXMLHelper();
//        
//        // this visitor will visit all of the configuration objects in a
//        // configuration collecting their state as JDOM XML Elements
//        XMLConfigurationVisitor visitor = new XMLConfigurationVisitor(helper);
//        
//
//        Element root = helper.createRootConfigurationDocumentElement();
//
//        // create a new Document with a root element
//        Document doc = new Document(root);
//
//        // add the header element
//        root.addContent(helper.createHeaderElement(createHeaderProperties(props)));
//
////		String configName = "";
//        // iterate through the configuration objects, if there are any,
//        // and create elements for them.
//        Iterator iterator = configurations.iterator();
//        if (iterator.hasNext()) {
//            Element configurationsElement = helper.createConfigurationsElement();
//            root.addContent(configurationsElement);
//            while (iterator.hasNext()) {
//                Configuration config = (Configuration)iterator.next();
//
//                // generally only one configuration is done at a time
////                configName = config.getID().getFullName();
////                LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Found Configuration named: " + config + " in list of configuration objects to export.");
//                // this will cause the visitor to visit and build XML for the
//                // Configuration object and all objects that the Configuration
//                // references directly.
//                config.accept(visitor);
//                configurationsElement.addContent(visitor.getConfigurationElement());
//            }
//        }
//
//        iterator = connectors.iterator();
//        if (iterator.hasNext()) {
//            Element connectorsElement = helper.createConnectorBindingsElement();
//            root.addContent(connectorsElement);
//            while (iterator.hasNext()) {
//                ConnectorBinding connector = (ConnectorBinding)iterator.next();
//                Element connElement = helper.createConnectorBindingElement(connector, true);
//                connectorsElement.addContent(connElement);
//            }
//        }
//
//
//        // iterate through the product type objects, if there are any,
//        // and create elements for them.
//        iterator = productTypes.iterator();
//        if (iterator.hasNext()) {
//            Element productTypesElement = helper.createProductTypesElement();
//            root.addContent(productTypesElement);
//            while (iterator.hasNext()) {
//                ProductType type = (ProductType)iterator.next();
////                LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Found ProductType named: " + type + " in list of configuration objects to export.");
//                Element productTypeElement = helper.createProductTypeElement(type);
//                productTypesElement.addContent(productTypeElement);
//            }
//        }
//
//        iterator = hosts.iterator();
//        if (iterator.hasNext()) {
//            Element hostsElement = helper.createHostsElement();
//            root.addContent(hostsElement);
//            while (iterator.hasNext()) {
//                Host host = (Host)iterator.next();
////                LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Found Host named: " + host + " in list of configuration objects to export.");
//                Element hostElement = helper.createHostElement(host);
//                hostsElement.addContent(hostElement);
//            }
//        }
//
//        iterator = resources.iterator();
//        if (iterator.hasNext()) {
//            Element resourcesElement = helper.createSharedResourcesElement();
//            root.addContent(resourcesElement);
//            while (iterator.hasNext()) {
//                SharedResource resource = (SharedResource)iterator.next();
////                LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Found SharedResource named: " + resource + " in list of configuration objects to export.");
//                Element resourceElement = helper.createSharedResourceElement(resource);
//                resourcesElement.addContent(resourceElement);
//            }
//        }
//
//
//        iterator = componentTypes.iterator();
//        if (iterator.hasNext()) {
//            Element componentTypesElement = helper.createComponentTypesElement();
//            root.addContent(componentTypesElement);
//            while (iterator.hasNext()) {
//                ComponentType componentType = (ComponentType)iterator.next();
//                Element componentTypeElement = helper.createComponentTypeElement(componentType);
////                LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Found ComponentType named: " + componentType + " in list of configuration objects to export.");
//
//                componentTypesElement.addContent(componentTypeElement);
//            }
//        }
//
//
//
//        getXMLReaderWriter().writeDocument(doc, stream);
//        stream.close();
////        I18nLogManager.logInfo(LogCommonConstants.CTX_CONFIG, LogMessageKeys.CONFIG_MSG_0003, configName);
//    }
//

    public void exportComponentType(OutputStream stream, ComponentType type,
                     Properties props) throws IOException {


        Assertion.isNotNull(type);
        Assertion.isNotNull(stream);

//        LogManager.logDetail(LogCommonConstants.CTX_CONFIG, "Exporting a ComponentType: "+ type.getName() + ".");

        XMLHelper helper = getXMLHelper();

        Element root = helper.createRootConfigurationDocumentElement();

        // create a new Document with a root element
        Document doc = new Document(root);

        // add the header element
        root = XMLHelperUtil.addHeaderElement(root, props);
//        root.addContent(helper.createHeaderElement(createHeaderProperties(props)));

        Element componentTypesElement = helper.createComponentTypesElement();
        root.addContent(componentTypesElement);
        componentTypesElement.addContent(helper.createComponentTypeElement(type));

        getXMLReaderWriter().writeDocument(doc, stream);
//        I18nLogManager.logInfo(LogCommonConstants.CTX_CONFIG, LogMessageKeys.CONFIG_MSG_0004, type.getName());
    }
    
    public void exportComponentTypes(OutputStream stream, ComponentType[] types,
                                    Properties props) throws IOException {


           Assertion.isNotNull(types);
           Assertion.isNotNull(stream);

//                       LogManager.logDetail(LogCommonConstants.CTX_CONFIG, "Exporting a ComponentType: "+ type.getName() + ".");

           XMLHelper helper = getXMLHelper();

           Element root = helper.createRootConfigurationDocumentElement();

           // create a new Document with a root element
           Document doc = new Document(root);

           // add the header element
           root = XMLHelperUtil.addHeaderElement(root, props);
//           root.addContent(helper.createHeaderElement(createHeaderProperties(props)));

           Element componentTypesElement = helper.createComponentTypesElement();
           root.addContent(componentTypesElement);
           
           int s = types.length;

           for (int i = 0; i<s; i++) {
               ComponentType type = types[i];
    
               componentTypesElement.addContent(helper.createComponentTypeElement(type));

           }
           
           getXMLReaderWriter().writeDocument(doc, stream);
//                       I18nLogManager.logInfo(LogCommonConstants.CTX_CONFIG, LogMessageKeys.CONFIG_MSG_0004, type.getName());
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
    public void exportServiceComponentDefn(OutputStream stream,
                 ServiceComponentDefn defn, ComponentType type, Properties props)
                 throws IOException, ConfigObjectsNotResolvableException {
        Assertion.isNotNull(defn);
        Assertion.isNotNull(type);
        Assertion.isNotNull(stream);

//        LogManager.logDetail(LogCommonConstants.CTX_CONFIG, "Exporting a ServiceDefinition: " + defn.getName() + ".");

        List configurationObjects = new ArrayList(2);

        configurationObjects.add(defn);
        configurationObjects.add(type);

        // here we need to make sure that the serviceComponentDefn references
        // the passed in ComponentType instance.  If not, this will throw
        // the ConfigObjectsNotResolvableException
        resolveConfigurationObjects(configurationObjects);
//        LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Configuration objects to export resolved properly.");

        XMLHelper helper = getXMLHelper();

        Element root = helper.createRootConfigurationDocumentElement();

        // create a new Document with a root element
        Document doc = new Document(root);
        
        root = XMLHelperUtil.addHeaderElement(root, props);

        // add the header element
//        root.addContent(helper.createHeaderElement(createHeaderProperties(props)));

        Element componentTypesElement = helper.createComponentTypesElement();
        root.addContent(componentTypesElement);
        componentTypesElement.addContent(helper.createComponentTypeElement(type));

        Element serviceComponentDefnsElement = helper.createServiceComponentDefnsElement();
        root.addContent(serviceComponentDefnsElement);
        serviceComponentDefnsElement.addContent(helper.createServiceComponentDefnElement(defn));

        getXMLReaderWriter().writeDocument(doc, stream);
//        I18nLogManager.logInfo(LogCommonConstants.CTX_CONFIG, LogMessageKeys.CONFIG_MSG_0005, defn.getName());
    }

    
    protected Collection createConnectorBindings(ConfigurationID configurationID, Element root, ConfigurationObjectEditor editor, boolean importExistingBinding)
    	throws IOException, ConfigObjectsNotResolvableException,
                     InvalidConfigurationElementException {
//        Element root = doc.getRootElement();
        Element connectorsElement = root.getChild(XMLElementNames.ConnectorComponents.ELEMENT);
		if (connectorsElement == null) {
			return Collections.EMPTY_LIST;
		}

       List connectorBindings = connectorsElement.getChildren(XMLElementNames.ConnectorComponents.ConnectorComponent.ELEMENT);

	   List connObjects = new ArrayList(connectorBindings.size());
	   if (connectorBindings != null) {
	        Iterator iterator = connectorBindings.iterator();
	        while (iterator.hasNext()) {
	            Element connElement = (Element)iterator.next();
                ConnectorBinding conn = getXMLHelper().createConnectorBinding(configurationID, connElement, editor, null, importExistingBinding);
//				System.out.println("Created Connector Binding " + conn.getID());
	//            LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Found Host named: " + host + " in XML file Hosts element.");
	            connObjects.add(conn);
	        }
		}

    	return connObjects;
    }
    
    protected ConnectorBinding createConnectorBinding(ConfigurationID configurationID, Element root, ConfigurationObjectEditor editor, String name, boolean isImportConfig) 
    	throws IOException, ConfigObjectsNotResolvableException,
                     InvalidConfigurationElementException {
//        Element root = doc.getRootElement();
        Element connectorsElement = root.getChild(XMLElementNames.ConnectorComponents.ELEMENT);
		if (connectorsElement == null) {
			return null;
		}

       List connectorBindings = connectorsElement.getChildren(XMLElementNames.ConnectorComponents.ConnectorComponent.ELEMENT);

		// return the first binding to be created because only one
		// binding is created in this method
	   if (connectorBindings != null) {
	        Iterator iterator = connectorBindings.iterator();
	        while (iterator.hasNext()) {
	            Element connElement = (Element)iterator.next();
                ConnectorBinding conn = getXMLHelper().createConnectorBinding(configurationID, connElement, editor, name, isImportConfig);
				return conn;
	        }
		}

    	return null;
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
//    public ComponentType importComponentType(Element root,
//                                             //InputStream stream,
//                 ConfigurationObjectEditor editor, String name)
//                 throws IOException, InvalidConfigurationElementException {
//
// //       Assertion.isNotNull(stream);
//        Assertion.isNotNull(editor);
//
////***        LogManager.logDetail(LogCommonConstants.CTX_CONFIG, "Importing a ComponentType object...");
//
////        Document doc = null;
////
////        try {
////            doc = getXMLReaderWriter().readDocument(stream);
////        }catch(JDOMException e) {
////        	e.printStackTrace();
////            throw new IOException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0005));
////        }
////        Element root = doc.getRootElement();
//
//
//    	ComponentType t = createComponentType(root, editor, name);
//
////***        I18nLogManager.logInfo(LogCommonConstants.CTX_CONFIG, LogMessageKeys.CONFIG_MSG_0007, t.getName());
//
//        return t;
//    }
//    /**
//    * <p>This method will be used to import 1 or more a ComponentType Objects.</p>
//    *
//    * @param editor the ConfigurationObjectEditor to use to create the Configuration
//    * objects.
//    * @param stream the input stream to read the configuration object
//    * representation from
//    * @return Collection of objects of type <code>ComponentType</code>
//    * @throws IOException if there is an error reading from the DirectoryEntry
//    * @throws InvalidConfigurationElementException if there is a problem with
//    * the representation of the configuration element as it exists in the
//    * DirectoryEntry resource, usually some type of formatting problem.
//    */
//
//    public Collection importComponentTypes(InputStream stream,
//                 ConfigurationObjectEditor editor)
//                 throws IOException, InvalidConfigurationElementException {
//         Assertion.isNotNull(stream);
//         Assertion.isNotNull(editor);
//
//         Document doc = null;
//
//         try {
//             doc = getXMLReaderWriter().readDocument(stream);
//         }catch(JDOMException e) {
//             e.printStackTrace();
//             throw new IOException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0005));
//         }
//
//         Element root = doc.getRootElement();
//
//
//         Collection connectorTypes = createComponentTypes(root, editor) ;
//
//         return connectorTypes;       
//   
//    }




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
    * ConfigurationImportExportUtility.COMPONENT_TYPE_INDEX
    * ConfigurationImportExportUtility.SERVICE_COMPONENT_DEFN_INDEX
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
    public Object[] importServiceComponentDefn(InputStream stream,
                 Configuration config, ConfigurationObjectEditor editor,
                 String[] name)throws IOException,
                 ConfigObjectsNotResolvableException,
                 InvalidConfigurationElementException {

        Assertion.isNotNull(stream);
        Assertion.isNotNull(editor);

//***        LogManager.logDetail(LogCommonConstants.CTX_CONFIG, "Importing a ServiceComponentDefn object.");

        Document doc = null;


        try {
            doc = getXMLReaderWriter().readDocument(stream);
        }catch(JDOMException e) {
     		e.printStackTrace();
            throw new IOException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0006));
        }

        XMLHelper helper  = getXMLHelper();

        Element root = doc.getRootElement();

        ComponentType type = createComponentType(root, editor, name[ConfigurationImportExportUtility.COMPONENT_TYPE_INDEX]);

        Element serviceComponentDefnsElement = root.getChild(XMLElementNames.Configurations.Configuration.ServiceComponentDefns.ELEMENT);

        if (serviceComponentDefnsElement == null) {
            throw new InvalidConfigurationElementException(ErrorMessageKeys.CONFIG_ERR_0008, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0008, XMLElementNames.Configurations.Configuration.ServiceComponentDefns.ELEMENT));
        }
        

        Element serviceComponentDefnElement = serviceComponentDefnsElement.getChild(XMLElementNames.Configurations.Configuration.ServiceComponentDefns.ServiceComponentDefn.ELEMENT);

    	ComponentDefn cd = helper.createServiceComponentDefn(serviceComponentDefnElement, config, editor, name[ConfigurationImportExportUtility.SERVICE_COMPONENT_DEFN_INDEX]);
        Object[] object  = {type, cd};


//***        I18nLogManager.logInfo(LogCommonConstants.CTX_CONFIG, LogMessageKeys.CONFIG_MSG_0008, cd.getName());

        return object;
    }

//    protected Collection createComponentTypes(Element root, ConfigurationObjectEditor editor) throws InvalidConfigurationElementException{
//
//        Element componentTypesElement = root.getChild(XMLElementNames.ComponentTypes.ELEMENT);
//
//        if (componentTypesElement == null) {
//            return Collections.EMPTY_LIST;
//        }
//                
//
//        List componentTypes = componentTypesElement.getChildren(XMLElementNames.ComponentTypes.ComponentType.ELEMENT);
//  
//        List connObjects = null;
//        
//        if (componentTypes != null) {
//            connObjects = new ArrayList(componentTypes.size());
//        
//            getXMLHelper().orderComponentTypeElementList(componentTypes);
//            Iterator iterator = componentTypes.iterator();
//            while (iterator.hasNext()) {
//                Element connElement = (Element)iterator.next();
//                ComponentType type = getXMLHelper().createComponentType(connElement, editor, null, true);
//                connObjects.add(type);                
//                
//            }
//        } else {
//            return Collections.EMPTY_LIST; 
//        }
//        
//        
////        List connObjects = null;
////        if (connectorTypes != null) {
////            connObjects = new ArrayList(connectorTypes.size());
////             Iterator iterator = connectorTypes.iterator();
////             while (iterator.hasNext()) {
////                 Element connElement = (Element)iterator.next();
////                 ComponentType type = helper.createComponentType(connElement, editor, null, true);
////                 connObjects.add(type);
////             }
////        } else {
////            return Collections.EMPTY_LIST;
////        }
//
//        return connObjects;
//    }
     

    protected ComponentType createComponentType(Element root, ConfigurationObjectEditor editor, String name) throws InvalidConfigurationElementException{
        XMLHelper helper  = getXMLHelper();

//        Element root = doc.getRootElement();

        Element componentTypesElement = root.getChild(XMLElementNames.ComponentTypes.ELEMENT);

        if (componentTypesElement == null) {
            throw new InvalidConfigurationElementException(ErrorMessageKeys.CONFIG_ERR_0008, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0008,XMLElementNames.ComponentTypes.ELEMENT));
        }

        Element componentTypeElement = componentTypesElement.getChild(XMLElementNames.ComponentTypes.ComponentType.ELEMENT);

        if (componentTypeElement == null) {
            throw new InvalidConfigurationElementException(ErrorMessageKeys.CONFIG_ERR_0008, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0008,XMLElementNames.ComponentTypes.ComponentType.ELEMENT));
        }

        return helper.createComponentType(componentTypeElement, editor, name, true);
    }
    
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
        Assertion.isNotNull(defn);
        Assertion.isNotNull(type);
        Assertion.isNotNull(stream);

//***        LogManager.logDetail(LogCommonConstants.CTX_CONFIG, "Exporting Connector Binding: " + defn.getName() + ".");

        // here we ensure that this is a Connector and that the given component
        // type resolves to the given ServiceComponentDefn.
        resolveConnector(type, defn);

//        LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Connector binding object to export resolved properly.");

        XMLHelper helper = getXMLHelper();

        Element root = helper.createRootConfigurationDocumentElement();

        // create a new Document with a root element
        Document doc = new Document(root);

        root = XMLHelperUtil.addHeaderElement(root, props);
        // add the header element
//        root.addContent(helper.createHeaderElement(createHeaderProperties(props)));

        Element componentTypesElement = helper.createComponentTypesElement();
        root.addContent(componentTypesElement);
        componentTypesElement.addContent(helper.createComponentTypeElement(type));

        Element connectorsElement = helper.createConnectorBindingsElement();
        root.addContent(connectorsElement);
        Element connElement = helper.createConnectorBindingElement(defn, false );
        connectorsElement.addContent(connElement);

        getXMLReaderWriter().writeDocument(doc, stream);
//***        I18nLogManager.logInfo(LogCommonConstants.CTX_CONFIG, LogMessageKeys.CONFIG_MSG_0009, defn.getName());
    }
    
    
    public void exportConnectorBindings(OutputStream stream, ConnectorBinding[] bindings, ComponentType[] types, Properties props) throws IOException, ConfigObjectsNotResolvableException  {
        Assertion.isNotNull(bindings);
        Assertion.isNotNull(types);
        Assertion.isNotNull(stream);
        
       
//        LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Connector binding object to export resolved properly.");

        XMLHelper helper = getXMLHelper();

        Element root = helper.createRootConfigurationDocumentElement();

        // create a new Document with a root element
        Document doc = new Document(root);

        root = XMLHelperUtil.addHeaderElement(root, props);
        // add the header element
//        root.addContent(helper.createHeaderElement(createHeaderProperties(props)));

        exportConnectorBindings(bindings, types, root);
    
        getXMLReaderWriter().writeDocument(doc, stream);
       
    }
    
    
//    public ComponentType importConnector(InputStream stream, ConfigurationObjectEditor editor, String newName)throws IOException, InvalidConfigurationElementException {
//        // no special implementation changes needed to import Connectors
//        return importComponentType(stream, editor, newName);
//    }


    /** 
     * Add connector bindings and component types (connector types) under
     * a common root element.
     * @param bindings
     * @param types
     * @param helper
     * @param root
     * @since 4.2
     */
    public void exportConnectorBindings(ConnectorBinding[] bindings,
                                         ComponentType[] types,
                                         Element root) {
        XMLHelper helper = getXMLHelper();
        int s = bindings.length;
        List ts = new ArrayList(s);
        
        Element componentTypesElement = helper.createComponentTypesElement();
        root.addContent(componentTypesElement);


        Element connectorsElement = helper.createConnectorBindingsElement();
        root.addContent(connectorsElement);
        
        if (bindings == null || bindings.length == 0) {
            return;
        }
   
        int tsize = types.length;
        Map typeMap = new HashMap(tsize);

        for (int i = 0; i<tsize; i++) {
            ComponentType type = types[i];
            if (type != null) {
                typeMap.put(type.getID(), type);
            }
        }

        
        
        for (int i = 0; i<s; i++) {

            ConnectorBinding cb = bindings[i];
            if (cb == null) {
                continue;
            }
            ComponentType type = (ComponentType) typeMap.get(cb.getComponentTypeID());          
 
            // here we ensure that this is a Connector and that the given component
            // type resolves to the given ServiceComponentDefn.
//            try {
//                resolveConnector(type, cb);
//            } catch(ConfigObjectsNotResolvableException conr) {                
//               
//                conr.printStackTrace(System.out);
//                
//                System.out.print(conr.getMessage()); 
//                continue;
//            }

            if (type != null && !ts.contains(type.getFullName())) {
                componentTypesElement.addContent(helper.createComponentTypeElement(type));
                ts.add(type.getFullName());
            }
                
            Element connElement = helper.createConnectorBindingElement(cb, false );
            connectorsElement.addContent(connElement);              

        }
    }

    public ConnectorBinding importConnectorBinding(InputStream stream, ConfigurationObjectEditor editor, String newName)throws IOException, ConfigObjectsNotResolvableException, InvalidConfigurationElementException {
        Assertion.isNotNull(stream);
        Assertion.isNotNull(editor);

//***        LogManager.logDetail(LogCommonConstants.CTX_CONFIG, "Importing a ConnectorBinding object.");

        Document doc = null;

        try {
            doc = getXMLReaderWriter().readDocument(stream);
        } catch(JDOMException e) {
            throw new IOException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0010));
        }

        Element root = doc.getRootElement();
        

        ConnectorBinding binding = createConnectorBinding(Configuration.NEXT_STARTUP_ID, root, editor, newName, false) ;

   		if (binding == null) {
   			throw new ConfigObjectsNotResolvableException(ErrorMessageKeys.CONFIG_ERR_0011, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0011));
   		}

//***       I18nLogManager.logInfo(LogCommonConstants.CTX_CONFIG, LogMessageKeys.CONFIG_MSG_0010, binding.getName());


        return binding;

}

    public Collection importConnectorBindings(Element root, ConfigurationObjectEditor editor, boolean importExistingBinding)throws IOException, ConfigObjectsNotResolvableException, InvalidConfigurationElementException {
        Assertion.isNotNull(root);
        Assertion.isNotNull(editor);

//***        LogManager.logDetail(LogCommonConstants.CTX_CONFIG, "Importing 1 or more ConnectorBinding objects.");


        Collection connectorBindings = createConnectorBindings(Configuration.NEXT_STARTUP_ID, root, editor, importExistingBinding) ;

//***        I18nLogManager.logInfo(LogCommonConstants.CTX_CONFIG, LogMessageKeys.CONFIG_MSG_0011);

        return connectorBindings;
    }
    
    

    public Collection importConnectorBindings(InputStream stream, ConfigurationObjectEditor editor, boolean importExistingBinding)throws IOException, ConfigObjectsNotResolvableException, InvalidConfigurationElementException {
        Assertion.isNotNull(stream);
        Assertion.isNotNull(editor);

//***        LogManager.logDetail(LogCommonConstants.CTX_CONFIG, "Importing 1 or more ConnectorBinding objects.");

        Document doc = null;

        try {
            doc = getXMLReaderWriter().readDocument(stream);
        } catch(JDOMException e) {
            throw new IOException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0010));

        }

        Element root = doc.getRootElement();
        
        return importConnectorBindings(root, editor, importExistingBinding);
    }

    public Object[] importConnectorBindingAndType(InputStream stream, ConfigurationObjectEditor editor, String[] newName)throws IOException, ConfigObjectsNotResolvableException, InvalidConfigurationElementException {

        Assertion.isNotNull(stream);
        Assertion.isNotNull(editor);

//***        LogManager.logDetail(LogCommonConstants.CTX_CONFIG, "Importing a Connector Binding object and Connector Type.");

        Document doc = null;

        try {
            doc = getXMLReaderWriter().readDocument(stream);
        } catch(JDOMException e) {
            throw new IOException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0012));
        }

		String typeName = null;
		String bindingName = null;
		if (newName != null) {
			typeName = newName[ConfigurationImportExportUtility.COMPONENT_TYPE_INDEX];
			bindingName = newName[ConfigurationImportExportUtility.SERVICE_COMPONENT_DEFN_INDEX];
		}
        Element root = doc.getRootElement();

        ComponentType type = createComponentType(root, editor, typeName);

		if (type == null) {
			throw new InvalidConfigurationElementException(ErrorMessageKeys.CONFIG_ERR_0013, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0013));
		}


        ConnectorBinding binding = createConnectorBinding(Configuration.NEXT_STARTUP_ID, root, editor, bindingName, false) ;


   		if (binding == null) {
   	       throw new ConfigObjectsNotResolvableException(ErrorMessageKeys.CONFIG_ERR_0014, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0014));
   		}

        // here we ensure that this is a Connector and that the given component
        // type resolves to the given ServiceComponentDefn.
        resolveConnector(type, binding);

        Object[] object  = {type, binding
        };

//***        I18nLogManager.logInfo(LogCommonConstants.CTX_CONFIG, LogMessageKeys.CONFIG_MSG_0012, new Object[] {type.getName(), binding.getName()});

        return object;

    }

 
    /**
    * This method is used specifically to resolve Connector Binding
    * ServiceComponentDefns to their respective Connector ComponentTypes.
    */
    private void resolveConnector(ComponentType type, ComponentDefn defn) throws ConfigObjectsNotResolvableException {

        // check to be sure that the passed in  ComponentType is the correct one for
        // this ServiceComponentDefn.
        if (type == null) {
            String msg = CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0015, new Object[] {defn.getID(), defn.getComponentTypeID()} ); 
                
            System.err.print(msg);

            throw new ConfigObjectsNotResolvableException(ErrorMessageKeys.CONFIG_ERR_0015, msg); 
            
        } else if (!(defn.getComponentTypeID().equals(type.getID()))) {
            String msg = CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0015, new Object[] {defn.getID(), type.getID()} ); 
            
            System.err.print(msg);

            throw new ConfigObjectsNotResolvableException(ErrorMessageKeys.CONFIG_ERR_0015, msg);
        }
        // the super and parent ComponentTypeID's of all Connectors are
        // always the same as defined by the static finals in the config objects.
//        if (!(type.getParentComponentTypeID().getName().equals(ProductType.CONNECTOR_PRODUCT_TYPE_NAME))) {
//            throw new ConfigObjectsNotResolvableException(ErrorMessageKeys.CONFIG_ERR_0016, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0016, type.getParentComponentTypeID().getName()));
//        }

        if (!type.isOfTypeConnector()) {
            throw new ConfigObjectsNotResolvableException(ErrorMessageKeys.CONFIG_ERR_0017, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0017, type.getSuperComponentTypeID().getName()));
        }
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

        List[] lists = segregateConfigurationObjects(collection);
        
        resolveConfigurationObjects(lists);
    }
        
        
        
    protected void resolveConfigurationObjects(List[] lists) throws ConfigObjectsNotResolvableException{        

        List configurations = lists[CONFIGURATIONS_INDEX];
//        List productTypes = lists[PRODUCT_TYPES_INDEX];

        List hosts = lists[HOSTS_INDEX];
        List deployedComponents = lists[DEPLOYED_COMPONENTS_INDEX];
        List serviceComponentDefns = lists[SERVICE_COMPONENT_DEFNS_INDEX];
        List vmComponentDefns = lists[VM_COMPONENT_DEFNS_INDEX];
        List componentTypes = lists[COMPONENT_TYPES_INDEX];
        List productServiceConfigs = lists[PRODUCT_SERVICE_CONFIGS_INDEX];

        List connectionPools = lists[CONNECTION_POOL_CONFIGS_INDEX];

        List resources = lists[RESOURCES_INDEX];

        List connectorBindings = lists[CONNECTORS_INDEX];

        List configurationIDs = lists[CONFIGURATION_IDS_INDEX];
        List productTypeIDs = lists[PRODUCT_TYPE_IDS_INDEX];
        List hostIDs = lists[HOST_IDS_INDEX];
        List serviceComponentDefnIDs = lists[SERVICE_COMPONENT_DEFN_IDS_INDEX];
        List vmComponentDefnIDs = lists[VM_COMPONENT_DEFN_IDS_INDEX];
        List componentTypeIDs = lists[COMPONENT_TYPE_IDS_INDEX];
        List productServiceConfigIDs = lists[PRODUCT_SERVICE_CONFIG_IDS_INDEX];

        List connectorBindingsIDs = lists[CONNECTORS_IDS_INDEX];


        // we do this because a DeployedComponent can have a null ServiceComponentDefnID
        // and a null ProductServiceComponentID and still be viable.  This will
        // allow the contains methods called on these Lists later in this method
        // to return true if the ID is null instead of throwing a bogus exception
        serviceComponentDefnIDs.add(null);
        connectorBindingsIDs.add(null);
        productServiceConfigIDs.add(null);
        productTypeIDs.add(null);
        componentTypeIDs.add(null);

        // now we must iterate through each object type, pull out any references
        // to other configuration objects and ensure that those objects were
        // in the collection of objects passed into the method.
        Iterator iterator = deployedComponents.iterator();
        while (iterator.hasNext()) {
            DeployedComponent deployedComponent = (DeployedComponent) iterator.next();
            if(!vmComponentDefnIDs.contains(deployedComponent.getVMComponentDefnID())){
//            LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "The VMComponentDefnID " + deployedComponent.getVMComponentDefnID() + " was not found in the list of VMCompoentDefnIDs: " + vmComponentDefnIDs);

                throwObjectsNotResolvable(deployedComponent, deployedComponent.getVMComponentDefnID(), "vm component"); //$NON-NLS-1$
            }

            if (!serviceComponentDefnIDs.contains(deployedComponent.getServiceComponentDefnID()) &&
            	!connectorBindingsIDs.contains(deployedComponent.getServiceComponentDefnID()) ) {

                throwObjectsNotResolvable(deployedComponent, deployedComponent.getServiceComponentDefnID(), "service component"); //$NON-NLS-1$
            }

            if (!hostIDs.contains(deployedComponent.getHostID())) {
                throwObjectsNotResolvable(deployedComponent, deployedComponent.getHostID(), "host"); //$NON-NLS-1$
            }

            if (!productServiceConfigIDs.contains(deployedComponent.getProductServiceConfigID())) {
                throwObjectsNotResolvable(deployedComponent, deployedComponent.getProductServiceConfigID(), "psc"); //$NON-NLS-1$
            }

            checkComponentTypeID(deployedComponent, componentTypeIDs, productTypeIDs);
        }

        iterator = resources.iterator();
        while (iterator.hasNext()) {
            SharedResource defn = (SharedResource)iterator.next();
            checkComponentTypeID(defn, componentTypeIDs);
        }


        iterator = connectionPools.iterator();
        while (iterator.hasNext()) {
            ResourceDescriptor defn = (ResourceDescriptor)iterator.next();
            checkComponentTypeID(defn, componentTypeIDs);
            checkConfigurationID(defn, configurationIDs);
        }

        iterator = connectorBindings.iterator();
        while (iterator.hasNext()) {
            ConnectorBinding defn = (ConnectorBinding)iterator.next();
            checkComponentTypeID(defn, componentTypeIDs);
        }



        iterator = serviceComponentDefns.iterator();
        while (iterator.hasNext()) {
            ServiceComponentDefn defn = (ServiceComponentDefn)iterator.next();
            checkComponentTypeID(defn, componentTypeIDs, productTypeIDs);
            checkConfigurationID(defn, configurationIDs);
        }

        iterator = vmComponentDefns.iterator();
        while (iterator.hasNext()) {
            VMComponentDefn defn = (VMComponentDefn)iterator.next();
            checkComponentTypeID(defn, componentTypeIDs, productTypeIDs);
            checkConfigurationID(defn, configurationIDs);
        }


        iterator = productServiceConfigs.iterator();
        while (iterator.hasNext()) {
            ProductServiceConfig config = (ProductServiceConfig)iterator.next();
            
            // psc's component types are based on products, not actual component types
            checkPSCProductTypeID(config, productTypeIDs);
            
//          checkComponentTypeID(config, componentTypeIDs, productTypeIDs);
            checkConfigurationID(config, configurationIDs);
            Iterator iter = config.getServiceComponentDefnIDs().iterator();
            while (iter.hasNext()) {
                Object obj = iter.next();
 //           ComponentDefnID id = (ComponentDefnID) obj;
//            System.out.println("PSC ID: " + id.getFullName() + " class " + id.getClass().getName());

                if (serviceComponentDefnIDs.contains(obj) ||
                	connectorBindingsIDs.contains(obj) ) {
                } else {
                    throwObjectsNotResolvable(obj, config, "service component"); //$NON-NLS-1$
                }
            }
        }

        iterator = componentTypes.iterator();
        while (iterator.hasNext()) {
            ComponentType type = (ComponentType)iterator.next();
            checkForComponentTypeID(type.getSuperComponentTypeID(), componentTypeIDs);
            checkForProductTypeID(type, type.getParentComponentTypeID(), productTypeIDs, componentTypeIDs);
        }

//        iterator = productTypes.iterator();
//        while (iterator.hasNext()) {
//            ProductType type = (ProductType)iterator.next();
//
//            checkForComponentTypeID(type.getSuperComponentTypeID(), componentTypeIDs, productTypeIDs);
//            checkForComponentTypeID(type.getParentComponentTypeID(), componentTypeIDs, productTypeIDs);
//            Iterator iter = type.getComponentTypeIDs().iterator();
//            while (iter.hasNext()) {
//                ComponentTypeID id = (ComponentTypeID)iter.next();
//                checkForComponentTypeID(id, componentTypeIDs, productTypeIDs);
//            }
//        }

        iterator  = configurations.iterator();
        while (iterator.hasNext()) {
            Configuration config = (Configuration)iterator.next();
            checkComponentTypeID(config, componentTypeIDs, productTypeIDs);
        }

        iterator = hosts.iterator();
        while (iterator.hasNext()) {
            Host host = (Host)iterator.next();
            checkComponentTypeID(host, componentTypeIDs, productTypeIDs);
        }

//***        LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Configuration objects resolved properly.");

    }

    private void throwObjectsNotResolvable(Object referencingObject, Object referencedObject, String type) throws ConfigObjectsNotResolvableException{

		String msg = CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0018, new Object[]
				{referencingObject, type, referencedObject} );
        ConfigObjectsNotResolvableException e = new ConfigObjectsNotResolvableException(ErrorMessageKeys.CONFIG_ERR_0018, msg, referencingObject);

        throw e;
    }

    private void throwObjectsNotResolvable(Object referencingObject, String type) throws ConfigObjectsNotResolvableException{
		String msg = CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0019, new Object[]
				{referencingObject, type} );


        ConfigObjectsNotResolvableException e = new ConfigObjectsNotResolvableException(ErrorMessageKeys.CONFIG_ERR_0019, msg, referencingObject);

        throw e;
    }


//    private void throwConfigObjectsNotResolvableExceptionx(Object referencingObject, Object referencedObject) throws ConfigObjectsNotResolvableException{
//		String msg = CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0020, new Object[]
//				{referencingObject, referencedObject} );
//
//
//        ConfigObjectsNotResolvableException e = new ConfigObjectsNotResolvableException(msg);
//
//        throw e;
//    }

    private void checkComponentTypeID(ComponentObject object, List componentTypeIDs, List productTypeIDs) throws ConfigObjectsNotResolvableException{
        if (!(componentTypeIDs.contains(object.getComponentTypeID())||productTypeIDs.contains(object.getComponentTypeID()))) {
 //           LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "The ComponentTypeID: " + object.getComponentTypeID() + " was not found in the list of ComponentTypeIDs: " + componentTypeIDs + " or the list of Product TypeIDs: " + productTypeIDs);
 		throwObjectsNotResolvable(object, object.getComponentTypeID(), "component type"); //$NON-NLS-1$
//            throwConfigObjectsNotResolvableException(object, object.getComponentTypeID());
        }
    }

    private void checkConfigurationID(ComponentDefn defn, List configurationIDs) throws ConfigObjectsNotResolvableException {
        if (!configurationIDs.contains(defn.getConfigurationID())) {
 		throwObjectsNotResolvable(defn, defn.getConfigurationID(), "configuration object"); //$NON-NLS-1$

//            throwConfigObjectsNotResolvableException(defn, defn.getConfigurationID());
        }
    }

    /**
     * Used when product types ids are not part of validation - i.e., ResourceDescriptors
     */
    private void checkComponentTypeID(ComponentObject object, List componentTypeIDs) throws ConfigObjectsNotResolvableException{
        if (!(componentTypeIDs.contains(object.getComponentTypeID()) )) {
 //***           LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "The ComponentTypeID: " + object.getComponentTypeID() + " was not found in the list of ComponentTypeIDs: " + componentTypeIDs );
 		throwObjectsNotResolvable(object, object.getComponentTypeID(), "component type"); //$NON-NLS-1$

//            throwConfigObjectsNotResolvableException(object, object.getComponentTypeID());
        }
    }
    
    /**
     * used to validate PSC's  
     * @param object
     * @param productTypeIDs
     * @throws ConfigObjectsNotResolvableException
     * @since 4.2
     */
    private void checkPSCProductTypeID(ProductServiceConfig object, List productTypeIDs) throws ConfigObjectsNotResolvableException{
        if (!(productTypeIDs.contains(object.getComponentTypeID()))) {
 //           LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "The ComponentTypeID: " + object.getComponentTypeID() + " was not found in the list of ComponentTypeIDs: " + componentTypeIDs + " or the list of Product TypeIDs: " + productTypeIDs);
        throwObjectsNotResolvable(object, object.getComponentTypeID(), "component type"); //$NON-NLS-1$
//            throwConfigObjectsNotResolvableException(object, object.getComponentTypeID());
        }
    }    


    private void checkForComponentTypeID(ComponentTypeID id, List componentTypeIDs) throws ConfigObjectsNotResolvableException{

        if (id!=null) {
            if (!(componentTypeIDs.contains(id))) {
			 		throwObjectsNotResolvable(id, "component"); //$NON-NLS-1$

//                throwConfigObjectsNotResolvableException(id, id);
            }

        }

    }
    
    private void checkForProductTypeID(ComponentType type, ComponentTypeID parentid, List productTypeIDs,  List componentTypeIDs) throws ConfigObjectsNotResolvableException{

        if (parentid!=null) {
            if (type.isOfTypeConnector()) {
                // if connector, must have a valid product type parent
                if (!(productTypeIDs.contains(parentid) )) {
                        throwObjectsNotResolvable(parentid, "product type"); //$NON-NLS-1$
    
    //                throwConfigObjectsNotResolvableException(id, id);
                }
            } 
        }

    }    

    protected List[] segregateConfigurationObjects(Collection collection) {


        List[] lists = new ArrayList[NUMBER_OF_LISTS];

        Assertion.isNotNull(collection);
        ArrayList componentTypes = new ArrayList();
        ArrayList hosts = new ArrayList();
        ArrayList deployedComponents = new ArrayList();
        ArrayList serviceComponentDefns = new ArrayList();
        ArrayList vmComponentDefns = new ArrayList();
        ArrayList productServiceConfigs = new ArrayList();
        ArrayList configurations = new ArrayList();
        ArrayList productTypes = new ArrayList();
        ArrayList connPools = new ArrayList();
        ArrayList resources = new ArrayList();
        ArrayList bindings = new ArrayList();


        ArrayList componentTypeIDs = new ArrayList();
        ArrayList hostIDs = new ArrayList();
        ArrayList deployedComponentIDs = new ArrayList();
        ArrayList serviceComponentDefnIDs = new ArrayList();
        ArrayList vmComponentDefnIDs = new ArrayList();
        ArrayList productServiceConfigIDs = new ArrayList();
        ArrayList configurationIDs = new ArrayList();
        ArrayList productTypeIDs = new ArrayList();

        ArrayList bindingIDs = new ArrayList();


        // here we segregate the configuration objects by type so that we can
        // determine what references they may have to other configuration objects
        // we also set up lists of ID's as a convenience so that we can use
        // contains() to check to see if an id is resolvable within the collection
        Iterator iterator = collection.iterator();
        while (iterator.hasNext()) {
            Object obj = iterator.next();

          // product types need to go before component type
        if (obj instanceof ProductType) {

            productTypes.add(obj);
            productTypeIDs.add(((ProductType)obj).getID());              

            } else if (obj instanceof ComponentType) {

                    componentTypes.add(obj);
                    componentTypeIDs.add(((BaseObject)obj).getID());
            }else if(obj instanceof ComponentObject) {
                if (obj instanceof Host) {
                    hosts.add(obj);
                    hostIDs.add(((BaseObject)obj).getID());
                }else if(obj instanceof DeployedComponent) {
                    deployedComponents.add(obj);
                    deployedComponentIDs.add(((BaseObject)obj).getID());
                }else if(obj instanceof Configuration) {
                    configurations.add(obj);
                    configurationIDs.add(((BaseObject)obj).getID());

                }else if(obj instanceof SharedResource) {
                	resources.add(obj);
                }else if(obj instanceof ConnectorBinding) {
                	bindings.add(obj);
                	bindingIDs.add(((BaseObject)obj).getID());

                }else if(obj instanceof ComponentDefn) {
                    if (obj instanceof ProductServiceConfig) {
                        productServiceConfigs.add(obj);
                        productServiceConfigIDs.add(((BaseObject)obj).getID());
                    }else if(obj instanceof ServiceComponentDefn) {
                        serviceComponentDefns.add(obj);
                        serviceComponentDefnIDs.add(((BaseObject)obj).getID());
                    }else if(obj instanceof VMComponentDefn) {
                        vmComponentDefns.add(obj);
                        vmComponentDefnIDs.add(((BaseObject)obj).getID());
                    }else if(obj instanceof ResourceDescriptor) {
	                     connPools.add(obj);
//    	                 connPoolIDs.add(((BaseObject)obj).getID());

                    } else {
//                        ComponentDefn cd = (ComponentDefn) obj;
//***                        LogManager.logDetail(LogCommonConstants.CTX_CONFIG, "The ComponentDefn: " + cd.getFullName() + " could not be categorized as a Standard Configuration Object and will be ignored. Its class type is: " + cd.getClass());
                    }
                } else {

//                        ComponentObject co = (ComponentObject) obj;
//***                        LogManager.logDetail(LogCommonConstants.CTX_CONFIG, "The ComponentObject: " + co.getFullName() + " could not be categorized as a Standard Configuration Object and will be ignored. Its class type is: " + co.getClass());

                }
            }else {
//***                LogManager.logDetail(LogCommonConstants.CTX_CONFIG, "The Object: " + obj + " could not be categorized as a Standard Configuration Object and will be ignored. Its class type is: " + obj.getClass());
            }
        }

        lists[CONFIGURATIONS_INDEX] = configurations;
        lists[PRODUCT_TYPES_INDEX] = productTypes;
        lists[HOSTS_INDEX] = hosts;
        lists[DEPLOYED_COMPONENTS_INDEX] = deployedComponents;
        lists[SERVICE_COMPONENT_DEFNS_INDEX] = serviceComponentDefns;
        lists[VM_COMPONENT_DEFNS_INDEX] = vmComponentDefns;
        lists[COMPONENT_TYPES_INDEX] = componentTypes;
        lists[PRODUCT_SERVICE_CONFIGS_INDEX] = productServiceConfigs;

        lists[CONFIGURATION_IDS_INDEX] = configurationIDs;
        lists[PRODUCT_TYPE_IDS_INDEX] = productTypeIDs;
        lists[HOST_IDS_INDEX] = hostIDs;
        lists[DEPLOYED_COMPONENT_IDS_INDEX] = deployedComponentIDs;
        lists[SERVICE_COMPONENT_DEFN_IDS_INDEX] = serviceComponentDefnIDs;
        lists[VM_COMPONENT_DEFN_IDS_INDEX] = vmComponentDefnIDs;
        lists[COMPONENT_TYPE_IDS_INDEX] = componentTypeIDs;
        lists[PRODUCT_SERVICE_CONFIG_IDS_INDEX] = productServiceConfigIDs;

        lists[CONNECTION_POOL_CONFIGS_INDEX] = connPools;

        lists[RESOURCES_INDEX] = resources;
        lists[CONNECTORS_INDEX] = bindings;
      	lists[CONNECTORS_IDS_INDEX] = bindingIDs;


        return lists;
    }

    protected XMLReaderWriter getXMLReaderWriter() {
        if (readerWriter == null) {
            readerWriter = new XMLReaderWriterImpl();
        }
        return readerWriter;
    }

    protected abstract XMLHelper getXMLHelper();

//    protected abstract Properties createHeaderProperties(Properties props) ;
    
//    {
//        Properties defaultProperties = new Properties();
////        defaultProperties.setProperty(ConfigurationPropertyNames.DOCUMENT_TYPE_VERSION, XML_DOCUMENT_TYPE_VERSION);
//        defaultProperties.setProperty(ConfigurationPropertyNames.USER_CREATED_BY, DEFAULT_USER_CREATED_BY);
//        // the properties passed in by the user override those put in by this
//        // method.
//        if (props!=null) {
//            defaultProperties.putAll(props);
//        }
//        
//        defaultProperties.setProperty(ConfigurationPropertyNames.METAMATRIX_SYSTEM_VERSION, MetaMatrixProductNames.VERSION_NUMBER);
//        defaultProperties.setProperty(ConfigurationPropertyNames.TIME, DateUtil.getCurrentDateAsString());
//        defaultProperties.setProperty(ConfigurationPropertyNames., DEFAULT_USER_CREATED_BY);
//       
//        
//        return defaultProperties;
//    }



}
