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
import com.metamatrix.common.config.api.AuthenticationProvider;
import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentObject;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.config.model.ConfigurationModelContainerImpl;
import com.metamatrix.common.config.util.ConfigObjectsNotResolvableException;
import com.metamatrix.common.config.util.ConfigurationPropertyNames;
import com.metamatrix.common.config.util.InvalidConfigurationElementException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.namedobject.BaseObject;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.common.xml.XMLReaderWriter;
import com.metamatrix.common.xml.XMLReaderWriterImpl;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.ReflectionHelper;

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
class ConfigurationImportExport implements ConfigurationPropertyNames {
	private static final String CONFIG_MODEL_CLASS = ConfigurationModelContainerImpl.class.getName(); 
	private XMLHelperImpl xmlHelper = null;

	
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
    static final int HOSTS_INDEX = 2;
    static final int DEPLOYED_COMPONENTS_INDEX = 3;
    static final int SERVICE_COMPONENT_DEFNS_INDEX = 4;
    static final int VM_COMPONENT_DEFNS_INDEX = 5;
    static final int COMPONENT_TYPES_INDEX = 6;
    static final int CONNECTION_POOL_CONFIGS_INDEX = 16;
    static final int RESOURCES_INDEX = 18;
    static final int CONNECTORS_INDEX = 19;


    static final int CONFIGURATION_IDS_INDEX = 8;
    static final int HOST_IDS_INDEX = 10;
    static final int DEPLOYED_COMPONENT_IDS_INDEX = 11;
    static final int SERVICE_COMPONENT_DEFN_IDS_INDEX = 12;
    static final int VM_COMPONENT_DEFN_IDS_INDEX = 13;
    static final int COMPONENT_TYPE_IDS_INDEX = 14;
    static final int CONNECTORS_IDS_INDEX = 20;


    static final int NUMBER_OF_LISTS = 21;
    
    public ConfigurationImportExport() {
    	getXMLHelper();
    }

    public void exportComponentType(OutputStream stream, ComponentType type,
                     Properties props) throws IOException {


        Assertion.isNotNull(type);
        Assertion.isNotNull(stream);

        Element root = xmlHelper.createRootConfigurationDocumentElement();

        // create a new Document with a root element
        Document doc = new Document(root);

        // add the header element
        root = XMLHelperUtil.addHeaderElement(root, props);

        Element componentTypesElement = xmlHelper.createComponentTypesElement();
        root.addContent(componentTypesElement);
        componentTypesElement.addContent(xmlHelper.createComponentTypeElement(type));

        getXMLReaderWriter().writeDocument(doc, stream);
    }
    
    public void exportComponentTypes(OutputStream stream, ComponentType[] types,
                                    Properties props) throws IOException {


           Assertion.isNotNull(types);
           Assertion.isNotNull(stream);


           Element root = xmlHelper.createRootConfigurationDocumentElement();

           // create a new Document with a root element
           Document doc = new Document(root);

           // add the header element
           root = XMLHelperUtil.addHeaderElement(root, props);

           Element componentTypesElement = xmlHelper.createComponentTypesElement();
           root.addContent(componentTypesElement);
           
           int s = types.length;

           for (int i = 0; i<s; i++) {
               ComponentType type = types[i];
    
               componentTypesElement.addContent(xmlHelper.createComponentTypeElement(type));

           }
           
           getXMLReaderWriter().writeDocument(doc, stream);
       }
    
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


         Assertion.isNotNull(configurationObjects);
         Assertion.isNotNull(stream);


         // this will divide the configuration objects by their type
         // so that we can create an XML doc and put the objects in their proper
         // categories.
         List[] lists = segregateConfigurationObjects(configurationObjects);
         
         // this will throw ConfigObjectsNotResolvableException if the collection
         // of configuration objects are not self containing.
         resolveConfigurationObjects(lists);
         
         ConfigurationModelContainer cmc=null;
         try {
             Collection parms = new ArrayList(1);
             parms.add(configurationObjects);
             cmc = (ConfigurationModelContainer) ReflectionHelper.create(CONFIG_MODEL_CLASS, parms, this.getClass().getClassLoader());
        } catch (MetaMatrixCoreException err) {
            throw new IOException(err.getMessage());
        }


         Element root = xmlHelper.createRootConfigurationDocumentElement();

         // create a new Document with a root element
         final Document doc = new Document(root);

         // add the header element
         root = XMLHelperUtil.addHeaderElement(root, props);         

         final Configuration config = cmc.getConfiguration();
         Element configElement = xmlHelper.createConfigurationElement(config);
         
         root.addContent(configElement);
         
         try {
             Iterator hostIt=cmc.getHosts().iterator();
             while(hostIt.hasNext()) {
                 Host h = (Host) hostIt.next();
                 final Element hostElement = xmlHelper.createHostElement(h); 
                 configElement.addContent(hostElement);
                 
                 Iterator vmsIt=cmc.getConfiguration().getVMsForHost((HostID) h.getID()).iterator();
//                 getDeployedVMsForHost((HostID) h.getID()).iterator();
                 while(vmsIt.hasNext()) {
                     VMComponentDefn vm = (VMComponentDefn) vmsIt.next();
                     final Element vmElement = xmlHelper.createProcessElement(vm);
                     hostElement.addContent(vmElement);
                     
                     Map pscSvcMap = new HashMap(10);
                     Collection depsvcs = cmc.getConfiguration().getDeployedServicesForVM(vm);
                     Iterator svcIt = depsvcs.iterator();
                     while(svcIt.hasNext()) {
                         DeployedComponent dc = (DeployedComponent) svcIt.next();
                        final Element svcElement = xmlHelper.createDeployedServiceElement(dc);
                         vmElement.addContent(svcElement);
                     }
                 
                 
                 } // end of vms
                 
             } // end of host
         } catch(Exception e) {
             throw new ConfigObjectsNotResolvableException(e, "Error exporting configuration"); //$NON-NLS-1$
         }      
      
      Element providersElement = xmlHelper.createAuthenticationProviderElement();
      root.addContent(providersElement);

      Collection providers = cmc.getConfiguration().getAuthenticationProviders();
      if (providers != null && providers.size() > 0) {
           Iterator iterator = providers.iterator();
           while (iterator.hasNext()) {
        	   AuthenticationProvider provider = (AuthenticationProvider)iterator.next();
//               LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Found ProductType named: " + type + " in list of configuration objects to export.");
               Element resourceElement = xmlHelper.createAuthenticationProviderElement(provider);
               providersElement.addContent(resourceElement);
           }
       }       
      
         
      Element connectorsElement = xmlHelper.createConnectorBindingsElement();
      root.addContent(connectorsElement);
         

       Collection bindings = cmc.getConfiguration().getConnectorBindings();
       if (bindings != null && bindings.size() > 0) {
           Iterator iterator = bindings.iterator();

               while (iterator.hasNext()) {
                   ConnectorBinding connector = (ConnectorBinding)iterator.next();
                   Element connElement = xmlHelper.createConnectorBindingElement(connector, true);
                   connectorsElement.addContent(connElement);
               }

       }
       
       Element servicessElement = xmlHelper.createServiceComponentDefnsElement();
       root.addContent(servicessElement);
          

        Collection svcdefns = cmc.getConfiguration().getServiceComponentDefns();
        if (svcdefns != null && svcdefns.size() > 0) {
            Iterator iterator = svcdefns.iterator();

                while (iterator.hasNext()) {
                    ServiceComponentDefn svc = (ServiceComponentDefn)iterator.next();
                    Element svcElement = xmlHelper.createServiceComponentDefnElement(svc);
                    servicessElement.addContent(svcElement);
                }

        }       
       
       Element resourcesElement = xmlHelper.createSharedResourcesElement();
       root.addContent(resourcesElement);

       Collection sharedResources = cmc.getResources();
       if (sharedResources != null  && sharedResources.size() > 0) {
           
           Iterator iterator = sharedResources.iterator();
             while (iterator.hasNext()) {
                 SharedResource resource = (SharedResource)iterator.next();
                  Element resourceElement = xmlHelper.createSharedResourceElement(resource);
                 resourcesElement.addContent(resourceElement);
             }
       }       
      
       
       Element componentTypesElement = xmlHelper.createComponentTypesElement();
       root.addContent(componentTypesElement);
       
      Map compTypes = cmc.getComponentTypes();
      if (compTypes != null && compTypes.size() > 0) {
     
             Iterator iterator = compTypes.values().iterator();

             while (iterator.hasNext()) {
                 ComponentType componentType = (ComponentType)iterator.next();
                 Element componentTypeElement = xmlHelper.createComponentTypeElement(componentType);
    //             LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Found ComponentType named: " + componentType + " in list of configuration objects to export.");
    
                 componentTypesElement.addContent(componentTypeElement);
             }
      }
         

         getXMLReaderWriter().writeDocument(doc, stream);
         stream.close();

         LogManager.logInfo(LogConstants.CTX_CONFIG, CommonPlugin.Util.getString("MSG.003.001.0003", cmc.getConfigurationID().getFullName())); //$NON-NLS-1$
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
//        XMLHelper helper = getXMLHelper();
//
//        Element root = helper.createRootConfigurationDocumentElement();
//
//        // create a new Document with a root element
//        Document doc = new Document(root);
//        
//        root = XMLHelperUtil.addHeaderElement(root, props);
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
//    }

    
    protected Collection createConnectorBindings(ConfigurationID configurationID, Element root, ConfigurationObjectEditor editor, boolean importExistingBinding)
    	throws IOException, ConfigObjectsNotResolvableException,
                     InvalidConfigurationElementException {
        Element connectorsElement = root.getChild(XMLConfig_ElementNames.Configuration.ConnectorComponents.ELEMENT);
		if (connectorsElement == null) {
			return Collections.EMPTY_LIST;
		}

       List connectorBindings = connectorsElement.getChildren(XMLConfig_ElementNames.Configuration.ConnectorComponents.ConnectorComponent.ELEMENT);

	   List connObjects = new ArrayList(connectorBindings.size());
	   if (connectorBindings != null) {
	        Iterator iterator = connectorBindings.iterator();
	        while (iterator.hasNext()) {
	            Element connElement = (Element)iterator.next();
                ConnectorBinding conn = getXMLHelper().createConnectorBinding(configurationID, connElement, editor, null, importExistingBinding);
	            connObjects.add(conn);
	        }
		}

    	return connObjects;
    }
    
    protected ConnectorBinding createConnectorBinding(ConfigurationID configurationID, Element root, ConfigurationObjectEditor editor, String name, boolean isImportConfig) 
    	throws IOException, ConfigObjectsNotResolvableException,
                     InvalidConfigurationElementException {
        Element connectorsElement = root.getChild(XMLConfig_ElementNames.Configuration.ConnectorComponents.ELEMENT);
		if (connectorsElement == null) {
			return null;
		}

       List connectorBindings = connectorsElement.getChildren(XMLConfig_ElementNames.Configuration.ConnectorComponents.ConnectorComponent.ELEMENT);

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


    protected ComponentType createComponentType(Element root, ConfigurationObjectEditor editor, String name) throws InvalidConfigurationElementException{

        Element componentTypesElement = root.getChild(XMLConfig_ElementNames.ComponentTypes.ELEMENT);

        if (componentTypesElement == null) {
            throw new InvalidConfigurationElementException(ErrorMessageKeys.CONFIG_ERR_0008, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0008,XMLConfig_ElementNames.ComponentTypes.ELEMENT));
        }

        Element componentTypeElement = componentTypesElement.getChild(XMLConfig_ElementNames.ComponentTypes.ComponentType.ELEMENT);

        if (componentTypeElement == null) {
            throw new InvalidConfigurationElementException(ErrorMessageKeys.CONFIG_ERR_0008, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0008,XMLConfig_ElementNames.ComponentTypes.ComponentType.ELEMENT));
        }

        return xmlHelper.createComponentType(componentTypeElement, editor, name, true);
    }
    
    
    protected Collection createComponentTypes(Element root, ConfigurationObjectEditor editor) throws InvalidConfigurationElementException{

        Element componentTypesElement = root.getChild(XMLConfig_ElementNames.ComponentTypes.ELEMENT);

        if (componentTypesElement == null) {
            return Collections.EMPTY_LIST;
        }
                

        // Make a copy of the lists that we intend to change so that we don't affect JDOM's list (which changes the underlying structure)
        List componentTypes = new ArrayList(componentTypesElement.getChildren(XMLConfig_ElementNames.ComponentTypes.ComponentType.ELEMENT));
  
        List connObjects = null;
        
        if (componentTypes != null) {
            connObjects = new ArrayList(componentTypes.size());
        
            getXMLHelper().orderComponentTypeElementList(componentTypes);
            Iterator iterator = componentTypes.iterator();
            while (iterator.hasNext()) {
                Element connElement = (Element)iterator.next();
                ComponentType type = getXMLHelper().createComponentType(connElement, editor, null, true);
                connObjects.add(type);                
                
            }
        } else {
            return Collections.EMPTY_LIST; 
        }

        return connObjects;
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

        // here we ensure that this is a Connector and that the given component
        // type resolves to the given ServiceComponentDefn.
        resolveConnector(type, defn);

        Element root = xmlHelper.createRootConfigurationDocumentElement();

        // create a new Document with a root element
        Document doc = new Document(root);

        root = XMLHelperUtil.addHeaderElement(root, props);

        Element componentTypesElement = xmlHelper.createComponentTypesElement();
        root.addContent(componentTypesElement);
        componentTypesElement.addContent(xmlHelper.createComponentTypeElement(type));

        Element connectorsElement = xmlHelper.createConnectorBindingsElement();
        root.addContent(connectorsElement);
        Element connElement = xmlHelper.createConnectorBindingElement(defn, false );
        connectorsElement.addContent(connElement);

        getXMLReaderWriter().writeDocument(doc, stream);
    }
    
    
    public void exportConnectorBindings(OutputStream stream, ConnectorBinding[] bindings, ComponentType[] types, Properties props) throws IOException, ConfigObjectsNotResolvableException  {
        Assertion.isNotNull(bindings);
        Assertion.isNotNull(types);
        Assertion.isNotNull(stream);
       

        Element root = xmlHelper.createRootConfigurationDocumentElement();

        // create a new Document with a root element
        Document doc = new Document(root);

        root = XMLHelperUtil.addHeaderElement(root, props);

        exportConnectorBindings(bindings, types, root);
    
        getXMLReaderWriter().writeDocument(doc, stream);
       
    }
 
    /** 
     * Add connector bindings and component types (connector types) under
     * a common root element.
     * @param bindings
     * @param types
     * @param helper
     * @param root
     * @since 4.2
     */
    private void exportConnectorBindings(ConnectorBinding[] bindings,
                                         ComponentType[] types,
                                         Element root) {
        int s = bindings.length;
        List ts = new ArrayList(s);
        
        Element componentTypesElement = xmlHelper.createComponentTypesElement();
        root.addContent(componentTypesElement);


        Element connectorsElement = xmlHelper.createConnectorBindingsElement();
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
 
            if (type != null && !ts.contains(type.getFullName())) {
                componentTypesElement.addContent(xmlHelper.createComponentTypeElement(type));
                ts.add(type.getFullName());
            }
                
            Element connElement = xmlHelper.createConnectorBindingElement(cb, false );
            connectorsElement.addContent(connElement);              

        }
    }
    
    
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
     public Collection importConfigurationObjects(Element root,
                      ConfigurationObjectEditor editor, String name)
                      throws IOException, ConfigObjectsNotResolvableException,
                      InvalidConfigurationElementException {
         Assertion.isNotNull(root);
         Assertion.isNotNull(editor);

         ArrayList configurationObjects = new ArrayList();

         // get the first configuration element from the document
         Element configurationElement = root.getChild(XMLConfig_ElementNames.Configuration.ELEMENT);

         // if there is no configuration element under the Configurations element
         // then we do not know how to import one.
         if (configurationElement == null) {
             throw new ConfigObjectsNotResolvableException(ErrorMessageKeys.CONFIG_ERR_0004, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0004));
         }

        
         
         // NOTE the ordering of the following iterations is very important
         // as the actions to create the configuration objects are being created
         // within the ConfigurationObjectEditor.  If these are created out of
         // order, a Configuration object that references another configuration
         // object may be created before the configuration object that it references.
         // This will work fine until the actions are committed.
         Configuration config = xmlHelper.createConfiguration(configurationElement, editor, name);

         ConfigurationID configID = (ConfigurationID) config.getID();
         configurationObjects.add(config);
         
         xmlHelper.addProperties(configurationElement, config, editor);



         
         Collection componentTypes = createComponentTypes(root, editor);
          Map componentTypeMap = new HashMap();
         if (componentTypes != null) {
             for (Iterator it=componentTypes.iterator(); it.hasNext();) {
                 ComponentType type = (ComponentType) it.next();
                 componentTypeMap.put(type.getID(), type); 
                 configurationObjects.add(type);
                 
             }
         }



         Element resourcesElement = root.getChild(XMLConfig_ElementNames.Configuration.Resources.ELEMENT);

         List resources = null;
         if (resourcesElement != null) {
           resources = resourcesElement.getChildren(XMLConfig_ElementNames.Configuration.Resources.Resource.ELEMENT);
         } else {
           resources = Collections.EMPTY_LIST;
         }

         Iterator iterator = resources.iterator();
         while (iterator.hasNext()) {
             Element resourceElement = (Element)iterator.next();
             SharedResource resource = xmlHelper.createSharedResource(resourceElement, editor);
             configurationObjects.add(resource);
         }
          
         Element authprovidersElement =
             root.getChild(XMLConfig_ElementNames.Configuration.AuthenticationProviders.ELEMENT);
         if (authprovidersElement!=null) {
             Collection authproviderElements =
           	  authprovidersElement.getChildren(XMLConfig_ElementNames.Configuration.AuthenticationProviders.Provider.ELEMENT);

             iterator = authproviderElements.iterator();
             while (iterator.hasNext()) {
                 Element resourcePoolElement = (Element)iterator.next();
                 AuthenticationProvider provider =
                     xmlHelper.createAuthenticationProvider(resourcePoolElement, configID, editor);
                 configurationObjects.add(provider);

             }
         }          
         
         
         
         Collection bindings = createConnectorBindings(configID, root, editor, true);
         
         
         Map serviceComponentDefnMap = new HashMap();

         for (Iterator itb=bindings.iterator(); itb.hasNext();) {
           ComponentDefn defn = (ComponentDefn) itb.next();
           serviceComponentDefnMap.put(defn.getID(), defn);
         }

         // this list of servicecomponent defns will be used in the creation
         // of deployed components, we need the actual reference to the
         // servicecomponentdefn to create a deployed version of it...
         Element serviceComponentDefnsElement =
             root.getChild(XMLConfig_ElementNames.Configuration.Services.ELEMENT);
         if (serviceComponentDefnsElement!=null) {
             Collection serviceComponentDefnElements =
                 serviceComponentDefnsElement.getChildren(XMLConfig_ElementNames.Configuration.Services.Service.ELEMENT);

             iterator = serviceComponentDefnElements.iterator() ;
             while (iterator.hasNext()) {
                 Element serviceComponentDefnElement = (Element)iterator.next();
                 ComponentDefn defn =
                     xmlHelper.createServiceComponentDefn(serviceComponentDefnElement, config, editor, null);
	                     serviceComponentDefnMap.put(defn.getID(), defn);
	                  configurationObjects.add(defn);

             }
         }

       // add the bindings to the map that is used by deployment
         configurationObjects.addAll(bindings);

         // retreive the host elements from the document.
         List hostElements = configurationElement.getChildren(XMLConfig_ElementNames.Configuration.Host.ELEMENT);
         Iterator hostiterator = hostElements.iterator();
         Host host = null;
         HostID hostID = null;
         while (hostiterator.hasNext()) {
             Element hostElement = (Element)hostiterator.next();
             
             host = xmlHelper.createHost(hostElement,configID, editor, null);
             hostID = (HostID) host.getID();
             configurationObjects.add(host);
             
             
             List vmElements = hostElement.getChildren(XMLConfig_ElementNames.Configuration.Process.ELEMENT);
             Iterator vmIt = vmElements.iterator();
             while (vmIt.hasNext()) {
                 Element vmElement = (Element)vmIt.next();
                 
                 VMComponentDefn vm = xmlHelper.createProcess(vmElement, configID, hostID, editor, null);
                 
//                 DeployedComponent vmdc = xmlHelper.createDeployedVMComponentDefn(vmElement, configID, hostID, vm.getComponentTypeID(), editor);
                 VMComponentDefnID vmID = (VMComponentDefnID) vm.getID();
                   
                 configurationObjects.add(vm);

                     List svcElements = vmElement.getChildren(XMLConfig_ElementNames.Configuration.DeployedService.ELEMENT);

                     Iterator svcIt = svcElements.iterator();
                     while (svcIt.hasNext()) {
                         Element svcElement = (Element)svcIt.next();
                         // create this only for use to create the deployed service
                         DeployedComponent dc = xmlHelper.createDeployedServiceComponent(svcElement, configID, hostID, vmID, componentTypeMap, editor);
                         configurationObjects.add(dc);
  
                     } // end services
             }  // end vm
            
         } // end host
         
         

//         Map vmComponentDefnMap = new HashMap();
//         Element vmComponentDefnsElement = configurationElement.getChild(XMLConfig_ElementNames.Configurations.Configuration.VMComponentDefns.ELEMENT);
//         if (vmComponentDefnsElement!=null) {
//             Collection vmComponentDefnElements =
//                 vmComponentDefnsElement.getChildren(XMLConfig_ElementNames.Configurations.Configuration.VMComponentDefns.VMComponentDefn.ELEMENT);
//
//             iterator = vmComponentDefnElements.iterator();
//             while (iterator.hasNext()) {
//                 Element vmComponentDefnElement = (Element)iterator.next();
//                 VMComponentDefn vmComponentDefn =
//                     xmlHelper.createVMComponentDefn(vmComponentDefnElement, (ConfigurationID) config.getID() , editor, null);
////                 LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Found VMComponentDefn named: " + vmComponentDefn + " in XML file VMComponentDefns element.");
//                 vmComponentDefnMap.put(vmComponentDefn.getID(), vmComponentDefn);
//                 configurationObjects.add(vmComponentDefn);
//
//             }
//         }
//         Element deployedComponentsElement =
//             configurationElement.getChild(XMLConfig_ElementNames.Configurations.Configuration.DeployedComponents.ELEMENT);
//         if (deployedComponentsElement!=null) {
//             Collection deployedComponentElements =
//                 deployedComponentsElement.getChildren(XMLConfig_ElementNames.Configurations.Configuration.DeployedComponents.DeployedComponent.ELEMENT);
//
//             iterator = deployedComponentElements.iterator();
//             while (iterator.hasNext()) {
//                 Element deployedComponentElement = (Element)iterator.next();
//                 DeployedComponent deployedComponent =
//                     xmlHelper.createDeployedComponent(deployedComponentElement, config, editor, serviceComponentDefnMap, vmComponentDefnMap, componentTypeMap,  null);
////                 LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Found DeployedComponent named: " + deployedComponent + " in XML file DeployedComponents element.");
//                 configurationObjects.add(deployedComponent);
//             }
//         }
         // this will ensure that the actions that exist in the editor are
         // self contained....that there are no references to configuration objects
         // in the configuration objects that are not also contained in the Collection
         resolveConfigurationObjects(configurationObjects);

// ***        I18nLogManager.logInfo(LogCommonConstants.CTX_CONFIG, LogMessageKeys.CONFIG_MSG_0006);

         return configurationObjects;
     }
    

    public ConnectorBinding importConnectorBinding(InputStream stream, ConfigurationObjectEditor editor, String newName)throws IOException, ConfigObjectsNotResolvableException, InvalidConfigurationElementException {
        Assertion.isNotNull(stream);
        Assertion.isNotNull(editor);

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

        return binding;

}

    public Collection importConnectorBindings(Element root, ConfigurationObjectEditor editor, boolean importExistingBinding)throws IOException, ConfigObjectsNotResolvableException, InvalidConfigurationElementException {
        Assertion.isNotNull(root);
        Assertion.isNotNull(editor);


        Collection connectorBindings = createConnectorBindings(Configuration.NEXT_STARTUP_ID, root, editor, importExistingBinding) ;

        return connectorBindings;
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
     public ComponentType importComponentType(Element root,
                                              //InputStream stream,
                  ConfigurationObjectEditor editor, String name)
                  throws IOException, InvalidConfigurationElementException {

         Assertion.isNotNull(editor);


       ComponentType t = createComponentType(root, editor, name);

         return t;
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

      public Collection importComponentTypes(Element root,
                                             //InputStream stream,
                   ConfigurationObjectEditor editor)
                   throws IOException, InvalidConfigurationElementException {
//           Assertion.isNotNull(stream);
           Assertion.isNotNull(editor);


           Collection connectorTypes = createComponentTypes(root, editor) ;

           return connectorTypes;       
     
      }    
    

    public Collection importConnectorBindings(InputStream stream, ConfigurationObjectEditor editor, boolean importExistingBinding)throws IOException, ConfigObjectsNotResolvableException, InvalidConfigurationElementException {
        Assertion.isNotNull(stream);
        Assertion.isNotNull(editor);

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

        Document doc = null;

        try {
            doc = getXMLReaderWriter().readDocument(stream);
        } catch(JDOMException e) {
            throw new IOException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0012));
        }

		String typeName = null;
		String bindingName = null;
		if (newName != null && newName.length > 0) {
			if (newName[0] != null) {
				typeName = newName[0];
			}
			if (newName[1] != null) {
				bindingName = newName[1];
			}
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

        List hosts = lists[HOSTS_INDEX];
        List deployedComponents = lists[DEPLOYED_COMPONENTS_INDEX];
        List serviceComponentDefns = lists[SERVICE_COMPONENT_DEFNS_INDEX];
        List vmComponentDefns = lists[VM_COMPONENT_DEFNS_INDEX];
        List componentTypes = lists[COMPONENT_TYPES_INDEX];
 
        List connectionPools = lists[CONNECTION_POOL_CONFIGS_INDEX];

        List resources = lists[RESOURCES_INDEX];

        List connectorBindings = lists[CONNECTORS_INDEX];

        List configurationIDs = lists[CONFIGURATION_IDS_INDEX];
        List hostIDs = lists[HOST_IDS_INDEX];
        List serviceComponentDefnIDs = lists[SERVICE_COMPONENT_DEFN_IDS_INDEX];
        List vmComponentDefnIDs = lists[VM_COMPONENT_DEFN_IDS_INDEX];
        List componentTypeIDs = lists[COMPONENT_TYPE_IDS_INDEX];

        List connectorBindingsIDs = lists[CONNECTORS_IDS_INDEX];


        // we do this because a DeployedComponent can have a null ServiceComponentDefnID
        // and a null ProductServiceComponentID and still be viable.  This will
        // allow the contains methods called on these Lists later in this method
        // to return true if the ID is null instead of throwing a bogus exception
        serviceComponentDefnIDs.add(null);
        connectorBindingsIDs.add(null);
         componentTypeIDs.add(null);

        // now we must iterate through each object type, pull out any references
        // to other configuration objects and ensure that those objects were
        // in the collection of objects passed into the method.
        Iterator iterator = deployedComponents.iterator();
        while (iterator.hasNext()) {
            DeployedComponent deployedComponent = (DeployedComponent) iterator.next();
            if(!vmComponentDefnIDs.contains(deployedComponent.getVMComponentDefnID())){

                throwObjectsNotResolvable(deployedComponent, deployedComponent.getVMComponentDefnID(), "vm component"); //$NON-NLS-1$
            }

            if (!serviceComponentDefnIDs.contains(deployedComponent.getServiceComponentDefnID()) &&
            	!connectorBindingsIDs.contains(deployedComponent.getServiceComponentDefnID()) ) {

                throwObjectsNotResolvable(deployedComponent, deployedComponent.getServiceComponentDefnID(), "service component"); //$NON-NLS-1$
            }

            if (!hostIDs.contains(deployedComponent.getHostID())) {
                throwObjectsNotResolvable(deployedComponent, deployedComponent.getHostID(), "host"); //$NON-NLS-1$
            }

            checkComponentTypeID(deployedComponent, componentTypeIDs);
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
            checkComponentTypeID(defn, componentTypeIDs);
            checkConfigurationID(defn, configurationIDs);
        }

        iterator = vmComponentDefns.iterator();
        while (iterator.hasNext()) {
            VMComponentDefn defn = (VMComponentDefn)iterator.next();
            checkComponentTypeID(defn, componentTypeIDs);
            checkConfigurationID(defn, configurationIDs);
            
         
        }


        iterator  = configurations.iterator();
        while (iterator.hasNext()) {
            Configuration config = (Configuration)iterator.next();
            checkComponentTypeID(config, componentTypeIDs);
        }

        iterator = hosts.iterator();
        while (iterator.hasNext()) {
            Host host = (Host)iterator.next();
            checkComponentTypeID(host, componentTypeIDs);
        }

    }

    private void throwObjectsNotResolvable(Object referencingObject, Object referencedObject, String type) throws ConfigObjectsNotResolvableException{

		String msg = CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0018, new Object[]
				{referencingObject, type, referencedObject} );
        ConfigObjectsNotResolvableException e = new ConfigObjectsNotResolvableException(ErrorMessageKeys.CONFIG_ERR_0018, msg);

        throw e;
    }


    private void checkConfigurationID(ComponentDefn defn, List configurationIDs) throws ConfigObjectsNotResolvableException {
        if (!configurationIDs.contains(defn.getConfigurationID())) {
        	throwObjectsNotResolvable(defn, defn.getConfigurationID(), "configuration object"); //$NON-NLS-1$
        }
    }

    /**
     * Used when product types ids are not part of validation - i.e., ResourceDescriptors
     */
    private void checkComponentTypeID(ComponentObject object, List componentTypeIDs) throws ConfigObjectsNotResolvableException{
        if (!(componentTypeIDs.contains(object.getComponentTypeID()) )) {
        	throwObjectsNotResolvable(object, object.getComponentTypeID(), "component type"); //$NON-NLS-1$
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
        ArrayList configurations = new ArrayList();
        ArrayList connPools = new ArrayList();
        ArrayList resources = new ArrayList();
        ArrayList bindings = new ArrayList();


        ArrayList componentTypeIDs = new ArrayList();
        ArrayList hostIDs = new ArrayList();
        ArrayList deployedComponentIDs = new ArrayList();
        ArrayList serviceComponentDefnIDs = new ArrayList();
        ArrayList vmComponentDefnIDs = new ArrayList();
        ArrayList configurationIDs = new ArrayList();

        ArrayList bindingIDs = new ArrayList();


        // here we segregate the configuration objects by type so that we can
        // determine what references they may have to other configuration objects
        // we also set up lists of ID's as a convenience so that we can use
        // contains() to check to see if an id is resolvable within the collection
        Iterator iterator = collection.iterator();
        while (iterator.hasNext()) {
            Object obj = iterator.next();

          // product types need to go before component type

            if (obj instanceof ComponentType) {

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
                    if(obj instanceof ServiceComponentDefn) {
                        serviceComponentDefns.add(obj);
                        serviceComponentDefnIDs.add(((BaseObject)obj).getID());
                    }else if(obj instanceof VMComponentDefn) {
                        vmComponentDefns.add(obj);
                        vmComponentDefnIDs.add(((BaseObject)obj).getID());
                    }else if(obj instanceof ResourceDescriptor) {
	                     connPools.add(obj);

                    } 
                } 
            }
        }


        lists[CONFIGURATIONS_INDEX] = configurations;
        lists[HOSTS_INDEX] = hosts;
        lists[DEPLOYED_COMPONENTS_INDEX] = deployedComponents;
        lists[SERVICE_COMPONENT_DEFNS_INDEX] = serviceComponentDefns;
        lists[VM_COMPONENT_DEFNS_INDEX] = vmComponentDefns;
        lists[COMPONENT_TYPES_INDEX] = componentTypes;

        lists[CONFIGURATION_IDS_INDEX] = configurationIDs;
        lists[HOST_IDS_INDEX] = hostIDs;
        lists[DEPLOYED_COMPONENT_IDS_INDEX] = deployedComponentIDs;
        lists[SERVICE_COMPONENT_DEFN_IDS_INDEX] = serviceComponentDefnIDs;
        lists[VM_COMPONENT_DEFN_IDS_INDEX] = vmComponentDefnIDs;
        lists[COMPONENT_TYPE_IDS_INDEX] = componentTypeIDs;

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

    protected XMLHelperImpl getXMLHelper() {
        if (xmlHelper == null) {
            xmlHelper = new XMLHelperImpl();
            
        }
        return xmlHelper;

    }
}
