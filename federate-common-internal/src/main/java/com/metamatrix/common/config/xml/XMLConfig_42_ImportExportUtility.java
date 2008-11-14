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

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.api.AuthenticationProvider;
import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.ProductServiceConfig;
import com.metamatrix.common.config.api.ProductServiceConfigID;
import com.metamatrix.common.config.api.ProductType;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.config.model.ConfigurationModelContainerImpl;
import com.metamatrix.common.config.util.ConfigObjectsNotResolvableException;
import com.metamatrix.common.config.util.InvalidConfigurationElementException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.ReflectionHelper;

/**
* This version of the configuration import/exporter supports the configuration in 4.2.
*/
public class XMLConfig_42_ImportExportUtility extends XMLConfig_Base_ImportExportUtility {

    private static final String CONFIG_MODEL_CLASS = ConfigurationModelContainerImpl.class.getName(); 
    private XMLConfig_42_HelperImpl xmlHelper = null;



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


         // this xmlHelper class contains all of the code to convert configuration
         // objects into JDOM Elements
          getXMLHelper();
         
         // this visitor will visit all of the configuration objects in a
         // configuration collecting their state as JDOM XML Elements
         XMLConfig_42_Visitor visitor = new XMLConfig_42_Visitor(xmlHelper);



         Element root = xmlHelper.createRootConfigurationDocumentElement();

         // create a new Document with a root element
         final Document doc = new Document(root);

         // add the header element
         root = XMLHelperUtil.addHeaderElement(root, props);         
 //        root.addContent(xmlHelper.createHeaderElement(createHeaderProperties(props)));

         final Configuration config = cmc.getConfiguration();
         Element configElement = visitor.visitComponent(config);
         
         root.addContent(configElement);
         
         try {
             Iterator hostIt=cmc.getHosts().iterator();
             while(hostIt.hasNext()) {
                 Host h = (Host) hostIt.next();
                 final Element hostElement = visitor.visitComponent(h); 
                 configElement.addContent(hostElement);
                 
                 Iterator vmsIt=cmc.getConfiguration().getVMsForHost((HostID) h.getID()).iterator();
//                 getDeployedVMsForHost((HostID) h.getID()).iterator();
                 while(vmsIt.hasNext()) {
                     VMComponentDefn vm = (VMComponentDefn) vmsIt.next();
//                     VMComponentDefn vm = cmc.getConfiguration().getVMComponentDefn(vmDC.getVMComponentDefnID());
                     final Element vmElement = visitor.visitComponent(vm);
                     hostElement.addContent(vmElement);
                     
                     Map pscSvcMap = new HashMap(10);
                     Iterator depIt = cmc.getConfiguration().getDeployedServicesForVM(vm).iterator();
                     Collection svcs = null;
                     // group the deployed services by pc for adding to the vm element
                    while(depIt.hasNext()) {
                        
                        DeployedComponent dc = (DeployedComponent) depIt.next();
                        if (!pscSvcMap.containsKey(dc.getProductServiceConfigID())) {
                            svcs = new ArrayList();
                            pscSvcMap.put(dc.getProductServiceConfigID(),svcs);
                        } else {
                            svcs = (Collection) pscSvcMap.get(dc.getProductServiceConfigID());
                        }
                        
                        svcs.add(dc);
                    }
                     
                    
                     Iterator pscIt = pscSvcMap.keySet().iterator();
                     while(pscIt.hasNext()) {
                         ProductServiceConfigID pscID = (ProductServiceConfigID) pscIt.next();
                         ProductServiceConfig psc = cmc.getConfiguration().getPSC(pscID);
                        
                         final Element pscElement = getXMLHelper().createDeployedProductServiceConfigElement(psc);
                         vmElement.addContent(pscElement);
                         
                         Collection depsvcs = (Collection) pscSvcMap.get(pscID);
                         
                         Iterator svcIt = depsvcs.iterator();
                         while(svcIt.hasNext()) {
                             DeployedComponent dc = (DeployedComponent) svcIt.next();
//                             ComponentDefn defn = null;
//                             if (dc.isDeployedService()) {
//                                 defn = cmc.getConfiguration().getServiceComponentDefn(dc.getServiceComponentDefnID());
//                             } else {
//                                 defn = cmc.getConfiguration().getConnectorBinding(dc.getServiceComponentDefnID());
//                                 
//                             }
//                             if (defn == null) {
//                                 throw new ConfigObjectsNotResolvableException( "DeployedComponent " + dc.getID() + " could not find service " + dc.getServiceComponentDefnID(), dc); //$NON-NLS-1$ //$NON-NLS-2$
//                             }
                            final Element svcElement = visitor.visitComponent(dc);
                             pscElement.addContent(svcElement);
                             
                             
                         } // end of svcs
                         
                     } // end of pscs
                     
    //                 Iterator svcIt=cmc.getConfiguration().getDeployedServicesForVM(vmDC.getVMComponentDefnID()).iterator();
    //                 while(svcIt.hasNext()) {
    //                     DeployedComponent svcDc=(DeployedComponent) svcIt.next();
    //                     ComponentDefn cd = null;
    //                     if (svcDc.isDeployedConnector()) {
    //                         cd = cmc.getConfiguration().getConnectorBinding(svcDc.getServiceComponentDefnID());
    //                     } else {
    //                         cd = cmc.getConfiguration().getServiceComponentDefn(svcDc.getServiceComponentDefnID());
    //                     }
    //                     final Element svcElement = visitor.visitComponent(cd);
    //                     vmElement.addContent(svcElement);
    //                 }
                 
                 
                 } // end of vms
                 
             } // end of host
         } catch(Exception e) {
             throw new ConfigObjectsNotResolvableException(e, "Error exporting configuration"); //$NON-NLS-1$
         }
         
         // iterate through the psc objects, if there are any,
         // and create elements for them.
         
        Element pscsElement = xmlHelper.createProductServiceConfigsElement();
        root.addContent(pscsElement);

        Collection pscs = cmc.getConfiguration().getPSCs();
        if (pscs != null && pscs.size() > 0) {
             Iterator iterator = pscs.iterator();
             while (iterator.hasNext()) {
                 ProductServiceConfig type = (ProductServiceConfig)iterator.next();
//                 LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Found ProductType named: " + type + " in list of configuration objects to export.");
                 Element productTypeElement = visitor.visitComponent(type); 
                 pscsElement.addContent(productTypeElement);
             }
         }         
        
       // iterate through the product type objects, if there are any,
       // and create elements for them.
         
      Element productTypesElement = xmlHelper.createProductTypesElement();
      root.addContent(productTypesElement);

      Collection productTypes = cmc.getProductTypes();
      if (productTypes != null && productTypes.size() > 0) {
           Iterator iterator = productTypes.iterator();
           while (iterator.hasNext()) {
               ProductType type = (ProductType)iterator.next();
//               LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Found ProductType named: " + type + " in list of configuration objects to export.");
               Element productTypeElement = visitor.visitComponent(type); 
               productTypesElement.addContent(productTypeElement);
           }
       }     
      
      Element resourcePoolsElement = xmlHelper.createResourcePoolsElement();
      root.addContent(resourcePoolsElement);

      Collection pools = cmc.getConnectionPools();
      if (pools != null && pools.size() > 0) {
           Iterator iterator = pools.iterator();
           while (iterator.hasNext()) {
               ResourceDescriptor type = (ResourceDescriptor)iterator.next();
//               LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Found ProductType named: " + type + " in list of configuration objects to export.");
               Element resourceElement = visitor.visitComponent(type);
               resourcePoolsElement.addContent(resourceElement);
           }
       }      
      
      Element providersElement = xmlHelper.createAuthenticationProviderElement();
      root.addContent(providersElement);

      Collection providers = cmc.getConfiguration().getAuthenticationProviders();
      if (providers != null && providers.size() > 0) {
           Iterator iterator = providers.iterator();
           while (iterator.hasNext()) {
        	   AuthenticationProvider provider = (AuthenticationProvider)iterator.next();
//               LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Found ProductType named: " + type + " in list of configuration objects to export.");
               Element resourceElement = visitor.visitComponent(provider);
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
                   Element connElement = visitor.visitConnectorBindingComponent(connector);
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
                    Element svcElement = visitor.visitComponent(svc);
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
    //             LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Found SharedResource named: " + resource + " in list of configuration objects to export.");
                 Element resourceElement = visitor.visitComponent(resource);
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
                 Element componentTypeElement = visitor.visitComponent(componentType);
    //             LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Found ComponentType named: " + componentType + " in list of configuration objects to export.");
    
                 componentTypesElement.addContent(componentTypeElement);
             }
      }
         

         getXMLReaderWriter().writeDocument(doc, stream);
         stream.close();

         LogManager.logInfo(LogCommonConstants.CTX_CONFIG, CommonPlugin.Util.getString("MSG.003.001.0003", cmc.getConfigurationID().getFullName())); //$NON-NLS-1$
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

//  ***        LogManager.logDetail(LogCommonConstants.CTX_CONFIG, "Importing a Configuration...");


          ArrayList configurationObjects = new ArrayList();

            getXMLHelper();

          // get the first configuration element from the document
          Element configurationElement = root.getChild(XMLConfig_42_ElementNames.Configuration.ELEMENT);

          // if there is no configuration element under the Configurations element
          // then we do not know how to import one.
          if (configurationElement == null) {
              throw new ConfigObjectsNotResolvableException(ErrorMessageKeys.CONFIG_ERR_0004, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0004));
          }

          // add the system properties to the Configuration object
//          Element systemPropertiesElement =
//              configurationElement.getChild(XMLConfig_42_ElementNames.Properties.ELEMENT);
//          if (systemPropertiesElement!=null) {
//              Collection propertyElements =
//                  systemPropertiesElement.getChildren(XMLConfig_42_ElementNames.Properties.Property.ELEMENT);
//          }
          
          
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

          //          Element componentTypesElement  = root.getChild(XMLConfig_42_ElementNames.ComponentTypes.ELEMENT);
//
//          if (componentTypesElement != null) {
//              List componentTypes = componentTypesElement.getChildren(XMLConfig_42_ElementNames.ComponentTypes.ComponentType.ELEMENT);
//    
//    
//              if (componentTypes != null && componentTypes.size() > 0) {
//    
//                  // we must do this to make sure that the componentTypes are created in
//                  // the correct order.
//                  xmlHelper.orderComponentTypeElementList(componentTypes);
//                  Iterator iterator = componentTypes.iterator();
//                  while (iterator.hasNext()) {
//                      Element componentTypeElement = (Element)iterator.next();
//                      ComponentType type =
//                          xmlHelper.createComponentType(componentTypeElement, editor, null, true);
//        //              LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Found ComponentType named: " + type + " in XML file ComponentTypes element.");
//                      configurationObjects.add(type);
//                      componentTypeMap.put(type.getID(), type);
//                  }
//              }
//          }


          Element productTypesElement = root.getChild(XMLConfig_42_ElementNames.ProductTypes.ELEMENT);

          if (productTypesElement != null) {
              
              List productTypes = productTypesElement.getChildren(XMLConfig_42_ElementNames.ProductTypes.ProductType.ELEMENT);
              if (productTypes != null && productTypes.size() > 0) {
                  // Make a copy of the lists that we intend to change so that we don't affect JDOM's list (which changes the underlying structure)
                  productTypes = new ArrayList(productTypes);
                  // we must do this to make sure that the productTypes are created in
                  // the correct order.
                  xmlHelper.orderComponentTypeElementList(productTypes);
                  Iterator iterator = productTypes.iterator();
                  while (iterator.hasNext()) {
                      Element productTypeElement = (Element)iterator.next();
                      ProductType type = xmlHelper.createProductType(productTypeElement, editor, componentTypeMap, null);
        //              LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Found ProductType named: " + type + " in XML file ProductTypes element.");
                      configurationObjects.add(type);
                  }
              }
          }
          


          Element resourcesElement = root.getChild(XMLConfig_42_ElementNames.Resources.ELEMENT);

          List resources = null;
          if (resourcesElement != null) {
            resources = resourcesElement.getChildren(XMLConfig_42_ElementNames.Resources.Resource.ELEMENT);
          } else {
            resources = Collections.EMPTY_LIST;
          }

          Iterator iterator = resources.iterator();
          while (iterator.hasNext()) {
              Element resourceElement = (Element)iterator.next();
              SharedResource resource = xmlHelper.createSharedResource(resourceElement, editor);
//              LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Found Host named: " + host + " in XML file Hosts element.");
              configurationObjects.add(resource);
          }
          
          Map pscMap = new HashMap(1);
          Element productServiceConfigsElement =
              root.getChild(XMLConfig_42_ElementNames.ProductServiceConfigs.ELEMENT);
          if (productServiceConfigsElement!=null) {
              Collection productServiceConfigElements =
                  productServiceConfigsElement.getChildren(XMLConfig_42_ElementNames.ProductServiceConfigs.ProductServiceConfig.ELEMENT);
              pscMap = new HashMap(productServiceConfigElements.size());
              iterator = productServiceConfigElements.iterator();
              while (iterator.hasNext()) {
                  Element productServiceConfigElement = (Element)iterator.next();
                  ProductServiceConfig productServiceConfig =
                      xmlHelper.createProductServiceConfig(productServiceConfigElement, configID, editor, null);
                  configurationObjects.add(productServiceConfig);
                  pscMap.put(productServiceConfig.getName(), productServiceConfig);

              }
          } 
          
//          else {
//
//                Assertion.assertTrue(false, "No ProductServiceConfigs defined in the ConfigurationDocument"); //$NON-NLS-1$
//
//          }
          
          Element resourcePoolsElement =
              root.getChild(XMLConfig_42_ElementNames.ResourcePools.ELEMENT);
          if (resourcePoolsElement!=null) {
              Collection resourcePoolsElements =
                  resourcePoolsElement.getChildren(XMLConfig_42_ElementNames.ResourcePools.ResourcePool.ELEMENT);

              iterator = resourcePoolsElements.iterator();
              while (iterator.hasNext()) {
                  Element resourcePoolElement = (Element)iterator.next();
                  ResourceDescriptor pool =
                      xmlHelper.createResourcePool(resourcePoolElement, configID, editor);
                  configurationObjects.add(pool);

              }
          }
          
          Element authprovidersElement =
              root.getChild(XMLConfig_42_ElementNames.AuthenticationProviders.ELEMENT);
          if (authprovidersElement!=null) {
              Collection authproviderElements =
            	  authprovidersElement.getChildren(XMLConfig_42_ElementNames.AuthenticationProviders.Provider.ELEMENT);

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
              root.getChild(XMLConfig_42_ElementNames.ServiceComponentDefns.ELEMENT);
          if (serviceComponentDefnsElement!=null) {
              Collection serviceComponentDefnElements =
                  serviceComponentDefnsElement.getChildren(XMLConfig_42_ElementNames.ServiceComponentDefns.ServiceComponentDefn.ELEMENT);

              iterator = serviceComponentDefnElements.iterator() ;
              while (iterator.hasNext()) {
                  Element serviceComponentDefnElement = (Element)iterator.next();
                  ComponentDefn defn =
                      xmlHelper.createServiceComponentDefn(serviceComponentDefnElement, config, editor, null);
//                  LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Found ServiceComponentDefn named: " + defn + " in XML file ServiceComponentDefns element.");
                 // don't include the ResourceDescriptors
//                  if (defn instanceof ServiceComponentDefn) {
                     serviceComponentDefnMap.put(defn.getID(), defn);
 //                 }
                  configurationObjects.add(defn);

              }
          }
          
          
          
          
//          if (resources.isEmpty()) {
//              Assertion.assertTrue(false, "No Resources to import"); //$NON-NLS-1$
//          }
//          if (hosts.isEmpty()) {
//              Assertion.assertTrue(false, "No Hosts to import"); //$NON-NLS-1$
//          }   
//          if (componentTypes.isEmpty()) {
//              Assertion.assertTrue(false, "No ComponentTypes to import"); //$NON-NLS-1$
//          }              
          

        // add the bindings to the map that is used by deployment
          configurationObjects.addAll(bindings);
          
          
        
         
          

          // retreive the host elements from the document.
          List hostElements = configurationElement.getChildren(XMLConfig_42_ElementNames.Configuration.Host.ELEMENT);
          Iterator hostiterator = hostElements.iterator();
          Host host = null;
          HostID hostID = null;
          while (hostiterator.hasNext()) {
              Element hostElement = (Element)hostiterator.next();
              
              host = xmlHelper.createHost(hostElement,configID, editor, null);
              hostID = (HostID) host.getID();
              configurationObjects.add(host);
              
              
              List vmElements = hostElement.getChildren(XMLConfig_42_ElementNames.Configuration.Process.ELEMENT);
              Iterator vmIt = vmElements.iterator();
              while (vmIt.hasNext()) {
                  Element vmElement = (Element)vmIt.next();
                  
                  VMComponentDefn vm = xmlHelper.createVMComponentDefn(vmElement, configID, hostID, editor, null);
                  
//                  DeployedComponent vmdc = xmlHelper.createDeployedVMComponentDefn(vmElement, configID, hostID, vm.getComponentTypeID(), editor);
                  VMComponentDefnID vmID = (VMComponentDefnID) vm.getID();
                    
                  configurationObjects.add(vm);
//                  configurationObjects.add(vmdc);
                  
                  List pscElements = vmElement.getChildren(XMLConfig_42_ElementNames.Configuration.ProductServiceConfig.ELEMENT);
                  Iterator pscIt = pscElements.iterator();
                  while (pscIt.hasNext()) {
                      Element pscElement = (Element)pscIt.next();
                      // create this only for use to create the deployed service
                      String pscName = pscElement.getAttributeValue(XMLConfig_42_ElementNames.Configuration.ProductServiceConfig.Attributes.NAME);
                      
                      ProductServiceConfig psc = (ProductServiceConfig) pscMap.get(pscName);
                   //   ProductServiceConfigID pscID = new ProductServiceConfigID(pscName);
                      
                      
                  //    ProductServiceConfig psc = xmlHelper.createProductServiceConfig(pscElement, configID, editor, null);
                      
                      List svcElements = pscElement.getChildren(XMLConfig_42_ElementNames.Configuration.DeployedService.ELEMENT);

                      Iterator svcIt = svcElements.iterator();
                      while (svcIt.hasNext()) {
                          Element svcElement = (Element)svcIt.next();
                          // create this only for use to create the deployed service
                          DeployedComponent dc = xmlHelper.createDeployedServiceComponent(svcElement, configID, hostID, vmID, (ProductServiceConfigID)psc.getID(), componentTypeMap, editor);
                          configurationObjects.add(dc);
   
                      }                       
                      
                  }                  
                  
                  
              }
              
              
              
              
          }
          
          
 
//          Map vmComponentDefnMap = new HashMap();
//          Element vmComponentDefnsElement = configurationElement.getChild(XMLConfig_42_ElementNames.Configurations.Configuration.VMComponentDefns.ELEMENT);
//          if (vmComponentDefnsElement!=null) {
//              Collection vmComponentDefnElements =
//                  vmComponentDefnsElement.getChildren(XMLConfig_42_ElementNames.Configurations.Configuration.VMComponentDefns.VMComponentDefn.ELEMENT);
//
//              iterator = vmComponentDefnElements.iterator();
//              while (iterator.hasNext()) {
//                  Element vmComponentDefnElement = (Element)iterator.next();
//                  VMComponentDefn vmComponentDefn =
//                      xmlHelper.createVMComponentDefn(vmComponentDefnElement, (ConfigurationID) config.getID() , editor, null);
////                  LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Found VMComponentDefn named: " + vmComponentDefn + " in XML file VMComponentDefns element.");
//                  vmComponentDefnMap.put(vmComponentDefn.getID(), vmComponentDefn);
//                  configurationObjects.add(vmComponentDefn);
//
//              }
//          }
//          Element deployedComponentsElement =
//              configurationElement.getChild(XMLConfig_42_ElementNames.Configurations.Configuration.DeployedComponents.ELEMENT);
//          if (deployedComponentsElement!=null) {
//              Collection deployedComponentElements =
//                  deployedComponentsElement.getChildren(XMLConfig_42_ElementNames.Configurations.Configuration.DeployedComponents.DeployedComponent.ELEMENT);
//
//              iterator = deployedComponentElements.iterator();
//              while (iterator.hasNext()) {
//                  Element deployedComponentElement = (Element)iterator.next();
//                  DeployedComponent deployedComponent =
//                      xmlHelper.createDeployedComponent(deployedComponentElement, config, editor, serviceComponentDefnMap, vmComponentDefnMap, componentTypeMap,  null);
////                  LogManager.logTrace(LogCommonConstants.CTX_CONFIG, "Found DeployedComponent named: " + deployedComponent + " in XML file DeployedComponents element.");
//                  configurationObjects.add(deployedComponent);
//              }
//          }
          // this will ensure that the actions that exist in the editor are
          // self contained....that there are no references to configuration objects
          // in the configuration objects that are not also contained in the Collection
          resolveConfigurationObjects(configurationObjects);

//  ***        I18nLogManager.logInfo(LogCommonConstants.CTX_CONFIG, LogMessageKeys.CONFIG_MSG_0006);

          return configurationObjects;
      }
    


     protected XMLHelper getXMLHelper() {
         if (xmlHelper == null) {
             xmlHelper = new XMLConfig_42_HelperImpl();
             
         }
         return xmlHelper;

     }
     
//     protected Properties createHeaderProperties(Properties props) {
//         Properties defaultProperties = new Properties();
//         defaultProperties.setProperty(ConfigurationPropertyNames.USER_CREATED_BY, DEFAULT_USER_CREATED_BY);
//         
//         // the properties passed in by the user override those put in by this
//         // method.
//         if (props!=null) {
//             defaultProperties.putAll(props);
//         }
//         defaultProperties.setProperty(ConfigurationPropertyNames.CONFIGURATION_VERSION, ConfigurationPropertyNames.MM_CONFIG_4_2_VERSION);        
//         defaultProperties.setProperty(ConfigurationPropertyNames.METAMATRIX_SYSTEM_VERSION, MetaMatrixProductNames.VERSION_NUMBER);
//         defaultProperties.setProperty(ConfigurationPropertyNames.TIME, DateUtil.getCurrentDateAsString());
//        
//         
//         return defaultProperties;
//     }
     
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

//          Assertion.isNotNull(stream);
          Assertion.isNotNull(editor);

//  ***        LogManager.logDetail(LogCommonConstants.CTX_CONFIG, "Importing a ComponentType object...");

//          Document doc = null;
//
//          try {
//              doc = getXMLReaderWriter().readDocument(stream);
//          }catch(JDOMException e) {
//            e.printStackTrace();
//              throw new IOException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0005));
//          }
//          Element root = doc.getRootElement();


        ComponentType t = createComponentType(root, editor, name);

//  ***        I18nLogManager.logInfo(LogCommonConstants.CTX_CONFIG, LogMessageKeys.CONFIG_MSG_0007, t.getName());

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
//            Assertion.isNotNull(stream);
            Assertion.isNotNull(editor);


            Collection connectorTypes = createComponentTypes(root, editor) ;

            return connectorTypes;       
      
       }
      
      
      protected ComponentType createComponentType(Element root, ConfigurationObjectEditor editor, String name) throws InvalidConfigurationElementException{
          XMLHelper helper  = getXMLHelper();

//          Element root = doc.getRootElement();

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
      
      protected Collection createComponentTypes(Element root, ConfigurationObjectEditor editor) throws InvalidConfigurationElementException{

          Element componentTypesElement = root.getChild(XMLElementNames.ComponentTypes.ELEMENT);

          if (componentTypesElement == null) {
              return Collections.EMPTY_LIST;
          }
                  

          // Make a copy of the lists that we intend to change so that we don't affect JDOM's list (which changes the underlying structure)
          List componentTypes = new ArrayList(componentTypesElement.getChildren(XMLElementNames.ComponentTypes.ComponentType.ELEMENT));
    
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
          
          
//          List connObjects = null;
//          if (connectorTypes != null) {
//              connObjects = new ArrayList(connectorTypes.size());
//               Iterator iterator = connectorTypes.iterator();
//               while (iterator.hasNext()) {
//                   Element connElement = (Element)iterator.next();
//                   ComponentType type = helper.createComponentType(connElement, editor, null, true);
//                   connObjects.add(type);
//               }
//          } else {
//              return Collections.EMPTY_LIST;
//          }

          return connObjects;
      }
    
     

     
}
