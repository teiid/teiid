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


import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.jdom.Element;

import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentObject;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationInfo;
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
import com.metamatrix.common.config.util.InvalidConfigurationElementException;
import com.metamatrix.common.object.PropertyDefinition;

/**
* This helper class is used to create JDOM XML Elements from configuration objects.
*/
public interface XMLHelper {

    /**
    * This method is used to create a Configuration JDOM Element from a 
    * Configuration object.
    *
    * @param configuration the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createConfigurationElement(Configuration configuration);
        
        
    /**
    * This method is used to create a ConfigurationInfo JDOM Element from a 
    * ConfigurationInfo object.
    *
    * @param info the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createConfigurationInfoElement(ConfigurationInfo info);
    
    /**
    * This method is used to create a LogConfiguration JDOM Element from a 
    * LogConfiguration object.
    *
    * @param info the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
//    public Element createLogConfigurationElement(LogConfiguration logConfiguration) ;
    
    
    /**
    * This method is used to create a DeployedComponent JDOM Element from a 
    * DeployedComponent object.
    *
    * @param deployedComponent the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
//    public Element createDeployedComponentElement(DeployedComponent deployedComponent) ;
    
//    public DeployedComponent createDeployedVMComponentDefn(Element element,
//                                                           ConfigurationID configID, 
//                                                           HostID hostID,
//                                                           ComponentTypeID typeID,
//                                                           ConfigurationObjectEditor editor) 
//                                                           throws InvalidConfigurationElementException;
    
    
    /**
    * This method is used to create a VMComponentDefn JDOM Element from a 
    * VMComponentDefn object.
    *
    * @param defn the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createVMComponentDefnElement(VMComponentDefn defn) ;
    
    /**
    * This method is used to create a ServiceComponentDefn JDOM Element from a 
    * ServiceComponentDefn object.
    *
    * @param defn the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createServiceComponentDefnElement(ServiceComponentDefn defn);

    /**
    * This method is used to create a ResourceDescriptor JDOM Element from a 
    * ResourceDescriptor object.
    *
    * @param defn the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */

//    public Element createServiceComponentDefnElement(ResourceDescriptor defn);

          
    /**
    * This method is used to create a ProductServiceConfig JDOM Element from a 
    * ProductServiceConfig object.
    *
    * @param config the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createProductServiceConfigElement(ProductServiceConfig config);
        
    /**
    * This method is used to create a ComponentType JDOM Element from a 
    * ComponentType object.
    *
    * @param type the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createComponentTypeElement(ComponentType type) ;
    
    /**
    * This method is used to create a PropertyDefinition JDOM Element from a 
    * PropertyDefinition object.
    *
    * @param defn the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createPropertyDefinitionElement(PropertyDefinition defn) ;
    
    /**
    * This method is used to create a ComponentTypeDefn JDOM Element from a 
    * ComponentTypeDefn object.
    *
    * @param defn the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createComponentTypeDefnElement(ComponentTypeDefn defn) ;
    
    /**
    * This method is used to create a ProductType JDOM Element from a 
    * ProductType object.
    *
    * @param type the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createProductTypesElement() ;
    
    /**
     * This method is used to create a ProductServiceConfig JDOM Element from a 
     * ProductServiceConfig object.
     *
     * @param type the Object to be converted to a JDOM XML Element
     * @return a JDOM XML Element
     */
     public Element createProductServiceConfigsElement() ;
    
    
    /**
    * This method is used to create a ProductType JDOM Element from a 
    * ProductType object.
    *
    * @param type the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createProductTypeElement(ProductType type);
    
    /**
    * This method is used to create a Host JDOM Element from a 
    * Host object.
    *
    * @param host the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createHostElement(Host host) ;
    
    /**
    * <p>This method is used to create a Header JDOM Element from a 
    * Properties object.  The properties object can contain any of the 
    * following properties that will be included in the header:<p>
    * <pre>
    * XMLElementNames.Header.ApplicationCreatedDate.ELEMENT
    * XMLElementNames.Header.ApplicationVersionCreatedBy.ELEMENT
    * XMLElementNames.Header.UserName.ELEMENT
    * XMLElementNames.Header.DocumentTypeVersion.ELEMENT
    * XMLElementNames.Header.MetaMatrixServerVersion.ELEMENT
    * XMLElementNames.Header.Time.ELEMENT
    * <pre>
    * <p>Any of these properties that are not included in the properties object
    * will not be included in the header Element that is returned.
    *
    * @param props the properties object that contains the values for the Header
    * @return a JDOM XML Element
    */
    public Element createHeaderElement(Properties props) ;
    
    
    /**
     * Return the properties stored in the header section. 
     * @param element 
     * @return Properties containing the header information
     * @throws InvalidConfigurationElementException if the element passed in 
     * is not the header element or its XML structure do not conform to the 
     * XML structure specfied in the XMLElementNames class.
     * 
     * @since 4.2
     */
    public Properties getHeaderProperties(Element element)throws InvalidConfigurationElementException;
 
    
    /**
     * Returns true if the information in the element indicates 
     * that it is compatible with the 4.2 configuration format.
     * Otherwise, it considered to be pre 4.2 compatible.
     * @param element
     * @return
     * @throws InvalidConfigurationElementException
     * @since 4.2
     */
    public boolean is42ConfigurationCompatible(Element element) throws InvalidConfigurationElementException;

    
    /**
    * This method is used to create a Properties JDOM Element from a 
    * Properties object.
    *
    * @param props the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createPropertiesElement(Properties props);
    
    /**
    * This method is used to create a Configuration ID JDOM Element from a 
    * Configuration ID object.
    *
    * @param type the ID type to be created. @see XMLElementNames.Configurations.Configuration.XXXID.ELEMENT for valid values
    * @param name the calue of the name attribute of the ID element to create.
    * @return a JDOM XML Element
    */
    public Element createIDElement(String type, String name);
    
    /**
    * This method is used to create a Configurations JDOM Element.
    * This element is for structural organization
    * only and does not represent any real configuration object.
    *
    * @return a JDOM XML Element
    */
    public Element createConfigurationsElement();
    
    /**
    * This method is used to create a Hosts JDOM Element.
    * This element is for structural organization
    * only and does not represent any real configuration object.
    *
    * @return a JDOM XML Element
    */
    public Element createHostsElement();
    
    /**
    * This method is used to create a ComponentTypes JDOM Element.
    * This element is for structural organization
    * only and does not represent any real configuration object.
    *
    * @return a JDOM XML Element
    */
    public Element createComponentTypesElement();
    
    /**
    * This method is used to create a ServiceComponentDefns JDOM Element.
    * This element is for structural organization
    * only and does not represent any real configuration object.
    *
    * @return a JDOM XML Element
    */
    public Element createServiceComponentDefnsElement();
    
    /**
    * This method is used to create a root JDOM Element.
    * This element is for structural organization
    * only and does not represent any real configuration object.
    *
    * @return a JDOM XML Element
    */
    public Element createRootConfigurationDocumentElement();
    
    /**
    * This method will create a Host configuration object from an XML element
    * that represents a Host.
    * 
    * @param element the JDOM element to convert to a configuration object
    * @param editor the editor to use to create the configuration object
    * @param name the name of the returned configuration object. Note this 
    * name will override the name in the JDOM element.  If the name parameter
    * is null, the name of the object in the JDOM element will be used as 
    * the name of the object.
    * @return the Host configuration object
    * @throws InvalidConfigurationElementException if the element passed in 
    * or its XML structure do not conform to the XML structure specfied in 
    * the XMLElementNames class.
    */
    public Host createHost(Element element, ConfigurationID configID, ConfigurationObjectEditor editor, String name)throws InvalidConfigurationElementException;
    
    
    /**
    * This method will create a SharedResource configuration object from an XML element
    * that represents a SharedResource.
    * 
    * @param element the JDOM element to convert to a configuration object
    * @param editor the editor to use to create the configuration object
    * @return the ResourceDescriptor configuration object
    * @throws InvalidConfigurationElementException if the element passed in 
    * or its XML structure do not conform to the XML structure specfied in 
    * the XMLElementNames class.
    */
    public SharedResource createSharedResource(Element element, ConfigurationObjectEditor editor) throws InvalidConfigurationElementException;
    
    /**
    * This method is used to create a Resources JDOM Element.
    * This element is for structural organization
    * only and does not represent any real configuration object.
    *
    * @return a JDOM XML Element
    */
    public Element createSharedResourcesElement();
    
    
    /**
    * This method is used to create a Shared Resource JDOM Element from a 
    * SharedResource object.
    *
    * @param host the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createSharedResourceElement(SharedResource resource) ;
    
    
    /**
    * This method will create a ComponentType configuration object from an XML element
    * that represents a ComponentType.
    * 
    * @param element the JDOM element to convert to a configuration object
    * @param editor the editor to use to create the configuration object
    * @param name the name of the returned configuration object. Note this 
    * name will override the name in the JDOM element.  If the name parameter
    * is null, the name of the object in the JDOM element will be used as 
    * the name of the object.
    * @param maintainIDs should be true if the created ComponentType
    * should use the Parent ID name that is defined within the element
    * otherwise their parentID will be null.  This should only be true if
    * it is known that the configuration environment that this ComponentType
    * is to be commited to contains a valid ComponentType with the ParentID
    * matching that in the XML element (typically in the case of Connector
    * ComponentTypes).
    * @return the ComponentType configuration object
    * @throws InvalidConfigurationElementException if the element passed in 
    * or its XML structure do not conform to the XML structure specfied in 
    * the XMLElementNames class.
    */
    public ComponentType createComponentType(Element element, ConfigurationObjectEditor editor, String name, boolean maintainParentID)throws InvalidConfigurationElementException;
    
    /**
    * This method will create a ProductType configuration object from an XML element
    * that represents a ProductType.
    * 
    * @param element the JDOM element to convert to a configuration object
    * @param editor the editor to use to create the configuration object
    * @param name the name of the returned configuration object. Note this 
    * name will override the name in the JDOM element.  If the name parameter
    * is null, the name of the object in the JDOM element will be used as 
    * the name of the object.
    * @param componentTypeMap this is a map of ComponentTypeID--->ComponentType
    * it must contain all of the Component types that the ProductType 
    * that is represented by the passed in XML element references.
    * @return the ProductType configuration object
    * @throws InvalidConfigurationElementException if the element passed in 
    * or its XML structure do not conform to the XML structure specfied in 
    * the XMLElementNames class.
    */
    public ProductType createProductType(Element element, ConfigurationObjectEditor editor,Map ComponentTypeMap, String name)throws InvalidConfigurationElementException;
    
    
    /**
    * This method will create a Configuration configuration object from an XML element
    * that represents a Configuration.
    * 
    * @param element the JDOM element to convert to a configuration object
    * @param editor the editor to use to create the configuration object
    * @param name the name of the returned configuration object. Note this 
    * name will override the name in the JDOM element.  If the name parameter
    * is null, the name of the object in the JDOM element will be used as 
    * the name of the object.
    * @return the Configuration configuration object
    * @throws InvalidConfigurationElementException if the element passed in 
    * or its XML structure do not conform to the XML structure specfied in 
    * the XMLElementNames class.
    */
    public Configuration createConfiguration(Element element, ConfigurationObjectEditor editor, String name)throws InvalidConfigurationElementException;
    
    /**
    * This method will create a LogConfiguration configuration object from an XML element
    * that represents a LogConfiguration.
    * 
    * @param element the JDOM element to convert to a configuration object
    * @param editor the editor to use to create the configuration object
    * @param name the name of the returned configuration object. Note this 
    * name will override the name in the JDOM element.  If the name parameter
    * is null, the name of the object in the JDOM element will be used as 
    * the name of the object.
    * @return the LogConfiguration configuration object
    * @throws InvalidConfigurationElementException if the element passed in 
    * or its XML structure do not conform to the XML structure specfied in 
    * the XMLElementNames class.
    */
//    public LogConfiguration createLogConfiguration(Element element)throws InvalidConfigurationElementException;
    
    
    /**
    * This method will create a resource pool configuration object from an XML element
    * that represents a ResourceDescriptor.
    * 
    * @param element the JDOM element to convert to a configuration object
    * @param editor the editor to use to create the configuration object
    * @return the ResourceDescriptor configuration object
    * @throws InvalidConfigurationElementException if the element passed in 
    * or its XML structure do not conform to the XML structure specfied in 
    * the XMLElementNames class.
    */    
    public ResourceDescriptor createResourcePool(Element element, ConfigurationID configID, ConfigurationObjectEditor editor) throws InvalidConfigurationElementException;
    
    
    
    /**
    * This method is used to create a ResourceDescriptor JDOM Element from a 
    * ResourceDescriptor object.
    *
    * @param connector the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createResourcePoolElement(ResourceDescriptor resource);
    
    /**
    * This method is used to create a ResourceDescriptors JDOM Element.
    * This element is for structural organization
    * only and does not represent any real configuration object.
    *
    * @return a JDOM XML Element
    */
    
    public Element createResourcePoolsElement();
    
    /**
    * This method will create a ServiceComponentDefn configuration object from an XML element
    * that represents a ServiceComponentDefn.
    * 
    * @param element the JDOM element to convert to a configuration object
    * @param editor the editor to use to create the configuration object
    * @param name the name of the returned configuration object. Note this 
    * name will override the name in the JDOM element.  If the name parameter
    * is null, the name of the object in the JDOM element will be used as 
    * the name of the object.
    * @return the ServiceComponentDefn configuration object
    * @throws InvalidConfigurationElementException if the element passed in 
    * or its XML structure do not conform to the XML structure specfied in 
    * the XMLElementNames class.
    */
    public ComponentDefn createServiceComponentDefn(Element element, Configuration config, ConfigurationObjectEditor editor, String name)throws InvalidConfigurationElementException;
 
    /**
    * This method will create a ServiceComponentDefn configuration object from an XML element
    * that represents a ServiceComponentDefn.
    * 
    * @param element the JDOM element to convert to a configuration object
    * @param editor the editor to use to create the configuration object
    * @param name the name of the returned configuration object. Note this 
    * name will override the name in the JDOM element.  If the name parameter
    * is null, the name of the object in the JDOM element will be used as 
    * the name of the object.
    * @return the ServiceComponentDefn configuration object
    * @throws InvalidConfigurationElementException if the element passed in 
    * or its XML structure do not conform to the XML structure specfied in 
    * the XMLElementNames class.
    */
    
    public ComponentDefn createServiceComponentDefn(Element element, ConfigurationID configID, ConfigurationObjectEditor editor, String name)throws InvalidConfigurationElementException;
    
    
    
    /**
    * This method is used to create a ConnectorBindings JDOM Element.
    * This element is for structural organization
    * only and does not represent any real configuration object.
    *
    * @return a JDOM XML Element
    */
    public Element createConnectorBindingsElement();
    
    /**
    * This method is used to create a Connector Binding JDOM Element from a 
    * ConnectorBinding object.
    *
    * @param connector the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createConnectorBindingElement(ConnectorBinding connector, boolean isExportConfig) ;
    
    
    /**
    * This method will create a ConnectorBinding object from an XML element
    * that represents a ServiceComponentDefn.
    * 
    * @param element the JDOM element to convert to a configuration object
    * @param editor the editor to use to create the configuration object
    * @param name the name of the returned configuration object. Note this 
    * name will override the name in the JDOM element.  If the name parameter
    * is null, the name of the object in the JDOM element will be used as 
    * the name of the object.
    * @return the ConnectorBinding configuration object
    * @throws InvalidConfigurationElementException if the element passed in 
    * or its XML structure do not conform to the XML structure specfied in 
    * the XMLElementNames class.
    */
    
    public ConnectorBinding createConnectorBinding(ConfigurationID configurationID, Element element, ConfigurationObjectEditor editor, String name, boolean isImportConfig)throws InvalidConfigurationElementException;
    
    /**
    * This method will create a DeployedComponent configuration object from an XML element
    * that represents a DeployedComponent.
    * 
    * @param element the JDOM element to convert to a configuration object
    * @param editor the editor to use to create the configuration object
    * @param name the name of the returned configuration object. Note this 
    * name will override the name in the JDOM element.  If the name parameter
    * is null, the name of the object in the JDOM element will be used as 
    * the name of the object.
    * @param serviceComponentDefnMap a map of ServiceComponentDefnID-->ServiceComponentDefn
    * this map must contain at the very least the ServiceComponentDefn that 
    * is the service definition of the deployed component that the XML element 
    * references.  This is used if the deployedComponent is a Service. Otherwise
    * it is ignored.
    * @param vmComponentDefnMap a map of vmComponentDefnID-->vmComponentDefn
    * this map must contain at the very least the vmComponentDefn that 
    * is the VM definition of the deployed component that the XML element 
    * references.  This is used if the deployedComponent is a VM. Otherwise
    * it is ignored.
    * @return the DeployedComponent configuration object
    * @throws InvalidConfigurationElementException if the element passed in 
    * or its XML structure do not conform to the XML structure specfied in 
    * the XMLElementNames class.
    */
    public DeployedComponent createDeployedComponent(Element element, Configuration config, ConfigurationObjectEditor editor, Map serviceComponentDefnMap, Map vmComponentDefnMap, Map componentTypeMap, String name)throws InvalidConfigurationElementException;
    
    
    public DeployedComponent createDeployedComponent(Element element,
                                                     ConfigurationID configID, 
                                                     HostID hostID,
                                                     VMComponentDefnID vmID,
                                                     ProductServiceConfigID pscID,
                                                     Map componentTypeMap,
                                                     ConfigurationObjectEditor editor) 
                                                     throws InvalidConfigurationElementException;
    
    
    public Element createDeployedProductServiceConfigElement(ProductServiceConfig config);
    
    /**
    * This method will create a VMComponentDefn configuration object from an XML element
    * that represents a VMComponentDefn.
    * 
    * @param element the JDOM element to convert to a configuration object
    * @param configID is the configuration the vm will become a part of
    * @param editor the editor to use to create the configuration object
    * @param name the name of the returned configuration object. Note this 
    * name will override the name in the JDOM element.  If the name parameter
    * is null, the name of the object in the JDOM element will be used as 
    * the name of the object.
    * @return the VMComponentDefn configuration object
    * @throws InvalidConfigurationElementException if the element passed in 
    * or its XML structure do not conform to the XML structure specfied in 
    * the XMLElementNames class.
    */
    public VMComponentDefn createVMComponentDefn(Element element, ConfigurationID configID, HostID hostID, ConfigurationObjectEditor editor, String name)throws InvalidConfigurationElementException;
    
    /**
     *  
     * @param element
     * @param configID
     * @param hostID
     * @param editor
     * @param name
     * @return
     * @throws InvalidConfigurationElementException
     * @since 4.1
     * @deprecated
     */
    
 //   public VMComponentDefn createVMComponentDefn(Element element, ConfigurationID configID, ConfigurationObjectEditor editor, String name)throws InvalidConfigurationElementException;
    
    /**
    * This method will create a ProductServiceConfig configuration object from an XML element
    * that represents a ProductServiceConfig.
    * 
    * @param element the JDOM element to convert to a configuration object
    * @param configID is the configuration this PSC will become a part of
    * @param editor the editor to use to create the configuration object
    * @param name the name of the returned configuration object. Note this 
    * name will override the name in the JDOM element.  If the name parameter
    * is null, the name of the object in the JDOM element will be used as 
    * the name of the object.
    * @return the ProductServiceConfig configuration object
    * @throws InvalidConfigurationElementException if the element passed in 
    * or its XML structure do not conform to the XML structure specfied in 
    * the XMLElementNames class.
    */
   public ProductServiceConfig createProductServiceConfig(Element element, ConfigurationID configID, ConfigurationObjectEditor editor, String name)throws InvalidConfigurationElementException;
    
    /**
    * This method will create a ComponentObject configuration object from an XML element
    * that represents a ComponentObject.
    * 
    * @param element the JDOM element to convert to a configuration object
    * @param editor the editor to use to create the configuration object
    * @param name the name of the returned configuration object. Note this 
    * name will override the name in the JDOM element.  If the name parameter
    * is null, the name of the object in the JDOM element will be used as 
    * the name of the object.
    * @return the ComponentObject configuration object
    * @throws InvalidConfigurationElementException if the element passed in 
    * or its XML structure do not conform to the XML structure specfied in 
    * the XMLElementNames class.
    */
    public ComponentObject addProperties(Element propertiesElement, ComponentObject object, ConfigurationObjectEditor editor) throws InvalidConfigurationElementException;
    
    /**
    * This helper method will order a List of XML JDOM elements that represent
    * ComponentTypes or ProductTypes so that types that are referenced by other
    * types in the list are in the returned list before the object that is
    * referencing it.  This is necessary so that the object editor that is
    * used to create the ComponentTypes will contain viable actions.  
    * a ComponentType object that references a super cannot be created prior to the super
    * ComponentType object.
    *
    * @param List of ComponentType objects
    */
    public void orderComponentTypeElementList(List componentTypeElements);
    
}
