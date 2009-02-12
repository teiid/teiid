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

//note that this class is in the model directory so that the Configuration
//object implementations can invoke the package level methods on it
import org.jdom.Element;

import com.metamatrix.common.config.api.AuthenticationProvider;
import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentObject;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.Configuration;
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
import com.metamatrix.common.config.model.BasicUtil;
import com.metamatrix.core.util.Assertion;

/**
* This class is used to create a Configuration XML element that will contain
* all of the information related to a Configuration object.  The pattern for 
* use of this class should be to ONLY pass this visitor to a Configuration object
* throught its accept() method.  DO NOT pass this visitor directly to any
* configuration object other than a Configuration object.  The algorithm will
* fail.
*
*/
public class XMLConfig_42_Visitor  {
 
 private XMLConfig_42_HelperImpl helper;
 private ConfigurationObjectEditor editor;
     
 // these local variables are used to hold the current Element that all 
 // subsequent Elments of that type are to be added to.  When a new
 // configuration is visited, new category elements are created and put
 // into these references so that subsequent configuration objects will 
 // be put in the category elements for that new configuration.
// private Element systemPropertiesElement;
// private Element productServiceConfigsElement;
// private Element serviceDefnsElement;
// private Element resourcePoolsElement;
// private Element connectorsElement;    
// private Element deployedComponentsElement;
// private Element vmComponentDefnsElement;
// private Element hostsElement;
// private Element compTypesElement;
    
 /**
 * This constructor does nothing.
 */
 public XMLConfig_42_Visitor(XMLConfig_42_HelperImpl impl) {
     helper = impl;
                                             
 }
 
 public XMLConfig_42_Visitor(XMLConfig_42_HelperImpl impl, ConfigurationObjectEditor configEditor) {
     helper = impl;
     editor = configEditor;                                                 
 } 
 
 public ConfigurationObjectEditor getEditor() {
     return editor;
 }

// object visitDeployedElement(Element element,                                 
//                             int componentDefnType, 
//                             ConfigurationID configID,
//                             HostID hostID,
//                             VMComponentDefnID vmID,
//                             ProductServiceConfigID pscID,
//                             ) throws Exception {)
// 
// // @see ComponentDefn for the various types
// Object visitElement(Element element, int componentDefnType, ConfigurationID configID) throws Exception {
//     Object result = null;
//     
//     switch(componentDefnType){
//         case ComponentDefn.VM_COMPONENT_CODE:
//             return helper.createVMComponentDefn(element, configID, editor, null);
//         case ComponentDefn.PSC_COMPONENT_CODE:
//             return helper.createProductServiceConfig(element, configID, getEditor(), null);
//         case ComponentDefn.CONNECTOR_COMPONENT_CODE:
//             return helper.createConnectorBinding(element, getEditor(), null, true);
//         case ComponentDefn.SERVICE_COMPONENT_CODE:                
//             return helper.createServiceComponentDefn(element, configID, getEditor(), null);
//         case ComponentDefn.RESOURCE_DESCRIPTOR_COMPONENT_CODE:
//             return helper.createResourcePool(element, configID, getEditor());
//         case ComponentDefn.HOST_COMPONENT_CODE:
//             return helper.createHost(element,configID, editor, null);
//         case ComponentDefn.CONFIGURATION_COMPONENT_CODE:
//             return helper.createConfiguration(element, getEditor(), null);
//         case ComponentDefn.DEPLOYED_COMPONENT_CODE:
//             return helper.createDeployedComponent(element, null, result, null, null, null, null)
//
//     }
//     
//     
//     return result;
//     
// }
 
 Element visitComponent(ComponentType component) {
     if (component instanceof ProductType) {
         return helper.createProductTypeElement((ProductType) component);
     } 
     return helper.createComponentTypeElement(component);
     
 }
 
 
 Element visitComponent(ComponentObject component) {
     Assertion.isNotNull(component);
     
     switch(BasicUtil.getComponentDefnType(component)){
         case ComponentDefn.VM_COMPONENT_CODE:
             return helper.createVMComponentDefnElement((VMComponentDefn) component);
         case ComponentDefn.PSC_COMPONENT_CODE:
             return helper.createProductServiceConfigElement((ProductServiceConfig) component);
         case ComponentDefn.CONNECTOR_COMPONENT_CODE:
             return helper.createServiceComponentDefnElement((ServiceComponentDefn) component);

             
//             return helper.createConnectorBindingElement((ConnectorBinding) component, true);
         case ComponentDefn.SERVICE_COMPONENT_CODE:  
             return helper.createServiceComponentDefnElement((ServiceComponentDefn) component);
         case ComponentDefn.AUTHPROVIDER_COMPONENT_CODE:  
             return helper.createAuthenticationProviderElement((AuthenticationProvider) component);
             
         case ComponentDefn.RESOURCE_DESCRIPTOR_COMPONENT_CODE:
             return helper.createResourcePoolElement((ResourceDescriptor) component);
         case ComponentDefn.HOST_COMPONENT_CODE:
             return helper.createDeployedHostElement((Host) component);
         case ComponentDefn.CONFIGURATION_COMPONENT_CODE:
             return helper.createConfigurationElement((Configuration) component);
         case ComponentDefn.DEPLOYED_COMPONENT_CODE:
             return helper.createDeployedComponentElement((DeployedComponent) component);
         case ComponentDefn.SHARED_RESOURCE_COMPONENT_CODE:
             return helper.createSharedResourceElement((SharedResource) component);
         case ComponentDefn.PRODUCT_COMPONENT_CODE:
             return helper.createProductTypeElement((ProductType) component);
         
     }
     return null;
 }
 
 Element visitConnectorBindingComponent(ComponentObject component) {
     Assertion.isNotNull(component);
   return helper.createConnectorBindingElement((ConnectorBinding) component, true);
     
 }
 
     

 
         
}
 
