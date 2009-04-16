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

package com.metamatrix.common.config.model;

import com.metamatrix.common.config.api.AuthenticationProvider;
import com.metamatrix.common.config.api.AuthenticationProviderID;
import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentObject;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ConnectorBindingID;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.DeployedComponentID;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.HostType;

import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.api.ResourceDescriptorID;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.api.SharedResourceID;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.config.api.VMComponentDefnType;
import com.metamatrix.common.namedobject.BaseID;
import com.metamatrix.common.namedobject.BaseObject;
import com.metamatrix.core.util.Assertion;


/** 
 * @since 4.2
 */
public class BasicUtil {
	
	   private static BasicConfigurationObjectEditor editor = new BasicConfigurationObjectEditor();

	    
	    /**
	     * Returns a COnfigurationObjectEditor that does not create actions.  This editor,
	     * because it doesn't maintain any state, is sharable for reuse. 
	     * @return
	     * @since 4.3
	     */
	    public static ConfigurationObjectEditor getEditor() {
	        return editor;
	    }    


    /**
     *  static method that is used to create the specified instance type of BasicComponentDefn.
     *  @param defnTypeCode identifies the type of class this component defn should represent
     *      @see ComponentDefn for type codes
     *  @param configID is the ConfigurationID identifying what configuration this defn belongs
     *  @param typeID is the ComponentTypeID identifying this new created BasicComponentType
     *  @param defnName is the name of the component defn
     *  @return BasicComponentDefn
     */
    public static final BasicComponentDefn createComponentDefn(int defnTypeCode, ConfigurationID configID, ComponentTypeID typeID, String defnName)  {

        BasicComponentDefn defn = null;
//            
//        if (defnTypeCode == ComponentDefn.RESOURCE_DESCRIPTOR_COMPONENT_CODE) {
//            ResourceDescriptorID descriptorID = new ResourceDescriptorID(configID,defnName);
//            defn = new BasicResourceDescriptor(configID, descriptorID, typeID);
//           
                             
        if (defnTypeCode == ComponentDefn.CONNECTOR_COMPONENT_CODE) { 
            ConnectorBindingID conID = new ConnectorBindingID(configID, defnName);
            defn = new BasicConnectorBinding(configID, conID, typeID);                    

        } else if (defnTypeCode == ComponentDefn.AUTHPROVIDER_COMPONENT_CODE) { 
            AuthenticationProviderID conID = new AuthenticationProviderID(configID, defnName);
            defn = new BasicAuthenticationProvider(configID, conID, typeID);                    

        } else if (defnTypeCode == ComponentDefn.HOST_COMPONENT_CODE) { 
            HostID conID = new HostID(defnName);
            defn = new BasicHost(configID, conID, typeID);                    
            
            
        } else if (defnTypeCode == ComponentDefn.SERVICE_COMPONENT_CODE) {
            ServiceComponentDefnID sID = new ServiceComponentDefnID(configID, defnName);
            defn = new BasicServiceComponentDefn(configID, sID, typeID);
        } else {
           Assertion.assertTrue(true, "DefnTypeCode:" + defnTypeCode + " is not defined in BasicUtil.createComponentDefn"); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
        return defn;
    }
    
    public static final BasicComponentDefn createComponentDefn(int defnTypeCode, ConfigurationID configID, HostID hostID, ComponentTypeID typeID, String defnName)  {

        BasicComponentDefn defn = null;
        if (defnTypeCode == ComponentDefn.VM_COMPONENT_CODE) {
            VMComponentDefnID vmID = new VMComponentDefnID(configID, hostID, defnName);
            defn = new BasicVMComponentDefn(configID, hostID, vmID, typeID);
        } else {
            Assertion.assertTrue(true, "DefnTypeCode:" + defnTypeCode + " is not defined in BasicUtil.createComponentODefn for VM"); //$NON-NLS-1$ //$NON-NLS-2$
    }

        return defn;
    }
    
    
    /**
     *  static method that is used to create the specified instance type of BasicComponentObject
     *  for a PSC.
     *  @param defnTypeCode identifies the type of class this component defn should represent
     *      @see ComponentObject for type codes
     *  @param configID is the ConfigurationID identifying what configuration this defn belongs
     *  @param typeID is the ComponentTypeID identifying this new created BasicComponentType
     *  @param defnName is the name of the component defn
     *  @return BasicComponentObject
     */
    public static final BasicComponentObject createComponentObject(int defnTypeCode,  ComponentTypeID typeID, String objName)  {

        BasicComponentObject defn = null;
        if (defnTypeCode == ComponentDefn.SHARED_RESOURCE_COMPONENT_CODE) {
             SharedResourceID id = new SharedResourceID(objName);
             defn = new BasicSharedResource(id, typeID);
        } else {
                Assertion.assertTrue(true, "DefnTypeCode:" + defnTypeCode + " is not defined in BasicUtil.createComponentObject"); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
        return defn;
    }
    
 

    /**
     *  static method that is used to create the specified instance type of BasicComponentType.
     *  @param classTypeCode identifies the type of class this component type should represent
     *      @see ComponentDefn or @see ComponentObject for type codes
     *  @param typeID is the ComponentTypeID identifying this new created BasicComponentType
     *  @param parentID is the component type that is the parent
     *  @param superID is the component type that is a super class of this component type
     *  @param deployable is a boolean indicating if the type can be deployed in a configuration
     *  @param monitored is a boolean insdicating if the type is to be monitored
     *  @return BasicComponentType
     */
    public static BasicComponentType createComponentType(int classTypeCode, String name, ComponentTypeID parentID, ComponentTypeID superID, boolean deployable, boolean monitored) {
        ComponentTypeID typeID = new ComponentTypeID(name);

        BasicComponentType type = null;
        if (name.equalsIgnoreCase(VMComponentDefnType.COMPONENT_TYPE_NAME)) {
            type = new BasicVMComponentDefnType(typeID, parentID, superID, deployable, false, monitored);                        
            classTypeCode =  ComponentType.VM_COMPONENT_TYPE_CODE;
        } else if (name.equalsIgnoreCase(HostType.COMPONENT_TYPE_NAME)) {
            type = new BasicHostType(typeID, parentID, superID, deployable, false, monitored);            
            classTypeCode =  ComponentType.HOST_COMPONENT_TYPE_CODE;

        } else if (name.equalsIgnoreCase(Configuration.COMPONENT_TYPE_NAME)) {
            type = new BasicComponentType(typeID, parentID, superID, deployable, false, monitored);
            classTypeCode =  ComponentType.CONFIGURATION_COMPONENT_TYPE_CODE;
           
        } else if (name.equalsIgnoreCase("DeployedComponent")) { //$NON-NLS-1$
 //           type = new BasicDeployedComponentType(typeID, parentID, superID, deployable, false, monitored);
            type = new BasicComponentType(typeID, parentID, superID, deployable, false, monitored);

            classTypeCode =  ComponentType.DEPLOYED_COMPONENT_TYPE_CODE;
            
        } else if (classTypeCode == ComponentType.CONNECTOR_COMPONENT_TYPE_CODE) {
            type = new BasicConnectorBindingType(typeID, parentID, superID, deployable, false, monitored);
        } else if (classTypeCode == ComponentType.AUTHPROVIDER_COMPONENT_TYPE_CODE) {
            type = new BasicAuthenticationProviderType(typeID, parentID, superID, deployable, false, monitored);
        } else if (classTypeCode == ComponentType.SERVICE_COMPONENT_TYPE_CODE) {
            type = new BasicServiceComponentType(typeID, parentID, superID, deployable, false, monitored);
        
//        } else if (classTypeCode == ComponentDefn.VM_COMPONENT_TYPE_CODE) {
//            type = new BasicVMComponentDefnType(typeID, parentID, superID, deployable, false, monitored);            
//        } else if (classTypeCode == ComponentDefn.HOST_COMPONENT_TYPE_CODE) {
//            type = new BasicHostType(typeID, parentID, superID, deployable, false, monitored);            
       
//         } else if (classTypeCode == ComponentObject.SHARED_RESOURCE_COMPONENT_TYPE_CODE) {
//            type = new BasicSharedResourceType(typeID, parentID, superID, deployable, false, monitored);            
            
            // anything else that uses the zero type code is considered a general use type
        } else if (classTypeCode == ComponentType.CONFIGURATION_COMPONENT_TYPE_CODE) {
            type = new BasicComponentType(typeID, parentID, superID, deployable, false, monitored);
       
        } else if (classTypeCode == ComponentType.RESOURCE_COMPONENT_TYPE_CODE) {
            type = new BasicResourceComponentType(typeID, parentID, superID, deployable, false, monitored);            

            
        } else {
        	throw new RuntimeException("ComponentType Exception "  + classTypeCode);
            //Assertion.assertTrue(true, "ClassTypeCode:" + classTypeCode + " is not defined in BasicUtil.createComponentType"); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
        if (type == null) {
        	throw new RuntimeException("Null Type");
        }
        

        type.setComponentTypeCode(classTypeCode);

        return type;
    }
    
    public static BasicDeployedComponent createDeployedComponent(String name, ConfigurationID configID, HostID hostID, VMComponentDefnID vmID, ServiceComponentDefnID svcID, ComponentTypeID typeID) {

        DeployedComponentID id = new DeployedComponentID(name, configID,  hostID, vmID, svcID);
        
        BasicDeployedComponent deployComponent = new BasicDeployedComponent(id,
                                                                            configID,
                                                                            hostID,
                                                                            vmID,
                                                                            svcID,                                                                     
                                                                            typeID);
        return deployComponent;
    }
    
    public static BasicDeployedComponent createDeployedVMComponent(String name, ConfigurationID configID, HostID hostID, VMComponentDefnID vmID, ComponentTypeID vmtypeID) {
        DeployedComponentID id = new DeployedComponentID(name, configID,  hostID, vmID);
        
        BasicDeployedComponent deployComponent = new BasicDeployedComponent(id,
                                                                            configID,
                                                                            hostID,
                                                                            vmID,
                                                                            vmtypeID);
        return deployComponent;
    }    
    
//  utility methods for determining the component type that
//  the result is used in switch statements
// utility methods for determining the component type that
// the result is used in switch statements
 public static int getComponentType(BaseObject defn) {
  if (defn instanceof Host) {
      return ComponentType.HOST_COMPONENT_TYPE_CODE;
  }else if(defn instanceof VMComponentDefn) {
      return ComponentType.VM_COMPONENT_TYPE_CODE;
  }else if(defn instanceof ConnectorBinding) {
      return ComponentType.CONNECTOR_COMPONENT_TYPE_CODE;            
  }else if(defn instanceof ResourceDescriptor) {
      return ComponentType.RESOURCE_COMPONENT_TYPE_CODE;
  }else if(defn instanceof ServiceComponentDefn) {
      return ComponentType.SERVICE_COMPONENT_TYPE_CODE;   
  }else if(defn instanceof AuthenticationProvider) {
      return ComponentType.AUTHPROVIDER_COMPONENT_TYPE_CODE;         
  }else if (defn instanceof Configuration) {
      return ComponentType.CONFIGURATION_COMPONENT_TYPE_CODE;
  } else if (defn instanceof DeployedComponent) {
      return ComponentType.DEPLOYED_COMPONENT_TYPE_CODE;  
      
  } else {
      Assertion.assertTrue(false, "Process Error: component object of type " + defn.getClass().getName() + " not accounted for."); //$NON-NLS-1$ //$NON-NLS-2$
  }

      return -1;
 }

  static int getComponentType(BaseID defnID) {
     if (defnID instanceof HostID) {
         return ComponentType.HOST_COMPONENT_TYPE_CODE;
      }else if(defnID instanceof VMComponentDefnID) {
         return ComponentType.VM_COMPONENT_TYPE_CODE;
     }else if(defnID instanceof ResourceDescriptorID) {
         return ComponentType.RESOURCE_COMPONENT_TYPE_CODE;   
     }else if(defnID instanceof SharedResourceID) {
         return ComponentType.RESOURCE_COMPONENT_TYPE_CODE;           
     }else if(defnID instanceof ConnectorBindingID) {
         return ComponentType.CONNECTOR_COMPONENT_TYPE_CODE;            
     }else if(defnID instanceof ServiceComponentDefnID) {
         return ComponentType.SERVICE_COMPONENT_TYPE_CODE;
     }else if(defnID instanceof AuthenticationProviderID) {
         return ComponentType.AUTHPROVIDER_COMPONENT_TYPE_CODE;                  
     }else if(defnID instanceof ConfigurationID) {
         return ComponentType.CONFIGURATION_COMPONENT_TYPE_CODE;
     }else if(defnID instanceof DeployedComponentID) {
         return ComponentType.DEPLOYED_COMPONENT_TYPE_CODE;
                 
         
     }else {
         Assertion.assertTrue(false, "Process Error: component object of type " + defnID.getClass().getName() + " not accounted for."); //$NON-NLS-1$ //$NON-NLS-2$
     }
     
     return -1;
  }  

  
//utility methods for determining the component type that
//the result is used in switch statements
//utility methods for determining the component type that
//the result is used in switch statements
public static int getComponentDefnType(BaseObject defn) {
    if (defn instanceof Host) {
        return ComponentDefn.HOST_COMPONENT_CODE;
    }else if(defn instanceof VMComponentDefn) {
        return ComponentDefn.VM_COMPONENT_CODE;
    }else if(defn instanceof ConnectorBinding) {
        return ComponentDefn.CONNECTOR_COMPONENT_CODE;            
//    }else if(defn instanceof ResourceDescriptor) {
//        return ComponentDefn.RESOURCE_DESCRIPTOR_COMPONENT_CODE;
    }else if(defn instanceof ServiceComponentDefn) {
        return ComponentDefn.SERVICE_COMPONENT_CODE;   
    }else if(defn instanceof AuthenticationProvider) {
        return ComponentDefn.AUTHPROVIDER_COMPONENT_CODE;         
    }else if (defn instanceof Configuration) {
        return ComponentDefn.CONFIGURATION_COMPONENT_CODE;
    } else if (defn instanceof DeployedComponent) {
        return ComponentDefn.DEPLOYED_COMPONENT_CODE;
    } else if (defn instanceof SharedResource) {
        return ComponentDefn.SHARED_RESOURCE_COMPONENT_CODE;      

        
    } else {
        Assertion.assertTrue(false, "Process Error: component defn object of type " + defn.getClass().getName() + " not accounted for."); //$NON-NLS-1$ //$NON-NLS-2$
    }

    return -1;
}  


public static boolean isValdComponentTypeCode(int code) {
	switch (code) {
	case ComponentType.AUTHPROVIDER_COMPONENT_TYPE_CODE:
	case ComponentType.CONFIGURATION_COMPONENT_TYPE_CODE:
	case ComponentType.CONNECTOR_COMPONENT_TYPE_CODE:
	case ComponentType.DEPLOYED_COMPONENT_TYPE_CODE:
	case ComponentType.HOST_COMPONENT_TYPE_CODE:
	case ComponentType.PRODUCT_COMPONENT_TYPE_CODE:
	case ComponentType.RESOURCE_COMPONENT_TYPE_CODE:
	case ComponentType.SERVICE_COMPONENT_TYPE_CODE:
	case ComponentType.VM_COMPONENT_TYPE_CODE:
		return true;

	//	break;
	}
	return false;
}


// ----------------------------------------------------------------------------------
//                  M O D I F I C A T I O N    M E T H O D S
// ----------------------------------------------------------------------------------

public static ComponentType setLastChangedHistory(ComponentType type, String lastChangedBy, String lastChangedDate) {
	Assertion.isNotNull(type);

    BasicComponentType target = (BasicComponentType) type;

     	target.setLastChangedBy(lastChangedBy);
     	target.setLastChangedDate(lastChangedDate);

 	return target;

}
public static ComponentType setCreationChangedHistory(ComponentType type, String createdBy, String creationDate) {
	Assertion.isNotNull(type);

    BasicComponentType target = (BasicComponentType) type;

     	target.setCreatedBy(createdBy);
     	target.setCreatedDate(creationDate);

 	return target;
}


public static ComponentObject setLastChangedHistory(ComponentObject defn, String lastChangedBy, String lastChangedDate) {
	Assertion.isNotNull(defn);

    BasicComponentObject target = (BasicComponentObject) defn;

    	target.setLastChangedBy(lastChangedBy);

     	target.setLastChangedDate(lastChangedDate);

 	return target;

}
//public static boolean set = false;
public static ComponentObject setCreationChangedHistory(ComponentObject defn, String createdBy, String creationDate) {
	Assertion.isNotNull(defn);

    BasicComponentObject target = (BasicComponentObject) defn;

     	target.setCreatedBy(createdBy);
     	target.setCreatedDate(creationDate);


	return target;

}


    

}
