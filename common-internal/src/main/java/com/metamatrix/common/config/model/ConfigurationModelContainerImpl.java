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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.api.AuthenticationProvider;
import com.metamatrix.common.config.api.ComponentObjectID;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.DeployedComponentID;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.api.ResourceDescriptorID;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.api.SharedResourceID;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.namedobject.BaseID;
import com.metamatrix.common.util.ErrorMessageKeys;

public class ConfigurationModelContainerImpl implements ConfigurationModelContainer, Serializable {

    private BasicConfiguration configuration=null;

    private Map compTypes = Collections.synchronizedMap(new HashMap(45));

    private Map resources = Collections.synchronizedMap(new HashMap(25));
    
    public ConfigurationModelContainerImpl() {

    }
    
 


	public ConfigurationModelContainerImpl(ArrayList configObjects) throws ConfigurationException {
        this.setConfigurationObjects(configObjects);
    }       
    
    public ConfigurationModelContainerImpl(Collection configObjects) throws ConfigurationException {
        this.setConfigurationObjects(configObjects);
    }    

    public ConfigurationModelContainerImpl(Configuration config) {

    	this.configuration = (BasicConfiguration) config;
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }
    
/**
 * Method indicates if this configuration model was loaded with a complete configuration
 * or not.  The other use is just to load with connector bindings and connector types where
 * the configuration is not needed. 
 * @return
 * @since 4.3
 */
    public boolean isConfigurationModel() {
        return (getConfiguration() != null);
    }

    
    public ConfigurationID getConfigurationID() {
        return (ConfigurationID) configuration.getID();
    }


    public Collection getAllObjects() {
            Collection configObjects = new ArrayList();

            configObjects.add(getConfiguration());
            
            configObjects.addAll(getConfiguration().getVMComponentDefns());
            
            configObjects.addAll(getConfiguration().getServiceComponentDefns());
            
            configObjects.addAll(getConfiguration().getDeployedComponents());

// Add the objects that are not configuration based, meaning they
// are not added,updated,delete within the context of a configuration
            configObjects.addAll(getHosts());

            configObjects.addAll(getConfiguration().getConnectorBindings());

            configObjects.addAll(getComponentTypes().values());


            configObjects.addAll(getResources());

            return configObjects;


    }

    public Map getComponentTypes() {
        return new HashMap(this.compTypes);
    }
    
    /**
     * Return a <code>ComponentTypeDefn</code> for a give typeID and defnName.
     * @param typeID identifies the specific @link ComponentType to look for
     * @param defnName idenfities the @link ComponentTypeDefn to look for
     *      in the componentType. 
     * @return ComponentTypeDefn
     * @since 4.1
     */
    public ComponentTypeDefn getComponentTypeDefinition(ComponentTypeID typeID, String defnName) {
        ComponentType type = this.getComponentType(typeID.getFullName());
        if (type == null) {
            return null;
        }
        ComponentTypeDefn defn = null;
        defn = type.getComponentTypeDefinition(defnName);
        if (defn != null) {
            return defn;
        }
        
        Collection inheritedDefns = getSuperComponentTypeDefinitions(null, null, type);
        // need to look in the inherited types to find the defn
        if (inheritedDefns == null || inheritedDefns.size() == 0) {
            return defn;
        }

        Iterator inheritedIter =  inheritedDefns.iterator();

        ComponentTypeDefn inheritedDefn = null;

        while (inheritedIter.hasNext()){
            inheritedDefn = (ComponentTypeDefn)inheritedIter.next();
            if (inheritedDefn.getFullName().equalsIgnoreCase(defnName)) {
                return inheritedDefn;
            }
        }
        
        return defn;
    }
        
    
    public Collection getAllComponentTypeDefinitions(ComponentTypeID typeID) {
        ComponentType type = this.getComponentType(typeID.getFullName());
        if (type == null) {
            return Collections.EMPTY_LIST;
        }
        
        Collection defns = type.getComponentTypeDefinitions();
        
        Collection inheritedDefns = getSuperComponentTypeDefinitions(null, null, type);
        

            //We want the final, returned Collection to NOT have any
            //duplicate objects in it.  The two Collections above may have
            //duplicates - one in inheritedDefns which is a name and a default
            //value for a super-type, and one in defns which is a name AND A
            //DIFFERENT DEFAULT VALUE, from the specified type, which overrides
            //the default value of the supertype.  We want to only keep the
            //BasicComponentTypeDefn corresponding to the sub-type name and default
            //value.
            //For example, type "JDBCConnector" has a ComponentType for the
            //property called "ServiceClassName" and a default value equal to
            //"com.metamatrix.connector.jdbc.JDBCConnectorTranslator".  The
            //super type "Connector" also defines a "ServiceClassName" defn,
            //but defines no default values.  Or worse yet, it might define
            //in invalid default value.  So we only want to keep the right
            //defn and value.

            Iterator inheritedIter =  inheritedDefns.iterator();
            Iterator localIter = defns.iterator();

            ComponentTypeDefn inheritedDefn = null;
            ComponentTypeDefn localDefn = null;

            while (localIter.hasNext()){
                localDefn = (ComponentTypeDefn)localIter.next();
                while (inheritedIter.hasNext()){
                    inheritedDefn = (ComponentTypeDefn)inheritedIter.next();
                    if (localDefn.getPropertyDefinition().equals(inheritedDefn.getPropertyDefinition())){
                        inheritedIter.remove();
                    }
                }
                inheritedIter = inheritedDefns.iterator();
            }

            defns.addAll(inheritedDefns);


            return defns;
        }

      
    
/**
 * This method calls itself recursively to return a Collection of all
 * ComponentTypeDefn objects defined for the super-type of the
 * componentTypeID parameter.  The equality of each PropertyDefn object contained
 * within each ComponentTypeDefn is the criteria to determine if a
 * defn exists in the sub-type already, or not.
 * @param defnMap Map of PropertyDefn object to the ComponentTypeDefn
 * object containing that PropertyDefn
 * @param defns return-by-reference Collection, built recursively
 * @param componentTypes Collection of all possible ComponentType
 * objects
 * @param componentTypeID the type for which super-type ComponentTypeDefn
 * objects are sought
 * @param transaction The transaction to operate within
 * @return Collection of all super-type ComponentTypeDefn objects (which
 * are <i>not</i> overridden by sup-types)
 */
private Collection getSuperComponentTypeDefinitions(Map defnMap, Collection defns,
                                            ComponentType type)  {
    if (defnMap == null) {
        defnMap = new HashMap();
    }

    if (defns == null) {
        defns = new ArrayList();
    }

    if (type == null) {
        return defns;
    }

    if (type.getSuperComponentTypeID() == null) {
        return defns;
    }


    ComponentType superType = getComponentType(type.getSuperComponentTypeID().getFullName());
    if (superType == null) {
        return defns;
    }
    Collection superDefns = superType.getComponentTypeDefinitions(); 
    // add the defns not already defined to the map
    ComponentTypeDefn sDefn;
    if (superDefns != null && superDefns.size() > 0) {
        Iterator it = superDefns.iterator();
        while (it.hasNext()) {
            sDefn = (ComponentTypeDefn) it.next();
            //this map has been changed to be keyed
            //on the PropertyDefn object of a ComponentTypeDefn,
            //instead of the i.d. of the ComponentTypeDefn
            if (!defnMap.containsKey(sDefn.getPropertyDefinition())) {
                defnMap.put(sDefn.getPropertyDefinition(), sDefn);
                defns.add(sDefn);
            }
        }
    }

    return getSuperComponentTypeDefinitions(defnMap, defns, superType);
   }
   



	public ComponentType getComponentType(String fullName) {
    	if (compTypes.containsKey(fullName)) {
	        return (ComponentType) compTypes.get(fullName);
    	}

    	return null;

    }
    
    public Properties getDefaultPropertyValues(ComponentTypeID componentTypeID) {
        Properties result = new Properties();
        result = getDefaultPropertyValues(result, componentTypeID);
        return result; 
         
     }
    
    public Properties getDefaultPropertyValues(Properties defaultProperties, ComponentTypeID componentTypeID) {
    	Properties result = new Properties(defaultProperties);
        
    	Collection defns = getAllComponentTypeDefinitions(componentTypeID);
        
        for (Iterator it=defns.iterator(); it.hasNext();) {
            ComponentTypeDefn ctd = (ComponentTypeDefn) it.next();
            
            Object value = ctd.getPropertyDefinition().getDefaultValue();
            if (value != null) {
                    if (value instanceof String) {
                        String v = (String) value;
                        if (v.trim().length() > 0) {
                        result.put(ctd.getPropertyDefinition().getName(), v );
                        }
                    } else {
                    result.put(ctd.getPropertyDefinition().getName(), value.toString() );
                        
                    }
            }   
        }
        return result;     	
    }
    
    

   public Collection getHosts() {
       Collection hosts = new ArrayList(this.configuration.getHosts().size());
       hosts.addAll(this.configuration.getHosts());

       return hosts;

   }

   public Host getHost(String fullName) {
   		try {
	   		return this.configuration.getHost(fullName);
   		} catch (Exception e) {
   			return null;
   		}

   }

   public Collection getConnectionPools() {
       Collection pools = new ArrayList(this.configuration.getResourcePools().size());
       pools.addAll(this.configuration.getResourcePools());

     return pools;
   }


   public SharedResource getResource(String resourceName) {
   		if (resources.containsKey(resourceName)) {
   			return (SharedResource) resources.get(resourceName);
   		}
   		return null;
   }

   public Collection getResources() {
   		Collection result = new ArrayList(resources.size());
   		result.addAll(resources.values());
   		return result;
   }


   public void setComponentTypes(Map newCompTypes) {
        this.compTypes = Collections.synchronizedMap(new HashMap(newCompTypes.size()));

   		Iterator it = newCompTypes.values().iterator();
   		while (it.hasNext()) {
   			addComponentType((ComponentType) it.next());
   		}
     }


   public void setResources(Map theResources) {
         this.resources = Collections.synchronizedMap(new HashMap(theResources.size()));
         this.resources.putAll(theResources);

   }

   public void setResources(Collection theResources) {
         this.resources = Collections.synchronizedMap(new HashMap(theResources.size()));

         for (Iterator it=theResources.iterator(); it.hasNext(); ) {
         	SharedResource rd = (SharedResource) it.next();
         	this.resources.put(rd.getID().getFullName(), rd);
         }
   }



/**
 * NOTE: The following 2 methods are provided here because the editor does not
 *		 interact with the ConfiguratonModelContainer.  And these are objects
 * 		 that are not configuration bound and live outside of the Configuration object.
 * 		 This is why Host is not here, because it does live within the configuration
 * 		 even though it is not configuration bound.
 */

    public void addComponentType(ComponentType type) {
	      	compTypes.put(type.getFullName(), type);
    }
    

    public void addResource(SharedResource rd) {
		resources.put(rd.getID().getFullName(), rd);
    }


    public void addObjects(Collection objects) throws ConfigurationException {
        setConfigurationObjects(objects);
    }
    
    public void addObject(Object obj) throws ConfigurationException {

        if (obj instanceof ServiceComponentDefn) {
            ServiceComponentDefn scd = (ServiceComponentDefn) obj;

        	if (configuration == null) {
        		throw new ConfigurationException(ErrorMessageKeys.CONFIG_0001, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_0001));
        	}
    		configuration.addComponentDefn(scd);

        } else if(obj instanceof DeployedComponent) {
            DeployedComponent deployedComp = (DeployedComponent) obj;

        	if (configuration == null) {
        		throw new ConfigurationException(ErrorMessageKeys.CONFIG_0001, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_0001));
        	}

        	configuration.addDeployedComponent(deployedComp);

        } else if (obj instanceof VMComponentDefn) {
            VMComponentDefn vm = (VMComponentDefn) obj;

        	if (configuration == null) {
        		throw new ConfigurationException(ErrorMessageKeys.CONFIG_0001, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_0001));
        	}

        	configuration.addComponentDefn(vm);

        } else if (obj instanceof Host) {
            Host host = (Host) obj;
        	if (configuration == null) {
        		throw new ConfigurationException(ErrorMessageKeys.CONFIG_0001, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_0001));
        	}

        	configuration.addHost(host);

        } else if (obj instanceof SharedResource) {

            SharedResource rd = (SharedResource) obj;

        	addResource(rd);
        	
        } else if (obj instanceof AuthenticationProvider) {

        	AuthenticationProvider rd = (AuthenticationProvider) obj;

        	configuration.addComponentDefn(rd);
 

        } else if (obj instanceof ResourceDescriptor) {

            ResourceDescriptor rd = (ResourceDescriptor) obj;

        	if (configuration == null) {
        			throw new ConfigurationException(ErrorMessageKeys.CONFIG_0001, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_0001));
        	}

        	configuration.addComponentDefn(rd);


        } else if (obj instanceof Configuration) {
			if (this.configuration == null) {
					this.configuration = (BasicConfiguration) obj;
			} else {
        			throw new ConfigurationException(ErrorMessageKeys.CONFIG_0002, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_0002));

			}

        } else if (obj instanceof ComponentType) {
            addComponentType((ComponentType) obj);


        } else {

            throw new ConfigurationException(ErrorMessageKeys.CONFIG_0003, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_0003, obj.getClass().getName() ));
        }

    }

   /**
     * Method used when the XMLConfiguration is initialized for the first time.
     * Otherwsise should call updateConfigurationObjects.
     */
     public void setConfigurationObjects(Collection objects) throws ConfigurationException {

        if (this.configuration != null) {
        	throw new ConfigurationException(ErrorMessageKeys.CONFIG_0004, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_0004));
        }
        // first time thru update the configuration first
        // and load the component types

        Iterator configIt = objects.iterator();
        while (configIt.hasNext()) {
            Object obj = configIt.next();
            if (obj instanceof Configuration) {
                BasicConfiguration config = (BasicConfiguration) obj;
				this.configuration = config;
            } else {
                addObject(obj);
               
            } 
        }
        
        Properties allprops = this.getDefaultPropertyValues(this.configuration.getComponentTypeID());
        allprops.putAll(this.configuration.getProperties());
        allprops.putAll(System.getProperties());
        
        this.configuration.setProperties(allprops);
            
    }
     
     public void remove(BaseID objID) throws ConfigurationException {

         if (objID instanceof ServiceComponentDefnID) {
             remove((ServiceComponentDefnID) objID);

         } else if(objID instanceof DeployedComponentID) {
             remove((DeployedComponentID) objID);

         } else if (objID instanceof VMComponentDefnID) {
             remove((VMComponentDefnID) objID);

         } else if (objID instanceof HostID) {
             remove((HostID) objID);

         } else if (objID instanceof SharedResourceID) {
             removeSharedResource((SharedResourceID) objID);

         } else if (objID instanceof ResourceDescriptorID) {
             remove((ResourceDescriptorID) objID);


         } else if (objID instanceof ComponentTypeID) {
             removeComponentType((ComponentTypeID) objID);

         } else {
             throw new ConfigurationException(ErrorMessageKeys.CONFIG_0018, 
                 CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_0018, objID.getClass().getName()));
         }

     }
     

    private void removeComponentType(ComponentTypeID typeID) {
    	if (compTypes.containsKey(typeID.getFullName())) {
    		compTypes.remove(typeID.getFullName());
    	}

    }
    
 

    private void removeSharedResource(SharedResourceID rdID) {
    	if (resources.containsKey(rdID.getFullName())) {
    		resources.remove(rdID.getFullName());
    	}
    }

    private void remove(ServiceComponentDefnID defnID) throws ConfigurationException {

        	if (configuration == null) {
        		throw new ConfigurationException(ErrorMessageKeys.CONFIG_0001, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_0001));
        	}

			delete(defnID, configuration);
	}

    private void remove(VMComponentDefnID defnID) throws ConfigurationException {

        	if (configuration == null) {
        		throw new ConfigurationException(ErrorMessageKeys.CONFIG_0001, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_0001));
        	}

			delete(defnID, configuration);
    }

    private void remove(DeployedComponentID dcID) throws ConfigurationException {

        	if (configuration == null) {
        		throw new ConfigurationException(ErrorMessageKeys.CONFIG_0001, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_0001));
        	}

			delete(dcID, configuration);
    }

    private void remove(HostID hostID) throws ConfigurationException {

        	if (configuration == null) {
        		throw new ConfigurationException(ErrorMessageKeys.CONFIG_0001, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_0001));
        	}

			delete(hostID, configuration);
    }

    private void remove(ResourceDescriptorID rdID) throws ConfigurationException {

        	if (configuration == null) {
        		throw new ConfigurationException(ErrorMessageKeys.CONFIG_0001, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_0001));
        	}

			delete(rdID, configuration);
     }


    public Object clone() {

        Configuration config = (Configuration) configuration.clone();

        ConfigurationModelContainerImpl newConfig = new ConfigurationModelContainerImpl(config);
		newConfig.setComponentTypes(this.compTypes);
        newConfig.setResources(this.resources);

        return newConfig;
    }
    
	private Configuration delete( ComponentObjectID targetID, Configuration configuration ) throws ConfigurationException {
 
        BasicConfiguration basicConfig = (BasicConfiguration) configuration;
        basicConfig.removeComponentObject( targetID);

        return basicConfig;
    }   
    

}
