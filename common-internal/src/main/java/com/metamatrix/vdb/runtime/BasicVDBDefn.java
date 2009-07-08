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

package com.metamatrix.vdb.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.model.BasicUtil;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.common.vdb.api.VDBDefn;
import com.metamatrix.common.vdb.api.VDBStream;
import com.metamatrix.core.vdb.VDBStatus;

/**
 * Date Dec 3, 2002
 *
 *  To import a VDB, the following information is required:
 *  1. VDB Jar {@link #setVDBJar(Object)}.  This jar will provide
 *     the model and their model info. 
 *  2. Add a model to connector binding mapping {@link #addModelToConnectorMapping(String, String)}
 *     that indicates which binding should be used for that model
 *  3. set VDB name (version will be assign at creation time)
 * 
 *  Optional information:
 *  1. Add a ConnectorBinding {@link #addConnectorBinding(ConnectorBinding)}. If this
 *     already exist in the configration, it will not be loaded.  The
 *     model will reference the existing binding.
 *     If not added, the connector binding mapping will indicate an 
 *     assumed already existing binding.
 *  2. Add a ConnectorType {@link #addConnectorType(ComponentType). If
 *     already exist in the configration, it will not be loaded.  The
 *     binding will reference the existing type.
 */
public class BasicVDBDefn extends BasicVDBInfo implements VDBDefn {

    /**
     * The connector types are optional, if
     * not specified on the import, then the
     * connector type(s) is assumed to already 
     * exist in configuration
     * <key> connector type name <value> ComponentType {@see com.metamatrix.common.config.api.ComponentType }
     */
    private Map connectorTypes = null;

    /**
     * The connector bindings are optional, if
     * not specified on the import, then the
     * connector binding(s) is assumed to already 
     * exist in configuration
     * <key> connector binding name <value> ConnectorBinding {@see com.metamatrix.common.config.api.ConnectorBinding }
     */
    private Map connectorBindings = null;

    /**
     * contains the VDB Jar.
     */
    private VDBStream vdbcontent = null;
    
    // list of validity errors.
    private ArrayList validityErrors = null;
    
   
    /**
     * Controls when the VDB is imported whether
     * its status will be set to active or not. 
     */
    private short status = VDBStatus.INCOMPLETE;
    
    private boolean invalidVDBorModel = false;
    
    // Visibility information on the VDB resources.
    private Map visibilityMap = null;
    
    private char[] dataroles;
    
    private Properties headerProperties;
    
    private Properties infoProperties;
    
    public BasicVDBDefn(String name) {
        super(name);
    }
    
    /** 
     * @see com.metamatrix.common.vdb.api.VDBDefn#getVDBStream()
     * @since 4.3
     */
    public VDBStream getVDBStream() {
        return vdbcontent;
    }
    
    public void setVDBStream(VDBStream stream) {
    	this.vdbcontent = stream;
    }
    
    public boolean doesVDBHaveValidityError() {
        return this.invalidVDBorModel;
    }
          
    /**
     * Call to add the binding to the VDBDefn set of bindings and
     * add the binding to the model-to-binding mapping. 
     * @param modelName
     * @param binding
     * @since 4.2
     */
    public void addConnectorBinding(String modelName, ConnectorBinding binding) {
        addConnectorBinding(binding);
        BasicModelInfo model = (BasicModelInfo) this.getModel(modelName);
        if (model != null) {
            model.addConnectorBindingByName(binding.getFullName());
        }
    }

    public void addConnectorBinding(ConnectorBinding binding) {
        if (connectorBindings == null) {
            connectorBindings = new HashMap();
        }
        connectorBindings.put(binding.getName(), binding);

    }
    
    public void renameConnectorBinding(String existingBindingName, String newBindingName) {
        ConnectorBinding cb = removeConnectorBindingFromMap(existingBindingName);
        if (cb != null) {
            
            // go thru each model and rename the binding where it is used
            Collection ms = this.getModels();
            for (Iterator it=ms.iterator(); it.hasNext();) {
                BasicModelInfo model = (BasicModelInfo) it.next();
                if (model.isConnectorBindingUsed(cb.getFullName())) {
                    model.renameConnectorBinding(existingBindingName, newBindingName);
                }
            }
            
            // rename the binding and add it back
            ConnectorBinding newcb = BasicUtil.getEditor().createConnectorComponent(cb.getConfigurationID(), cb, newBindingName, cb.getRoutingUUID()); 
            addConnectorBinding(newcb);
            
         }

        
    }
    
    public void removeConnectorBinding(String bindingName) {
        if (connectorBindings != null) {
            ConnectorBinding cb = this.getConnectorBindingByName(bindingName);
            if (cb != null) {
                // remove the binding from any model that it is associated with
                Collection ms = this.getModels();
                for (Iterator it=ms.iterator(); it.hasNext();) {
                    BasicModelInfo model = (BasicModelInfo) it.next();
                    if (model.isConnectorBindingUsed(cb.getFullName())) {
                        model.removeConnectorBindingName(cb.getFullName());
                    }
                }
                removeFromAvailableConnectorBindings(bindingName);

             }
        }
    }
    
    public void removeConnectorBindingNameOnly(String bindingName) {
        if (connectorBindings != null) {
            ConnectorBinding cb = this.getConnectorBindingByName(bindingName);
            if (cb != null) {
                // remove the binding from any model that it is associated with
                Collection ms = this.getModels();
                for (Iterator it=ms.iterator(); it.hasNext();) {
                    BasicModelInfo model = (BasicModelInfo) it.next();
                    if (model.isConnectorBindingUsed(cb.getFullName())) {
                        model.removeConnectorBindingName(cb.getFullName());
                    }
                }
                removeConnectorBindingFromMap(bindingName);
             }
        }
    }

    private ConnectorBinding removeConnectorBindingFromMap(String bindingName) {
        ConnectorBinding cb = null;
        if (connectorBindings != null) {
            cb = (ConnectorBinding) connectorBindings.remove(bindingName);
        }
        return cb;
   
    }
    
    private void removeFromAvailableConnectorBindings(String bindingName) {
        ConnectorBinding cb = removeConnectorBindingFromMap(bindingName);
        if (cb != null) {
            if (!isConnectorTypeInUse(cb.getComponentTypeID().getFullName())) {
                removeConnectorType(cb.getComponentTypeID().getFullName());
            }
        }

    }
    
    public void removeConnectorBinding(String modelName, String bindingName) {
        if (connectorBindings != null) {
            ConnectorBinding cb = getConnectorBindingByName(bindingName);
            if (cb != null) {
                BasicModelInfo model = (BasicModelInfo) this.getModel(modelName);
                if (model != null) {                    
                    model.removeConnectorBindingName(cb.getFullName());
                }
                // if the connector binding is no referenced by any model
                // then remove
                if (!isBindingInUse(cb)) {
                    removeFromAvailableConnectorBindings(cb.getFullName());
                }
            }
        }
    }    
    
    public boolean isBindingInUse(ConnectorBinding binding) {
        
//        if (binding.getRoutingUUID() == null) {
//            ArgCheck.isNotNull(binding.getRoutingUUID(), "ConnectorBinding routing UUID must not be null"); //$NON-NLS-1$
//        }
        Collection ms = this.getModels();
        for (Iterator it=ms.iterator(); it.hasNext();) {
            BasicModelInfo model = (BasicModelInfo) it.next();
            if (model.isConnectorBindingUsed(binding.getFullName())) {
                return true;
            }
        }

        return false;
    }
    
    public boolean isBindingInUse(String bindingName) {        
      Collection ms = this.getModels();
      for (Iterator it=ms.iterator(); it.hasNext();) {
          BasicModelInfo model = (BasicModelInfo) it.next();
          if (model.isConnectorBindingUsed(bindingName)) {
              return true;
          }
      }
      return false;
  }    
    
    private boolean isConnectorTypeInUse(String typeName) {
        if (connectorTypes == null) {
            return true;
        }
        
        Iterator it = this.connectorBindings.values().iterator();
        while (it.hasNext()) {
            ConnectorBinding cb = (ConnectorBinding) it.next(); 
            if (cb.getComponentTypeID().getFullName().equalsIgnoreCase(typeName)) {
                return true;
            }
        }
        return false;
        
    }
    
    /**
     * Call to add the binding to the VDBDefn set of bindings and
     * initializes the set of model-to-binding mappings
     * with the binding. 
     * @param modelName
     * @param binding
     * @since 4.2
     */
    
    public void setConnectorBinding(String modelName, ConnectorBinding binding) {
        addConnectorBinding(binding);
        BasicModelInfo model = (BasicModelInfo) this.getModel(modelName);
        if (model != null) {
            ArrayList names = new ArrayList(1);
            names.add(binding.getFullName());
            model.setConnectorBindingNames(names);
        }
    }
    
    protected void setConnectorBindings(Map bindings) {
        if (bindings != null) {
            if (connectorBindings == null) {
                connectorBindings = new HashMap(bindings.size());
            }
    
            connectorBindings.putAll(bindings);
        }
    }
    
    
    protected void setConnectorTypes(Map types) {
        if (types != null) {
            if (connectorTypes == null) {
                connectorTypes = new HashMap(types.size());
            }
            connectorTypes.putAll(types);
        }
    }
    
    public void removeConnectorType(String typeName) {
        if (connectorTypes != null) {
            connectorTypes.remove(typeName);
        }
    }

    public void addConnectorType(ComponentType type) {
        if (connectorTypes == null) {
            connectorTypes = new HashMap();
        }
        connectorTypes.put(type.getName(), type);

    }

    public Map getConnectorTypes() {
        Map m = new HashMap();
        if (this.connectorTypes != null) {
            m.putAll(this.connectorTypes);
        }        
        return m;        

    }
    
    public ComponentType getConnectorType(String componentTypeName) {
        if (this.connectorTypes == null) {
            return null;    
        }
        if (this.connectorTypes.containsKey(componentTypeName)) {
            return (ComponentType) this.connectorTypes.get(componentTypeName);
        }
        
        return null;
    }


    public Map getConnectorBindings() {
        Map m = new HashMap();
        if (this.connectorBindings != null) {
            m.putAll(this.connectorBindings);
        }        
        return m;        

    }

    public ConnectorBinding getConnectorBindingByRouting(String routingUUID) {
        if (this.connectorBindings == null) {
            return null;
        }
        Iterator it = this.connectorBindings.values().iterator();
        while (it.hasNext()) {
            ConnectorBinding cb = (ConnectorBinding) it.next();            
            if (cb.getRoutingUUID().equals(routingUUID)) {
                return cb;
            }
        }
        
        return null;
    }
    
    public ConnectorBinding getConnectorBindingByName(String bindingName) {
        if (this.connectorBindings == null) {
            return null;
        }
        
        ConnectorBinding cb = (ConnectorBinding) this.connectorBindings.get(bindingName);
        return cb;
        
    }    
    

    public Map getModelToBindingMappings() {
        Map bm = new HashMap();
        
        Collection mdlsd = getModels();
         for (Iterator it=mdlsd.iterator(); it.hasNext(); ) {
             ModelInfo m = (ModelInfo) it.next();
             
             
             if(m.getConnectorBindingNames().size() > 0) {
                 List uuids = new ArrayList();
                 for (Iterator bits=m.getConnectorBindingNames().iterator(); bits.hasNext();) {
                     String name = (String) bits.next();
                     ConnectorBinding cb = (ConnectorBinding) this.connectorBindings.get(name);
                     uuids.add(cb.getRoutingUUID());
                 }
                  bm.put(m.getName(), uuids);
                    
             } else if(m.getConnectorBindingNames().size() > 0) {
                 List uuids = new ArrayList();
                 for (Iterator bits=m.getConnectorBindingNames().iterator(); bits.hasNext();) {
                     String name = (String) bits.next();
                     ConnectorBinding cb = (ConnectorBinding) this.connectorBindings.get(name);
                     uuids.add(cb.getRoutingUUID());
                 }
                  bm.put(m.getName(), uuids);
                     
             }
                        
         }
        
         return bm;        

    }
    
 
    /**
     * Returns the collection of all the model names contained
     * in the vdb archive.
     * @return Collection
     * 
     */
    public Collection getModelNames() {

        Collection mdlsd = getModels();
        Collection mdls = new ArrayList(mdlsd.size());
        for (Iterator it=mdlsd.iterator(); it.hasNext(); ) {
            ModelInfo m = (ModelInfo) it.next();
            mdls.add(m.getName());
        }
        
        return mdls;
    }
    
    public ModelInfo removeModelInfo(String modelName, boolean removeBindings) {
        
        ModelInfo m = super.removeModelInfo(modelName); 
            //this.getModel(modelName);
        if (m!= null && removeBindings) {
            Collection names = m.getConnectorBindingNames();
            for (Iterator it=names.iterator(); it.hasNext(); ) {
                final String cbName= (String) it.next();
                ConnectorBinding cb = getConnectorBindingByName(cbName);
                if (cb !=null) {
                    if (!isBindingInUse(cb)) {
                        removeConnectorBinding(cb.getFullName());
                    }

                }
                
            }
             
        }
        return m;
    }
    
    public ModelInfo removeModelInfo(String modelName) {
    	return removeModelInfo(modelName,true);
    }
    
    /** 
     * @see com.metamatrix.common.vdb.api.VDBDefn#getMatertializationModel()
     * @since 4.2
     */
    public ModelInfo getMatertializationModel() {
        ModelInfo matModel = null;
        Iterator modelItr = getModels().iterator();
        while ( modelItr.hasNext() ) {
            ModelInfo aModel = (ModelInfo)modelItr.next();
            if ( aModel != null && aModel.isMaterialization() ) {
                matModel = aModel;
                break;
            }
        }
        return matModel;
    }
    
    public short getStatus() {
        if (invalidVDBorModel) {
            return VDBStatus.INCOMPLETE;
        }
        
        return this.status;    
    }
    
   /**
    * Set the status of the VDB
    * see {@link  MetadataConstants.VDB_STATUS}
    * @param status
    * @since 4.2
    */
    public void setStatus(short status) {
        this.status = status;
    }
    
    public boolean isActiveStatus() {  
        // as long as it has a validity error, it cannot be active
        if (invalidVDBorModel) {
            return false;
        }
        if (this.status == VDBStatus.ACTIVE || this.status == VDBStatus.ACTIVE_DEFAULT) {
            return true;
        }
        return false;
    }
    
    public void setVDBValidityError(boolean hasError) {
        invalidVDBorModel = hasError;
    }

    public void setVDBValidityError(boolean hasError, String msg) {
        invalidVDBorModel = hasError;
        if (this.validityErrors == null) {
            this.validityErrors = new ArrayList();
        }
        if (msg != null) {
            this.validityErrors.add(msg);
        }
    }
    
    public String[] getVDBValidityErrors() {
        if (this.validityErrors != null) {
            return (String[])this.validityErrors.toArray(new String[this.validityErrors.size()]);
        }
        return new String[0];
    }    
    
    public void determineVdbsStatus() {        
        if (invalidVDBorModel) {
            setStatus( VDBStatus.INCOMPLETE); 
            return;                   
        }
        Map mapConnBind = this.getModelToBindingMappings();
        // examine the map looking for any models that do
        // not have a connector binding specified.
        // if any missing, return:
        // MetadataConstants.VDB_STATUS.INCOMPLETE
        // if all are present, return
        // MetadataConstants.VDB_STATUS.INACTIVE
        // how about this... just look for values that are ""?
        short siStatus;
        boolean bHasMissingValues = false;
        boolean bHasNoBindings = false;
        boolean requiresBindings = false;

        // verify that at least one model requires a binding
        Iterator mit = getModels().iterator();
        while (mit.hasNext()) {
            ModelInfo mi = (ModelInfo)mit.next();
            if (mi.requiresConnectorBinding()) {
                requiresBindings = true;
                break;
            }
        }

        // if no binding is required, then the vdb can
        // be considered for active status
        if (!requiresBindings) {
            setStatus(VDBStatus.INACTIVE);  
            return;
        }

        int connBind = 0;
        Iterator it = mapConnBind.values().iterator();

        // Count the number of bindings
        while (it.hasNext()) {
            List list = (List)it.next();
            if ((list == null) || (list.size() == 0)) {
                bHasMissingValues = true;
            } else {
                connBind += list.size();
            }
        }

        if (connBind == 0) {
            bHasNoBindings = true;
        }

        if (bHasNoBindings || bHasMissingValues) {
            siStatus = VDBStatus.INCOMPLETE;
        } else {
            siStatus = VDBStatus.ACTIVE;
        }
        setStatus(siStatus);  

    }
  
    
    /**
     * Remove ConnectorBindings from this VDBDefn that are not referenced by any of the ModelInfos 
     * 
     * @since 4.3
     */
    public synchronized void removeUnmappedBindings() {
        //build up list of all binding names referred to by a model
        HashSet allBindingNames = new HashSet();
        for (Iterator iter = getModels().iterator(); iter.hasNext(); ) {
            ModelInfo model = (ModelInfo) iter.next();
            allBindingNames.addAll(model.getConnectorBindingNames());
        }
        
        //remove any connector binding that's not referred to
        for (Iterator iter = getConnectorBindings().values().iterator(); iter.hasNext(); ) {
            ConnectorBinding binding = (ConnectorBinding) iter.next();
            String bindingName = binding.getFullName();
            
            if (! allBindingNames.contains(bindingName)) {
                connectorBindings.remove(bindingName);
            }
        }
        
    }

    /** 
     * @see com.metamatrix.common.vdb.api.VDBDefn#isVisible(java.lang.String)
     */
    public boolean isVisible(String resourcePath) { 
        if (visibilityMap == null) {
            return false;
        }
        
        Boolean visibility = (Boolean)visibilityMap.get(resourcePath);
        if (visibility == null && resourcePath.startsWith("/")) { //$NON-NLS-1$
            visibility = (Boolean)visibilityMap.get(resourcePath.substring(1));
        }
        
        if (visibility == null || visibility.equals(Boolean.FALSE)) {
            return false;
        }
        return true;        
    }

    public void setVisibility(String resoucePath, boolean visible) {
        if (visibilityMap == null) {
            visibilityMap = new HashMap();
        }
        visibilityMap.put(resoucePath, visible?Boolean.TRUE:Boolean.FALSE);
    }
    

    /** 
     * @see com.metamatrix.common.vdb.api.VDBDefn#getDataRoles()
     */
    public char[] getDataRoles() {
        return dataroles;
    }
    
    public void setDataRoles(char[] roles) {
        this.dataroles = roles;
    }
    
    public void renameModelInfo(String modelName, String newModelName) {
        if (modelName == null || modelName.length() == 0) {
            return;
        }
        if (this.modelInfos != null) {
            BasicModelInfo bmi = (BasicModelInfo) modelInfos.remove(modelName);
            if (bmi != null) {
                bmi.setName(newModelName);
                addModelInfo(bmi);
            }
        }
    }

	public Properties getHeaderProperties() {
		return headerProperties;
	}

	public void setHeaderProperties(Properties props) {
		this.headerProperties = props;
	}      
	
	public Properties getInfoProperties() {
		return infoProperties;
	}

	public void setInfoProperties(Properties props) {
		this.infoProperties = props;
	}
}
