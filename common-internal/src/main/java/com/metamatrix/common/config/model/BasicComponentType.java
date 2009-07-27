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
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.namedobject.BasicObject;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.core.util.DateUtil;

public class BasicComponentType extends BasicObject implements ComponentType, Serializable {
    
    public static final long serialVersionUID = 5972706380742168742L;
        
    private HashMap typeDefinitions;
    private HashMap typeDefnsByName;
    private ComponentTypeID parentTypeID;
    private ComponentTypeID superTypeID;
    private boolean isDeployable;
    private boolean isDeprecated;
    private boolean isMonitored;
    private int componentTypeCode = -1;
    private String description = null;
    
// The String dates are the values being passed thru the system and are the ones
// read from and saved to the config model
// The Date type dates are only for convience (i.e., modeler)    
    private String createdBy;
    private String createdDate;
    private Date createdDDate;
    
    private String lastChangedBy;
    private String lastChangedDate;
    private Date lastChangedDDate;
    

    BasicComponentType(ComponentTypeID id, ComponentTypeID parentID, ComponentTypeID superID, boolean deployable, boolean deprecated, boolean monitored) {
        super(id);
        this.parentTypeID = parentID;
        this.superTypeID = superID;
        this.isDeployable = deployable;
        this.isDeprecated = deprecated;
        this.isMonitored = monitored;
    }


    protected BasicComponentType(BasicComponentType type) {
        super(type.getID());

        setIsDeployable(type.isDeployable());
        setIsDeprecated(type.isDeprecated());
        setIsMonitored(type.isMonitored());
        setComponentTypeCode(type.getComponentTypeCode());
        if (type.getParentComponentTypeID() != null) {
            setParentComponentTypeID(type.getParentComponentTypeID());
        }

        if (type.getSuperComponentTypeID() != null) {
            setSuperComponentTypeID(type.getSuperComponentTypeID());
        }
        this.setCreatedBy(type.getCreatedBy());
        this.setCreatedDate(type.getCreatedDateString());
        this.setLastChangedBy(type.getLastChangedBy());
        this.setLastChangedDate(type.getLastChangedDateString());
    }


    public Collection getComponentTypeDefinitions() {
        if (typeDefinitions == null) {
            typeDefinitions = new HashMap(10);
            typeDefnsByName = new HashMap(10);
        } 

        Collection tds = new ArrayList(typeDefinitions.size());
        tds.addAll(typeDefinitions.values());
	    return tds;
    }
    
    
    public ComponentTypeDefn getComponentTypeDefinition(String name) {
        if (typeDefnsByName != null) {
            return (ComponentTypeDefn) typeDefnsByName.get(name) ;
        }
        return null;
    }
    
    public String getDefaultValue(String propertyName) {
        ComponentTypeDefn ctd = getComponentTypeDefinition(propertyName);
        if (ctd != null) {
            Object value = ctd.getPropertyDefinition().getDefaultValue();
            if (value != null) {
                    if (value instanceof String) {
                        String v = (String) value;
                        return v;
                    } 
                    return value.toString();
            }   
        }
        return null;
    }

    
    public Properties getDefaultPropertyValues() {
        Properties result = new Properties();
        result = getDefaultPropertyValues(result);
        return result; 
         
     }
    
    public Properties getDefaultPropertyValues(Properties props) {
        Collection defns = getComponentTypeDefinitions();
        
        for (Iterator it=defns.iterator(); it.hasNext();) {
            ComponentTypeDefn ctd = (ComponentTypeDefn) it.next();
            
            Object value = ctd.getPropertyDefinition().getDefaultValue();
            if (value != null) {
                    if (value instanceof String) {
                        String v = (String) value;
                        if (v.trim().length() > 0) {
                        props.put(ctd.getPropertyDefinition().getName(), v );
                        }
                    } else {
                    props.put(ctd.getPropertyDefinition().getName(), value.toString() );
                        
                    }
            }   
        }
        return props;     	
    }
    
    
    /** 
     * @see com.metamatrix.common.config.api.ComponentType#getMaskedPropertyNames()
     * @since 4.3
     */
    public Collection getMaskedPropertyNames() {
        Collection maskedPropNames = new ArrayList();
        Collection defns = getComponentTypeDefinitions();
        
        for (Iterator it=defns.iterator(); it.hasNext();) {
            ComponentTypeDefn ctd = (ComponentTypeDefn) it.next();
            PropertyDefinition propertyDefinition = ctd.getPropertyDefinition();
            String name = propertyDefinition.getName();
            
            if (propertyDefinition.isMasked()) {
                maskedPropNames.add(name);
            }
        }
        return maskedPropNames;
    }


    /**
     * Returns the description, if it has one, of the component type
     * @return String description
     * @since 4.2
     */
    public String getDescription() {
        return description;
    }

    public ComponentTypeID getParentComponentTypeID() {
        return parentTypeID;
    }

    public ComponentTypeID getSuperComponentTypeID() {
        return superTypeID;
    }

    public int getComponentTypeCode() {
        return componentTypeCode;
    }

    public boolean isDeployable() {
        return this.isDeployable;
    }

    public boolean isDeprecated() {
        return isDeprecated;
    }

    public boolean isMonitored() {
        return isMonitored;
    }
    
    public boolean isOfTypeConnector() {
        return false;
    }
    

    /**
     * Returns the principal who created this type
     * @return String principal name 
     */
    public String getCreatedBy(){
        return this.createdBy;
    }

    public void setCreatedBy(String createdBy){
        this.createdBy = createdBy;
    }

    /**
     * Returns the Date this type was created
     * @return Date this type was created
     */
    public Date getCreatedDate(){
        return this.createdDDate;
    }
    
    public String getCreatedDateString() {
    	return this.createdDate;
    }
    

    public void setCreatedDate(String createdDate){
        this.createdDate = createdDate;
        this.createdDDate = convertDate(createdDate);
        
    }

    /**
     * Returns the principal who last modified this type
     * @return String principal name
     */
    public String getLastChangedBy(){
        return this.lastChangedBy;
    }

    public void setLastChangedBy(String lastChangedBy){
        this.lastChangedBy = lastChangedBy;
    }

    /**
     * Returns the Date this type was last changed
     * @return Date this type was last changed
     */
    public Date getLastChangedDate(){
        return this.lastChangedDDate;
    }
    
    public String getLastChangedDateString(){
        return this.lastChangedDate;
    }
    

    public void setLastChangedDate(String lastChangedDate){
        this.lastChangedDate = lastChangedDate;
        this.lastChangedDDate = convertDate(lastChangedDate);
    }



    public void setComponentTypeDefinitions(Collection newDefinitions){

        if (newDefinitions == null) {
        	return;
        }

        typeDefinitions = new HashMap(newDefinitions.size());
        typeDefnsByName = new HashMap(newDefinitions.size());
        
        Collections.synchronizedMap(typeDefinitions);

        ComponentTypeDefn defn;
        for (Iterator it=newDefinitions.iterator(); it.hasNext(); ) {
            defn = (ComponentTypeDefn) it.next();
            typeDefinitions.put(defn.getID(), defn);
            typeDefnsByName.put(defn.getID().getFullName(), defn);
        }
    }

    public void setParentComponentTypeID(ComponentTypeID typeID) {
      parentTypeID = typeID;
    }

    public void setSuperComponentTypeID(ComponentTypeID typeID) {
      superTypeID = typeID;
    }

    void setIsDeployable(boolean deployable) {
      this.isDeployable = deployable;
    }

    public void setIsDeprecated(boolean deprecate) {
      this.isDeprecated=deprecate;
    }

    void setIsMonitored(boolean monitored) {
      this.isMonitored = monitored;
    }

    public void setComponentTypeCode(int code) {
      this.componentTypeCode = code;
    }
    
    public void setDescription(String desc) {
        this.description = desc;
      }    

    public void addComponentTypeDefinition(ComponentTypeDefn defn) {
        if (typeDefinitions == null) {
            typeDefinitions = new HashMap();
            typeDefnsByName = new HashMap(10);
            
        }

        Map defns = Collections.synchronizedMap(typeDefinitions);
        defns.put(defn.getID(), defn);
        typeDefnsByName.put(defn.getID().getName(), defn);
    }

    public void removeComponentTypeDefinition(ComponentTypeDefn defn) {
        if (typeDefinitions != null && typeDefinitions.containsKey(defn.getID())) {
            Map defns = Collections.synchronizedMap(typeDefinitions);
            defns.remove(defn.getID());
            typeDefnsByName.remove(defn.getID().getName());
        }
    }

    /**
     * Return a deep cloned instance of this object.  Subclasses must override
     *  this method.
     *  @return the object that is the clone of this instance.
     */
    public synchronized Object clone() {

        BasicComponentType result = null;
    	result = new BasicComponentType(this);

        Collection defns = this.getComponentTypeDefinitions();
        result.setComponentTypeDefinitions(defns);

        return result;

    }
    
    
    private static Date convertDate(String date) {
    	
		Date cd=null;
		if (date == null) {
			return new Date();
    	}		
     	try {
    	
     		cd = DateUtil.convertStringToDate(date);
    	

     	} catch (java.text.ParseException e) {
     		try {
     			cd = DateFormat.getInstance().parse(date);
     		} catch (java.text.ParseException iae) {
     				cd = new Date();	
     			
     		}

      	}
      	
      	return cd;
    	
    }
    
    public void accept(ConfigurationVisitor visitor) {
        visitor.visitComponent(this);
    }      
        
}
