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
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

import com.metamatrix.common.config.api.ComponentObject;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.namedobject.BaseID;
import com.metamatrix.common.namedobject.BasicObject;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.core.util.DateUtil;

public abstract class BasicComponentObject extends BasicObject implements ComponentObject, PropertiedObject, Serializable {
    
    private ComponentTypeID componentTypeID;
    private String description;
    
    
// The String dates are the values being passed thru the system and are the ones
// read from and saved to the config model
// The Date type dates are only for convience (i.e., modeler) 
    private String createdBy;
    private String createdDate;
    private Date createdDDate;
    private String lastChangedBy;
    private String lastChangedDate;
    private Date lastChangedDDate;
    
    
    private Properties properties = new Properties();

    public BasicComponentObject(BaseID componentID, ComponentTypeID typeID) {
        super(componentID);
	      componentTypeID = typeID;
	      
    }

    protected BasicComponentObject(BasicComponentObject component) {
        super(component.getID());
        setComponentTypeID(component.getComponentTypeID());
        setProperties(component.getEditableProperties());
        this.setCreatedBy(component.getCreatedBy());
        this.setCreatedDate(component.getCreatedDateString());
        this.setLastChangedBy(component.getLastChangedBy());
        this.setLastChangedDate(component.getLastChangedDateString());
        this.setDescription(component.getDescription());
        
        
    }

    public String getProperty(String name) {
        return properties.getProperty(name);
    }
    
    public ComponentTypeID getComponentTypeID(){
	    return componentTypeID;
    }

    public Properties getProperties(){
        Properties result = new Properties();
        result.putAll(this.properties);
  	    return result;
    }

    public boolean isDependentUpon(BaseID componentObjectId) {
        return false;
    }

    void setComponentTypeID(ComponentTypeID typeID){
	    componentTypeID = typeID;
    }

    /**
     * Set the properties for this object.  If the argument is null, an empty Properties
     * object is created.
     * @param properties the set of properties for this service; a reference to
     * this Properties instance should <i>not</i> be maintained by the caller
     */
    public void setProperties(Properties properties){
        Properties p = null;

        if (properties != null) {
            // Since the 'properties' object is mutable, we need to reserve a lock on it
            synchronized(properties) {
                p = (Properties)properties.clone();
            }
        } else {
            p = new Properties();
        }


        // Update the reference ...
        this.properties = p;

    }

/**
 * Add a name/value pair as a new property setting for the current <code>Configuration</code>. 
 */
    public void addProperty(String name, String value) {
	    properties.put(name, value);
    }
/**
 * Add a the newProperties to the current properties settings. 
 */
    public void addProperties(Properties newProperties) {
	    properties.putAll(newProperties);
    }
/**
 * Remove a property setting based on the name 
 */
    public void removeProperty(String name) {
	    properties.remove(name);
    }
/**
 * Remove the <code>Collection</code> of propertyNames from tthe current properties settings. 
 */
    public void removeProperties(Collection propertyNames) {
	    for (Iterator it = propertyNames.iterator(); it.hasNext(); ) {
		    removeProperty( (String) it.next() );
	    }
	
    }
    
    /**
     * Returns the description of this component definition
     * @return String description of this component definition
     */
    public String getDescription(){
        return this.description;
    }

    public void setDescription(String description){
        this.description = description;
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
    

    

 /**
 * Helper method to enable building the property hierarchy for deployed Components
 */
   public Properties getEditableProperties() {
      return this.properties;
    }

    /**
     * Returns a string representing the name of the object.  This has been
     * overriden for GUI display purposes - the Console only wants to display
     * the "name" (not the "fullname") of a component object.Me
     * @return the string representation of this instance.
     */
    public String toString(){
	    return this.getName();
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
