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
package org.teiid.adminapi.impl;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

import org.teiid.adminapi.AdminObject;

@XmlAccessorType(XmlAccessType.NONE)
public abstract class AdminObjectImpl implements AdminObject, Serializable {

	private static final long serialVersionUID = -6381303538713462682L;
	
	private String name;
		
	private ListOverMap<PropertyMetadata> properties = new ListOverMap<PropertyMetadata>(new KeyBuilder<PropertyMetadata>() {
		private static final long serialVersionUID = 3687928367250819142L;

		@Override
		public String getKey(PropertyMetadata entry) {
			return entry.getName();
		}
	});
	
	private transient Map<String, Object> attachments = Collections.synchronizedMap(new HashMap<String, Object>());
		
	@Override
	public String getName() {
		return this.name;
	}
	
	public void setName(String name) {
		this.name = name;
	}	

	@Override
	public Properties getProperties() {
		Properties props = new Properties();
		for (PropertyMetadata p:this.properties.getMap().values()) {
			props.setProperty(p.getName(), p.getValue());
		}
		return props;
	}
	
	public void setProperties(Properties props) {
		this.properties.clear();
		if (props != null) {
			for (String key:props.stringPropertyNames()) {
				addProperty(key, props.getProperty(key));
			}
		}
	}	
	
	public List<PropertyMetadata> getJAXBProperties(){
		return properties;
	}
	
	public void setJAXBProperties(List<PropertyMetadata> props){
		this.properties.clear();
		if (props != null) {			
			for (PropertyMetadata prop:props) {
				addProperty(prop.getName(), prop.getValue());
			}
		}
	}	
	
	@Override
	public String getPropertyValue(String name) {
		PropertyMetadata prop = this.properties.getMap().get(name);
		if (prop == null) {
			return null;
		}
		return prop.getValue();
	}

	public void addProperty(String key, String value) {
		this.properties.getMap().put(key, new PropertyMetadata(key, value));
	}
	
	   /**
	    * Add attachment
	    *
	    * @param <T> the expected type
	    * @param attachment the attachment
	    * @param type the type
	    * @return any previous attachment
	    * @throws IllegalArgumentException for a null name, attachment or type
	    * @throws UnsupportedOperationException when not supported by the implementation
	    */	
		public <T> T addAttchment(Class<T> type, T attachment) {
	      if (type == null)
	          throw new IllegalArgumentException("Null type"); //$NON-NLS-1$
	      Object result = this.attachments.put(type.getName(), attachment);
	      if (result == null)
	         return null;
	      return type.cast(result);
	      
		}
		
		public Object addAttchment(String key, Object attachment) {
		      if (key == null)
		          throw new IllegalArgumentException("Null type"); //$NON-NLS-1$
		      Object result = this.attachments.put(key, attachment);
		      if (result == null)
		         return null;
		      return result;
		}		
		
	   /**
	    * Remove attachment
	    * 
	    * @param <T> the expected type
	    * @return the attachment or null if not present
	    * @param type the type
	    * @throws IllegalArgumentException for a null name or type
	    */	
		public <T> T removeAttachment(Class<T> type) {
			if (type == null)
				throw new IllegalArgumentException("Null type"); //$NON-NLS-1$
			Object result = this.attachments.remove(type.getName());
			if (result == null)
				return null;
			return type.cast(result);
		}
		
		public Object removeAttachment(String key) {
			if (key == null)
				throw new IllegalArgumentException("Null type"); //$NON-NLS-1$
			Object result = this.attachments.remove(key);
			if (result == null)
				return null;
			return result;
		}		
	   /**
	    * Get attachment
	    * 
	    * @param <T> the expected type
	    * @param type the type
	    * @return the attachment or null if not present
	    * @throws IllegalArgumentException for a null name or type
	    */
	   public <T> T getAttachment(Class<T> type) {
	      if (type == null)
	          throw new IllegalArgumentException("Null type"); //$NON-NLS-1$
	      Object result = this.attachments.get(type.getName());
	      if (result == null)
	         return null;
	      return type.cast(result);      
	   }	
	   
	   public Object getAttachment(String key) {
		      if (key == null)
		          throw new IllegalArgumentException("Null type"); //$NON-NLS-1$
		      Object result = this.attachments.get(key);
		      if (result == null)
		         return null;
		      return result;  
	   }		
	   	   
}
