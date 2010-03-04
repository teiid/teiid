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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.XmlValue;

import org.jboss.managed.api.annotation.ManagementObject;
import org.jboss.managed.api.annotation.ManagementObjectID;
import org.jboss.managed.api.annotation.ManagementProperties;
import org.jboss.managed.api.annotation.ManagementProperty;
import org.teiid.adminapi.Model;

import com.metamatrix.core.vdb.ModelType;


@XmlAccessorType(XmlAccessType.NONE)
@XmlType(name = "", propOrder = {
    "JAXBProperties",
    "JAXBSources",
    "errors"
})
@ManagementObject(properties=ManagementProperties.EXPLICIT)
public class ModelMetaData extends AdminObjectImpl implements Model {
	
	private static final String SUPPORTS_MULTI_SOURCE_BINDINGS_KEY = "supports-multi-source-bindings"; //$NON-NLS-1$
	private static final long serialVersionUID = 3714234763056162230L;
		
	private ListOverMap<SourceMapping> sources = new ListOverMap(new KeyBuilder<SourceMapping>() {
		@Override
		public String getKey(SourceMapping entry) {
			return entry.getName();
		}
	});
	
	@XmlAttribute(name = "type")
	protected String modelType = Type.PHYSICAL.name(); //$NON-NLS-1$
    
    @XmlAttribute(name = "visible")
    private Boolean visible = true;
    
    @XmlElement(name = "validation-error")
    protected List<ValidationError> errors;    
    
	@ManagementProperty(description="Model Name", readOnly=true)
	@ManagementObjectID(type="model")
	@XmlAttribute(name = "name", required = true)
	public String getName() {
		return super.getName();
	}    

	// This is needed by JAXB
	public void setName(String name) {
		super.setName(name);
	}
	
	@Override
	@ManagementProperty(description = "Is Model Source model", readOnly=true)
    public boolean isSource() {
		if (modelType != null) {
			return ModelType.parseString(modelType.toUpperCase()) == ModelType.PHYSICAL;
		}
		throw new IllegalStateException("Model state is not correctly set");
	}

	@Override
	@ManagementProperty(description = "Is Model Visible", readOnly=true)
	public boolean isVisible() {
		return this.visible;
	}

	@Override
	@ManagementProperty(description = "Model Type", readOnly=true)
	public Type getModelType() {
		return Type.valueOf(modelType);
	}

	@Override
	@ManagementProperty(description = "Does Model supports multi-source bindings", readOnly=true)
    public boolean isSupportsMultiSourceBindings() {
		String supports = getPropertyValue(SUPPORTS_MULTI_SOURCE_BINDINGS_KEY);
		return Boolean.parseBoolean(supports);
    }    
	
	@Override
	@ManagementProperty(description = "Properties", readOnly=true)
    public Properties getProperties() {
        return new Properties(super.getProperties());
    }		
	
	@Override
	@XmlElement(name = "property", type = PropertyMetadata.class)
	protected List<PropertyMetadata> getJAXBProperties(){
		return super.getJAXBProperties();
	}
	
    public void setSupportsMultiSourceBindings(boolean supports) {
        addProperty(SUPPORTS_MULTI_SOURCE_BINDINGS_KEY, Boolean.toString(supports));
    }

    public void setModelType(String modelType) {
        this.modelType = modelType;
    }
    
    public String toString() {
		return getName() + this.sources;
    }
    
    public void setVisible(Boolean value) {
    	this.visible = value;
    }    

    @ManagementProperty(description = "Source Mappings (defined by user)")
	public List<SourceMapping> getSourceMappings(){
		return new ArrayList<SourceMapping>(this.sources.getMap().values());
	}
    
	@XmlElement(name = "source")
	protected List<SourceMapping> getJAXBSources(){
		// do not wrap this in another List object; we need direct access for jaxb
		return this.sources;
	}	
	
    @Override
    public List<String> getSourceNames() {
    	return new ArrayList<String>(this.sources.getMap().keySet());
	}
    
    public String getSourceJndiName(String sourceName) {
    	SourceMapping s = this.sources.getMap().get(sourceName);
    	return s.getJndiName();
	}
    
	public void addSourceMapping(String name, String jndiName) {
		this.sources.getMap().put(name, new SourceMapping(name, jndiName));
	}    
	
	@ManagementProperty(description = "Model Validity Errors", readOnly=true)
	public List<ValidationError> getErrors(){
		return this.errors;
	}
	
    public void addError(String severity, String message) {
        if (this.errors == null) {
            this.errors = new ArrayList<ValidationError>();
        }
        this.errors.add(new ValidationError(severity, message));
    }	
	
    @XmlAccessorType(XmlAccessType.NONE)
    @XmlType(name = "", propOrder = {
        "value"
    })
    @ManagementObject(properties=ManagementProperties.EXPLICIT)
    public static class ValidationError implements Serializable{
		private static final long serialVersionUID = 2044197069467559527L;

		public enum Severity {ERROR, WARNING};
    	
        @XmlValue
        protected String value;
        
        @XmlAttribute(name = "severity")
        protected String severity;

        public ValidationError() {};
        
        public ValidationError(String severity, String msg) {
        	this.severity = severity;
        	this.value = msg;
        }
    	
        @ManagementProperty (description="Error Message", readOnly = true)
        public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}

		@ManagementProperty (description="Severity", readOnly = true)
		public String getSeverity() {
			return severity;
		}

		public void setSeverity(String severity) {
			this.severity = severity;
		}        
    }	
    
    @XmlAccessorType(XmlAccessType.NONE)
    @XmlType(name = "")
    @ManagementObject(properties=ManagementProperties.EXPLICIT)
    public static class SourceMapping implements Serializable {
		private static final long serialVersionUID = -4417878417697685794L;

		@XmlAttribute(name = "name", required = true)
        private String name;
        
        @XmlAttribute(name = "jndi-name")
        private String jndiName;
        
        
        public SourceMapping() {}
        
        public SourceMapping(String name, String jndiName) {
        	this.name = name;
        	this.jndiName = jndiName;
        }

        @ManagementProperty (description="Source Name", readOnly = true)
		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@ManagementProperty (description="JNDI Name of the resource to assosiate with Source name")
		public String getJndiName() {
			// this default could be controlled if needed.
			if (this.jndiName == null) {
				return "java:"+name;
			}
			return jndiName;
		}

		public void setJndiName(String jndiName) {
			this.jndiName = jndiName;
		}
		
		public String toString() {
			return getName()+":"+getJndiName();
		}
    }    
}
