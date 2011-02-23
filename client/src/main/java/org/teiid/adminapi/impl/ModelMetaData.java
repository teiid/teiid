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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

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
import org.teiid.adminapi.impl.ModelMetaData.ValidationError.Severity;


@XmlAccessorType(XmlAccessType.NONE)
@XmlType(name = "", propOrder = {
	"description",
    "JAXBProperties",
    "sources",
    "errors"
})
@ManagementObject(properties=ManagementProperties.EXPLICIT)
public class ModelMetaData extends AdminObjectImpl implements Model {
	
	private static final int DEFAULT_ERROR_HISTORY = 10;
	private static final String SUPPORTS_MULTI_SOURCE_BINDINGS_KEY = "supports-multi-source-bindings"; //$NON-NLS-1$
	private static final long serialVersionUID = 3714234763056162230L;
		
	@XmlElement(name = "source")
	protected ListOverMap<SourceMappingMetadata> sources = new ListOverMap<SourceMappingMetadata>(new KeyBuilder<SourceMappingMetadata>() {
		private static final long serialVersionUID = 2273673984691112369L;

		@Override
		public String getKey(SourceMappingMetadata entry) {
			return entry.getName();
		}
	});
	
	@XmlAttribute(name = "type")
	protected String modelType = Type.PHYSICAL.name();
	
	@XmlElement(name = "description")
	protected String description;	
	
	@XmlAttribute(name = "path")
	protected String path; 
    
	@XmlAttribute(name = "visible")
    protected Boolean visible = true;
    
    @XmlElement(name = "validation-error")
    protected List<ValidationError> errors;    
    
	@ManagementProperty(description="Model Name")
	@XmlAttribute(name = "name", required = true)
	@ManagementObjectID(type="models")
	public String getName() {
		return super.getName();
	}    

	// This is needed by JAXB
	public void setName(String name) {
		super.setName(name);
	}

	@Override
	@ManagementProperty(description = "Model description")	
	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}	
	
	@Override
	@ManagementProperty(description = "Is Model Source model")
    public boolean isSource() {
		return getModelType() == Model.Type.PHYSICAL;
	}

	@Override
	@ManagementProperty(description = "Is Model Visible")
	public boolean isVisible() {
		return this.visible;
	}

	@Override
	@ManagementProperty(description = "Model Type")
	public Type getModelType() {
		try {
			return Type.valueOf(modelType.toUpperCase());
		} catch(IllegalArgumentException e) {
			return Type.OTHER;
		}
	}
	
	@ManagementProperty(description = "Path to model file inside the archive")
    public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}	

	@Override
	@ManagementProperty(description = "Does Model supports multi-source bindings")
    public boolean isSupportsMultiSourceBindings() {
		String supports = getPropertyValue(SUPPORTS_MULTI_SOURCE_BINDINGS_KEY);
		return Boolean.parseBoolean(supports);
    }    
	
	@Override
	@XmlElement(name = "property", type = PropertyMetadata.class)
	@ManagementProperty(description = "Model Properties", managed=true)
	public List<PropertyMetadata> getJAXBProperties(){
		return super.getJAXBProperties();
	}
	
    public void setSupportsMultiSourceBindings(boolean supports) {
        addProperty(SUPPORTS_MULTI_SOURCE_BINDINGS_KEY, Boolean.toString(supports));
    }

    public void setModelType(Model.Type modelType) {
        this.modelType = modelType.name();
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

    @ManagementProperty(description = "Source Mappings (defined by user)", managed=true)
	public List<SourceMappingMetadata> getSourceMappings(){
		return new ArrayList<SourceMappingMetadata>(this.sources.getMap().values());
	}
    
	public void setSourceMappings(List<SourceMappingMetadata> sources){
		for (SourceMappingMetadata source: sources) {
			addSourceMapping(source.getName(), source.getTranslatorName(), source.getConnectionJndiName());
		}
	}      
    
    @Override
    public List<String> getSourceNames() {
    	return new ArrayList<String>(this.sources.getMap().keySet());
	}
    
    @Override
    public String getSourceConnectionJndiName(String sourceName) {
    	SourceMappingMetadata s = this.sources.getMap().get(sourceName);
    	if (s == null) {
    		return null;
    	}
    	return s.getConnectionJndiName();
	}
    
    @Override
    public String getSourceTranslatorName(String sourceName) {
    	SourceMappingMetadata s = this.sources.getMap().get(sourceName);
    	if (s == null) {
    		return null;
    	}
    	return s.getTranslatorName();
	}    
    
	public void addSourceMapping(String name, String translatorName, String connJndiName) {
		this.sources.getMap().put(name, new SourceMappingMetadata(name, translatorName, connJndiName));
	}    
	
	@ManagementProperty(description = "Model Validity Errors", readOnly=true, managed=true)
	public List<ValidationError> getErrors(){
		return getValidationErrors(Severity.ERROR);
	}
	
	public void setErrors(List<ValidationError> errors){
		this.errors = errors;
	}	
	
	public synchronized List<ValidationError> getValidationErrors(ValidationError.Severity severity){
		if (this.errors == null) {
			return Collections.emptyList();
		}
		List<ValidationError> list = new ArrayList<ValidationError>();
		for (ValidationError ve: this.errors) {
			if (Severity.valueOf(ve.severity) == severity) {
				list.add(ve);
			}
		}
		return list;
	}	
	
    public synchronized ValidationError addError(String severity, String message) {
        if (this.errors == null) {
            this.errors = new LinkedList<ValidationError>();
        }
        ValidationError ve = new ValidationError(severity, message);
        this.errors.add(ve);
        if (this.errors.size() > DEFAULT_ERROR_HISTORY) {
        	this.errors.remove(0);
        }
        return ve;
    }
    
    public synchronized boolean removeError(ValidationError remove) {
    	if (this.errors == null) {
    		return false;
    	}
    	return this.errors.remove(remove);
    }
    
    public synchronized void clearErrors() {
    	this.errors.clear();
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
        
        @XmlAttribute(name = "severity", required = true)
        protected String severity;
        
        @XmlAttribute(name = "path")
        protected String path;
        
		public ValidationError() {};
        
        public ValidationError(String severity, String msg) {
        	this.severity = severity;
        	this.value = msg;
        }
    	
        @ManagementProperty (description="Error Message")
        public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}

		@ManagementProperty (description="Severity")
		public String getSeverity() {
			return severity;
		}

		public void setSeverity(String severity) {
			this.severity = severity;
		}       
		
		@ManagementProperty (description="Path")
        public String getPath() {
			return path;
		}

		public void setPath(String path) {
			this.path = path;
		}		

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ValidationError other = (ValidationError) obj;
			if (severity == null) {
				if (other.severity != null)
					return false;
			} else if (!severity.equals(other.severity))
				return false;
			if (value == null) {
				if (other.value != null)
					return false;
			} else if (!value.equals(other.value))
				return false;
			return true;
		}		
    }    
}
