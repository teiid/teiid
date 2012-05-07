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

import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.ModelMetaData.ValidationError.Severity;


public class ModelMetaData extends AdminObjectImpl implements Model {
	
	private static final int DEFAULT_ERROR_HISTORY = 10;
	private static final String SUPPORTS_MULTI_SOURCE_BINDINGS_KEY = "supports-multi-source-bindings"; //$NON-NLS-1$
	private static final long serialVersionUID = 3714234763056162230L;
		
	protected ListOverMap<SourceMappingMetadata> sources = new ListOverMap<SourceMappingMetadata>(new KeyBuilder<SourceMappingMetadata>() {
		private static final long serialVersionUID = 2273673984691112369L;

		@Override
		public String getKey(SourceMappingMetadata entry) {
			return entry.getName();
		}
	});
	
	protected String modelType = Type.PHYSICAL.name();
	protected String description;	
	protected String path; 
    protected Boolean visible = true;
    protected List<ValidationError> errors;    
    protected String schemaSourceType;
	protected String schemaText;

	public String getName() {
		return super.getName();
	}    

	// This is needed by JAXB
	public void setName(String name) {
		super.setName(name);
	}

	@Override
	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}	
	
	@Override
    public boolean isSource() {
		return getModelType() == Model.Type.PHYSICAL;
	}

	@Override
	public boolean isVisible() {
		return this.visible;
	}

	@Override
	public Type getModelType() {
		try {
			return Type.valueOf(modelType);
		} catch(IllegalArgumentException e) {
			return Type.OTHER;
		}
	}
	
    public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}	

	@Override
    public boolean isSupportsMultiSourceBindings() {
		String supports = getPropertyValue(SUPPORTS_MULTI_SOURCE_BINDINGS_KEY);
		return Boolean.parseBoolean(supports);
    }    
	
	@Override
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
    	if (modelType != null) {
    		this.modelType = modelType.toUpperCase();
    	} else {
    		this.modelType = null;
    	}
    }    
    
    public String toString() {
		return getName() + this.sources;
    }
    
    public void setVisible(Boolean value) {
    	this.visible = value;
    }    

	public List<SourceMappingMetadata> getSourceMappings(){
		return new ArrayList<SourceMappingMetadata>(this.sources.getMap().values());
	}
	
	public SourceMappingMetadata getSourceMapping(String sourceName){
		return this.sources.getMap().get(sourceName);
	}	
    
	public void setSourceMappings(List<SourceMappingMetadata> sources){
		this.sources.getMap().clear();
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
    
	public SourceMappingMetadata addSourceMapping(String name, String translatorName, String connJndiName) {
		return this.sources.getMap().put(name, new SourceMappingMetadata(name, translatorName, connJndiName));
	}
	
	public void addSourceMapping(SourceMappingMetadata source) {
		this.addSourceMapping(source.getName(), source.getTranslatorName(), source.getConnectionJndiName());
	}    
	
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
    
    public synchronized ValidationError addError(ValidationError ve) {
        if (this.errors == null) {
            this.errors = new LinkedList<ValidationError>();
        }
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
	
    public static class ValidationError implements Serializable{
		private static final long serialVersionUID = 2044197069467559527L;

		public enum Severity {ERROR, WARNING};
    	
        protected String value;
        protected String severity;
        protected String path;
        
		public ValidationError() {};
        
        public ValidationError(String severity, String msg) {
        	this.severity = severity;
        	this.value = msg;
        }
    	
        public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}

		public String getSeverity() {
			return severity;
		}

		public void setSeverity(String severity) {
			this.severity = severity;
		}       
		
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

    public String getSchemaSourceType() {
		return schemaSourceType;
	}

	public void setSchemaSourceType(String schemaSourceType) {
		this.schemaSourceType = schemaSourceType;
	}

	public String getSchemaText() {
		return schemaText;
	}

	public void setSchemaText(String schemaText) {
		this.schemaText = schemaText;
	}
	
}
