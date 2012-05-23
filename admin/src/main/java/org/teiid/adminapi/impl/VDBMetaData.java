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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.impl.ModelMetaData.ValidationError;


public class VDBMetaData extends AdminObjectImpl implements VDB {

	private static final String VERSION_DELIM = "."; //$NON-NLS-1$

	private static final long serialVersionUID = -4723595252013356436L;
	
	/**
	 * This simulating a list over a map. JAXB requires a list and performance recommends
	 * map and we would like to keep one variable to represent both. 
	 */
	protected ListOverMap<ModelMetaData> models = new ListOverMap<ModelMetaData>(new KeyBuilder<ModelMetaData>() {
		private static final long serialVersionUID = 846247100420118961L;

		@Override
		public String getKey(ModelMetaData entry) {
			return entry.getName();
		}
	});
	
	protected ListOverMap<VDBTranslatorMetaData> translators = new ListOverMap<VDBTranslatorMetaData>(new KeyBuilder<VDBTranslatorMetaData>() {
		private static final long serialVersionUID = 3890502172003653563L;

		@Override
		public String getKey(VDBTranslatorMetaData entry) {
			return entry.getName();
		}
	});	
	
	protected ListOverMap<DataPolicyMetadata> dataPolicies = new ListOverMap<DataPolicyMetadata>(new KeyBuilder<DataPolicyMetadata>() {
		private static final long serialVersionUID = 4954591545242715254L;

		@Override
		public String getKey(DataPolicyMetadata entry) {
			return entry.getName();
		}
	});
	
	private List<VDBImportMetadata> imports = new ArrayList<VDBImportMetadata>(2);
	
	private int version = 1;
	
	protected String description;
	
	private boolean dynamic = false;
	private VDB.Status status = VDB.Status.INACTIVE;
	private ConnectionType connectionType = VDB.ConnectionType.BY_VERSION;
	private boolean removed;
	private long queryTimeout = Long.MIN_VALUE;
	private Set<String> importedModels = Collections.emptySet();

	public String getName() {
		return super.getName();
	}
	
	public String getFullName() {
		return getName() + VERSION_DELIM + getVersion();
	}
	
	// This needed by JAXB marshaling
	public void setName(String name) {
		super.setName(name);
	} 
	
	public boolean isRemoved() {
		return removed;
	}
	
	public void setRemoved(boolean removed) {
		this.removed = removed;
	}
	
	@Override
	public ConnectionType getConnectionType() {
		return this.connectionType;
	}
	
	public void setConnectionType(ConnectionType allowConnections) {
		this.connectionType = allowConnections;
	}
	
	public void setConnectionType(String allowConnections) {
		this.connectionType = ConnectionType.valueOf(allowConnections);
	}
	
	@Override
	public Status getStatus() {
		return this.status;
	}
	
	public void setStatus(Status s) {
		this.status = s;
	}
	
	public void setStatus(String s) {
		this.status = Status.valueOf(s);
	}
	
	@Override
	public int getVersion() {
		return this.version;
	}
	
	public void setVersion(int version) {
		this.version = version;
	}	
		
	@Override
	public List<Model> getModels(){
		return new ArrayList<Model>(this.models.getMap().values());
	}
	
	public Map<String, ModelMetaData> getModelMetaDatas() {
		return this.models.getMap();
	}
	
	/**
	 * This method required to make the JNDI assignment on the model work; if not persistent Management framework
	 * treating "models" as ReadOnly property. The actual assignment is done in the VDBMetaDataClassInstancefactory
	 * @param models
	 */
	public void setModels(List<Model> models) {
		this.models.getMap().clear();
		for (Model obj : models) {
			ModelMetaData model = (ModelMetaData) obj;
			addModel(model);
		}
	}
	
	public ModelMetaData addModel(ModelMetaData m) {
		return this.models.getMap().put(m.getName(), m);
	}	
	
	@Override
	public List<Translator> getOverrideTranslators() {
		return new ArrayList<Translator>(this.translators.getMap().values());
	}
	
	public void setOverrideTranslators(List<Translator> translators) {
		for (Translator t: translators) {
			this.translators.getMap().put(t.getName(), (VDBTranslatorMetaData)t);
		}
	}
	
	public void addOverideTranslator(VDBTranslatorMetaData t) {
		this.translators.getMap().put(t.getName(), t);
	}
	
	public boolean isOverideTranslator(String name) {
		return this.translators.getMap().containsKey(name);
	}
	
	@Override
	public String getDescription() {
		return this.description;
	}
	
	public void setDescription(String desc) {
		this.description = desc;
	}

	@Override
	public List<String> getValidityErrors(){
		List<String> allErrors = new ArrayList<String>();
		for (ModelMetaData model:this.models.getMap().values()) {
			List<ValidationError> errors = model.getErrors();
			if (errors != null && !errors.isEmpty()) {
				for (ValidationError m:errors) {
					if (ValidationError.Severity.valueOf(m.getSeverity()).equals(ValidationError.Severity.ERROR)) {
						allErrors.add(m.getValue());
					}
				}
			}
		}
		return allErrors; 
	}

	@Override
    public boolean isValid() {
        if (!getValidityErrors().isEmpty()) {
            return false;
        }
                
        if (getModels().isEmpty()) {
            return false;        	
        }
    	for(ModelMetaData m: this.models.getMap().values()) {
    		if (m.isSource()) {
    			List<String> resourceNames = m.getSourceNames();
    			if (resourceNames.isEmpty()) {
    				return false;
    			}
    			for (String sourceName:resourceNames) {
    				if (m.getSourceConnectionJndiName(sourceName) == null) {
    					return false;
    				}
    			}
    		}
    	}
        return true;
    } 	
    
	public String toString() {
		return getName()+VERSION_DELIM+getVersion()+ models.getMap().values(); 
	}
	
	public boolean isVisible(String modelName) {
		ModelMetaData model = getModel(modelName);
		return model == null || model.isVisible();
	}

	public ModelMetaData getModel(String modelName) {
		return this.models.getMap().get(modelName);
	}
	
	public Set<String> getMultiSourceModelNames(){
		Set<String> list = new HashSet<String>();
		for(ModelMetaData m: models.getMap().values()) {
			if (m.isSupportsMultiSourceBindings()) {
				list.add(m.getName());
			}
		}
		return list;
	}
	
	public boolean isDynamic() {
		return dynamic;
	}

	public void setDynamic(boolean dynamic) {
		this.dynamic = dynamic;
	}	
	
	@Override
	public List<DataPolicy> getDataPolicies(){
		return new ArrayList<DataPolicy>(this.dataPolicies.getMap().values());
	}	
	
	/**
	 * This method is required by the Management framework to write the mappings to the persistent form. The actual assignment is done
	 * in the VDBMetaDataClassInstancefactory
	 * @param policies
	 */
	public void setDataPolicies(List<DataPolicy> policies){
		this.dataPolicies.getMap().clear();
		for (DataPolicy policy:policies) {
			this.dataPolicies.getMap().put(policy.getName(), (DataPolicyMetadata)policy);
		}
	}	
	
	public DataPolicyMetadata addDataPolicy(DataPolicyMetadata policy){
		return this.dataPolicies.getMap().put(policy.getName(), policy);
	}
	
	public DataPolicyMetadata getDataPolicy(String policyName) {
		return this.dataPolicies.getMap().get(policyName);
	}
	
	public VDBTranslatorMetaData getTranslator(String name) {
		return this.translators.getMap().get(name);
	}
	
	public boolean isPreview() {
		return Boolean.valueOf(getPropertyValue("preview")); //$NON-NLS-1$
	}
	public long getQueryTimeout() {
		if (queryTimeout == Long.MIN_VALUE) {
			String timeout = getPropertyValue("query-timeout"); //$NON-NLS-1$
			if (timeout != null) {
				queryTimeout = Math.max(0, Long.parseLong(timeout));
			} else {
				queryTimeout = 0;
			}
		}
		return queryTimeout;
	}	
	
	public List<VDBImportMetadata> getVDBImports() {
		return imports;
	}
	
	public Set<String> getImportedModels() {
		return importedModels;
	}
	
	public void setImportedModels(Set<String> importedModels) {
		this.importedModels = importedModels;
	}
}
