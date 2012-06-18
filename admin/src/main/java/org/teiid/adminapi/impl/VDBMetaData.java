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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
	
	private LinkedHashMap<String, ModelMetaData> models = new LinkedHashMap<String, ModelMetaData>();
	private LinkedHashMap<String, VDBTranslatorMetaData> translators = new LinkedHashMap<String, VDBTranslatorMetaData>(); 
	private LinkedHashMap<String, DataPolicyMetadata> dataPolicies = new LinkedHashMap<String, DataPolicyMetadata>(); 
	private List<VDBImportMetadata> imports = new ArrayList<VDBImportMetadata>(2);
	private int version = 1;
	private String description;
	private boolean dynamic = false;
	private volatile VDB.Status status = VDB.Status.ACTIVE;
	private ConnectionType connectionType = VDB.ConnectionType.BY_VERSION;
	private long queryTimeout = Long.MIN_VALUE;
	private Set<String> importedModels = Collections.emptySet();

	public String getFullName() {
		return getName() + VERSION_DELIM + getVersion();
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
	
	public boolean isLoading() {
		return this.status.isLoading();
	}
	
	public synchronized void setStatus(Status s) {
		this.notifyAll();
		if (this.status == Status.REMOVED) {
			return;
		}
		this.status = s;
	}
	
	public void setStatus(String s) {
		setStatus(Status.valueOf(s));
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
		return new ArrayList<Model>(this.models.values());
	}
	
	public Map<String, ModelMetaData> getModelMetaDatas() {
		return this.models;
	}
	
	/**
	 * @param models
	 */
	public void setModels(Collection<ModelMetaData> models) {
		this.models.clear();
		for (ModelMetaData obj : models) {
			addModel(obj);
		}
	}
	
	public ModelMetaData addModel(ModelMetaData m) {
		return this.models.put(m.getName(), m);
	}	
	
	@Override
	public List<Translator> getOverrideTranslators() {
		return new ArrayList<Translator>(this.translators.values());
	}
	
	public void setOverrideTranslators(List<Translator> translators) {
		for (Translator t: translators) {
			this.translators.put(t.getName(), (VDBTranslatorMetaData)t);
		}
	}
	
	public void addOverideTranslator(VDBTranslatorMetaData t) {
		this.translators.put(t.getName(), t);
	}
	
	public boolean isOverideTranslator(String name) {
		return this.translators.containsKey(name);
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
		for (ModelMetaData model:this.models.values()) {
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
	public List<String> getRuntimeErrors(){
		List<String> allErrors = new ArrayList<String>();
		for (ModelMetaData model:this.models.values()) {
			List<ValidationError> errors = model.getRuntimeErrors();
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
        if (!getRuntimeErrors().isEmpty()) {
            return false;
        }        
        if (getModels().isEmpty()) {
            return false;        	
        }
    	for(ModelMetaData m: this.models.values()) {
    		if (m.isSource()) {
    			List<String> resourceNames = m.getSourceNames();
    			if (resourceNames.isEmpty()) {
    				return false;
    			}
    		}
    	}
        return true;
    } 	
    
	public String toString() {
		return getName()+VERSION_DELIM+getVersion()+ models.values(); 
	}
	
	public boolean isVisible(String modelName) {
		ModelMetaData model = getModel(modelName);
		return model == null || model.isVisible();
	}

	public ModelMetaData getModel(String modelName) {
		return this.models.get(modelName);
	}
	
	public Set<String> getMultiSourceModelNames(){
		Set<String> list = new HashSet<String>();
		for(ModelMetaData m: models.values()) {
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
		return new ArrayList<DataPolicy>(this.dataPolicies.values());
	}	
	
	/**
	 * This method is required by the Management framework to write the mappings to the persistent form. The actual assignment is done
	 * in the VDBMetaDataClassInstancefactory
	 * @param policies
	 */
	public void setDataPolicies(List<DataPolicy> policies){
		this.dataPolicies.clear();
		for (DataPolicy policy:policies) {
			this.dataPolicies.put(policy.getName(), (DataPolicyMetadata)policy);
		}
	}	
	
	public DataPolicyMetadata addDataPolicy(DataPolicyMetadata policy){
		return this.dataPolicies.put(policy.getName(), policy);
	}
	
	public DataPolicyMetadata getDataPolicy(String policyName) {
		return this.dataPolicies.get(policyName);
	}
	
	public VDBTranslatorMetaData getTranslator(String name) {
		return this.translators.get(name);
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
