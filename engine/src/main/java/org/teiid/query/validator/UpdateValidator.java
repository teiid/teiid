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

package org.teiid.query.validator;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SupportConstants;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.util.SymbolMap;

/**
 * <p> This visitor is used to validate updates through virtual groups. The command defining
 * the virtual group is always a <code>Query</code>. This object visits various parts of
 * this <code>Query</code> and verifies if the virtual group definition will allows it to be
 * updated.</p>
 */
public class UpdateValidator {
	
	public static class UpdateMapping {
		private GroupSymbol group;
		private GroupSymbol correlatedName;
		private Map<ElementSymbol, ElementSymbol> updatableViewSymbols = new HashMap<ElementSymbol, ElementSymbol>();
		private boolean insertAllowed = false;
		private boolean updateAllowed = false;
		
		public Map<ElementSymbol, ElementSymbol> getUpdatableViewSymbols() {
			return updatableViewSymbols;
		}
		
		public boolean isInsertAllowed() {
			return insertAllowed;
		}
		
		public boolean isUpdateAllowed() {
			return updateAllowed;
		}
		
		public GroupSymbol getGroup() {
			return group;
		}
		
		public GroupSymbol getCorrelatedName() {
			return correlatedName;
		}
	}
	
	public static class UpdateInfo {
		private Map<String, UpdateMapping> updatableGroups = new HashMap<String, UpdateMapping>();
		private boolean isSimple = true;
		private UpdateMapping deleteTarget;
		private String validationError;
		private boolean inherentUpdate;
		private boolean inherentDelete;
		private boolean inherentInsert;
		private Query view;
		
		public boolean isSimple() {
			return isSimple;
		}
		
		public UpdateMapping getDeleteTarget() {
			return deleteTarget;
		}
		
		public Map<String, UpdateMapping> getUpdatableGroups() {
			return updatableGroups;
		}
		
		public String getValidationError() {
			return validationError;
		}
		
		public void setValidationError(String validationError) {
			this.validationError = validationError;
		}
		
		public boolean isInherentDelete() {
			return inherentDelete;
		}
		
		public boolean isInherentInsert() {
			return inherentInsert;
		}
		
		public boolean isInherentUpdate() {
			return inherentUpdate;
		}
		
		public UpdateMapping findUpdateMapping(Collection<ElementSymbol> updateCols, boolean insert) {
			for (UpdateMapping entry : getUpdatableGroups().values()) {
				if (((insert && entry.insertAllowed) || (!insert && entry.updateAllowed)) && entry.updatableViewSymbols.keySet().containsAll(updateCols)) {
					return entry;
				}
			}
			return null;
		}
		
		public Query getViewDefinition() {
			return view;
		}
		
	}
	
	private QueryMetadataInterface metadata;
	private ValidatorReport report = new ValidatorReport();
	private UpdateInfo updateInfo = new UpdateInfo();

	public UpdateValidator(QueryMetadataInterface qmi, boolean inherentUpdate, boolean inherentDelete, boolean inherentInsert) {
		this.metadata = qmi;
		this.updateInfo.inherentDelete = inherentDelete;
		this.updateInfo.inherentInsert = inherentInsert;
		this.updateInfo.inherentUpdate = inherentUpdate;
	}
	
	public UpdateInfo getUpdateInfo() {
		return updateInfo;
	}
	
	public ValidatorReport getReport() {
		return report;
	}
	
    public void validate(Command command, List<ElementSymbol> viewSymbols) throws QueryMetadataException, TeiidComponentException {
    	if (!(command instanceof Query)) {
    		report.handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0001")); //$NON-NLS-1$
    		return;
        }
    	
    	Query query = (Query)command;

    	if (query.getFrom() == null || query.getInto() != null) {
    		report.handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0001")); //$NON-NLS-1$
    		return;
    	}
    	
    	if (query.getWith() != null) {
    		report.handleValidationWarning(QueryPlugin.Util.getString("ERR.015.012.0002")); //$NON-NLS-1$
    		updateInfo.isSimple = false;
    	}

    	if (query.hasAggregates()) {
    		report.handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0006")); //$NON-NLS-1$
    		return;
    	}
    	
    	updateInfo.view = query;
    	
    	if (query.getLimit() != null) {
    		report.handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0013")); //$NON-NLS-1$
    		return;
    	}
    	
    	if (query.getSelect().isDistinct()) {
    		report.handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0008")); //$NON-NLS-1$
    		return;
    	} 
    	
    	List<SingleElementSymbol> projectedSymbols = query.getSelect().getProjectedSymbols();
    	
    	for (int i = 0; i < projectedSymbols.size(); i++) {
            SingleElementSymbol symbol = projectedSymbols.get(i);
            Expression ex = SymbolMap.getExpression(symbol);
            
            if (!metadata.elementSupports(viewSymbols.get(i).getMetadataID(), SupportConstants.Element.UPDATE)) {
            	continue;
            }
            if (ex instanceof ElementSymbol) {
            	ElementSymbol es = (ElementSymbol)ex;
            	String groupName = es.getGroupSymbol().getCanonicalName();
        		UpdateMapping info = updateInfo.updatableGroups.get(groupName);
        		if (es.getGroupSymbol().getDefinition() != null) {
            		ElementSymbol clone = (ElementSymbol)es.clone();
            		clone.setName(es.getGroupSymbol().getDefinition() + ElementSymbol.SEPARATOR + es.getShortName());
            		clone.getGroupSymbol().setName(clone.getGroupSymbol().getNonCorrelationName());
            		clone.getGroupSymbol().setDefinition(null);
            		es = clone;
            	}
            	if (info == null) {
            		info = new UpdateMapping();
            		info.group = es.getGroupSymbol();
            		info.correlatedName = ((ElementSymbol)ex).getGroupSymbol();
            		updateInfo.updatableGroups.put(groupName, info);
            	}
            	//TODO: warn if mapped twice
            	info.updatableViewSymbols.put(viewSymbols.get(i), es);
            } else {
            	//TODO: look for reversable widening conversions
            	
                report.handleValidationWarning(QueryPlugin.Util.getString("ERR.015.012.0007", viewSymbols.get(i), symbol)); //$NON-NLS-1$
            }
    	}
    	
    	if (query.getFrom().getClauses().size() > 1 || (!(query.getFrom().getClauses().get(0) instanceof UnaryFromClause))) {
    		report.handleValidationWarning(QueryPlugin.Util.getString("ERR.015.012.0009", query.getFrom())); //$NON-NLS-1$
    		updateInfo.isSimple = false;
    	}
    	List<GroupSymbol> allGroups = query.getFrom().getGroups();
    	HashSet<GroupSymbol> keyPreservingGroups = new HashSet<GroupSymbol>();
    	
    	if (query.getFrom().getClauses().size() == 1) {
    		ResolverUtil.findKeyPreserinvg((FromClause)query.getFrom().getClauses().get(0), keyPreservingGroups, metadata);
    	}
    	
		for (GroupSymbol groupSymbol : keyPreservingGroups) {
			setUpdateFlags(groupSymbol);
		}

		allGroups.removeAll(keyPreservingGroups);
		if (updateInfo.isSimple) {
			if (!allGroups.isEmpty()) {
				setUpdateFlags(allGroups.iterator().next());
			}
		} else if (this.updateInfo.inherentUpdate || this.updateInfo.inherentDelete) {
			for (GroupSymbol groupSymbol : allGroups) {
				UpdateMapping info = updateInfo.updatableGroups.get(groupSymbol.getCanonicalName());
				if (info == null) {
					continue; // not projected
				}
				report.handleValidationWarning(QueryPlugin.Util.getString("ERR.015.012.0004", groupSymbol)); //$NON-NLS-1$
			}
		}

    	boolean updatable = false;
    	boolean insertable = false;
    	for (UpdateMapping info : updateInfo.updatableGroups.values()) {
    		if (info.updateAllowed) {
    			if (!updatable) {
    				this.updateInfo.deleteTarget = info;
    			} else if (!info.getGroup().equals(this.updateInfo.deleteTarget.getGroup())){
    				//TODO: warning about multiple
    				this.updateInfo.deleteTarget = null;
    			}
    		}
    		updatable |= info.updateAllowed;
    		insertable |= info.insertAllowed;
    	}
    	if ((this.updateInfo.inherentInsert && !insertable)) {
    		report.handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0015")); //$NON-NLS-1$
    	} 
    	if (this.updateInfo.inherentUpdate && !updatable) {
    		report.handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0005")); //$NON-NLS-1$
    	}
    	if (this.updateInfo.inherentDelete && this.updateInfo.deleteTarget == null) {
    		if (this.updateInfo.isSimple) {
    			this.updateInfo.deleteTarget = this.updateInfo.updatableGroups.values().iterator().next();
    		} else {
    			report.handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0014")); //$NON-NLS-1$
    		}
    	}
    }
    
    private void setUpdateFlags(GroupSymbol groupSymbol) throws QueryMetadataException, TeiidComponentException {
    	UpdateMapping info = updateInfo.updatableGroups.get(groupSymbol.getCanonicalName());

		if (info == null) {
			return; // not projected
		}

		if (!metadata.groupSupports(groupSymbol.getMetadataID(), SupportConstants.Group.UPDATE)) {
			report.handleValidationWarning(QueryPlugin.Util.getString("ERR.015.012.0003", groupSymbol)); //$NON-NLS-1$
			return;
		}

		info.insertAllowed = true;
		for (ElementSymbol es : ResolverUtil.resolveElementsInGroup(info.group, metadata)) {
			if (!info.updatableViewSymbols.values().contains(es) && !validateInsertElement(es)) {
				info.insertAllowed = false;
			}
		}
		info.updateAllowed = true;
    }

	/**
	 * <p> This method validates an elements present in the group specified in the
	 * FROM clause of the query but not specified in its SELECT clause</p>
	 * @param element The <code>ElementSymbol</code> being validated
	 * @throws TeiidComponentException 
	 * @throws QueryMetadataException 
	 */
	private boolean validateInsertElement(ElementSymbol element) throws QueryMetadataException, TeiidComponentException {
		// checking if the elements not specified in the query are required.
		if(metadata.elementSupports(element.getMetadataID(), SupportConstants.Element.NULL) 
			|| metadata.elementSupports(element.getMetadataID(), SupportConstants.Element.DEFAULT_VALUE) 
			|| metadata.elementSupports(element.getMetadataID(), SupportConstants.Element.AUTO_INCREMENT)) {
			return true;
		}
		if (this.updateInfo.inherentInsert) {
			report.handleValidationWarning(QueryPlugin.Util.getString("ERR.015.012.0010", element, element.getGroupSymbol())); //$NON-NLS-1$
		}
	    return false;
	}
}
