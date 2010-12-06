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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.util.StringUtil;
import org.teiid.language.SQLConstants;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SupportConstants;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.lang.SetQuery.Operation;
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
	
	public enum UpdateType {
		/**
		 * The default handling should be used
		 */
		INHERENT, 
		/**
		 * A procedure handler has been defined
		 */
		UPDATE_PROCEDURE,
		/**
		 * An instead of trigger (TriggerAction) has been defined
		 */
		INSTEAD_OF
	}
	
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
		private UpdateType updateType;
		private boolean updateValidationError;
		private UpdateType deleteType;
		private boolean deleteValidationError;
		private UpdateType insertType;
		private boolean insertValidationError;
		private Query view;
		private List<UpdateInfo> unionBranches = new LinkedList<UpdateInfo>();
		
		public boolean isSimple() {
			return isSimple;
		}
		
		public UpdateMapping getDeleteTarget() {
			return deleteTarget;
		}
		
		public boolean isInherentDelete() {
			return deleteType == UpdateType.INHERENT;
		}
		
		public boolean isInherentInsert() {
			return insertType == UpdateType.INHERENT;
		}
		
		public boolean isInherentUpdate() {
			return updateType == UpdateType.INHERENT;
		}
		
		public UpdateType getUpdateType() {
			return updateType;
		}
		
		public UpdateType getDeleteType() {
			return deleteType;
		}
		
		public boolean hasValidUpdateMapping(Collection<ElementSymbol> updateCols) {
			if (findUpdateMapping(updateCols, false) == null) {
				return false;
			}
			for (UpdateInfo info : this.unionBranches) {
				if (info.findUpdateMapping(updateCols, false) == null) {
					return false;
				}
			}
			return true;
		}
		
		public UpdateMapping findUpdateMapping(Collection<ElementSymbol> updateCols, boolean insert) {
			for (UpdateMapping entry : this.updatableGroups.values()) {
				if (((insert && entry.insertAllowed) || (!insert && entry.updateAllowed)) && entry.updatableViewSymbols.keySet().containsAll(updateCols)) {
					return entry;
				}
			}
			return null;
		}
		
		public Query getViewDefinition() {
			return view;
		}
		
		public boolean isDeleteValidationError() {
			return deleteValidationError;
		}
		
		public boolean isInsertValidationError() {
			return insertValidationError;
		}
		
		public boolean isUpdateValidationError() {
			return updateValidationError;
		}
		
		public List<UpdateInfo> getUnionBranches() {
			return unionBranches;
		}
		
	}
	
	private QueryMetadataInterface metadata;
	private ValidatorReport report = new ValidatorReport();
	private UpdateInfo updateInfo = new UpdateInfo();

	public UpdateValidator(QueryMetadataInterface qmi, String updatePlan, String deletePlan, String insertPlan) {
		this.metadata = qmi;
		this.updateInfo.deleteType = determineType(deletePlan);
		this.updateInfo.insertType = determineType(insertPlan);
		this.updateInfo.updateType = determineType(updatePlan);
	}

	private UpdateType determineType(String plan) {
		UpdateType type = UpdateType.INHERENT;
		if (plan != null) {
			if (StringUtil.startsWithIgnoreCase(plan, SQLConstants.Reserved.CREATE)) {
				type = UpdateType.UPDATE_PROCEDURE;
			} else {
				type = UpdateType.INSTEAD_OF;
			}
		}
		return type;
	}
	
	public UpdateInfo getUpdateInfo() {
		return updateInfo;
	}
	
	public ValidatorReport getReport() {
		return report;
	}
	
	private void handleValidationError(String error, boolean update, boolean insert, boolean delete) {
		report.handleValidationError(error);
		updateInfo.updateValidationError |= update;
		updateInfo.insertValidationError |= insert;
		updateInfo.deleteValidationError |= delete;
	}
	
    public void validate(Command command, List<ElementSymbol> viewSymbols) throws QueryMetadataException, TeiidComponentException {
    	if (command instanceof SetQuery) {
    		SetQuery setQuery = (SetQuery)command;
        	if (setQuery.getLimit() != null) {
        		handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0013"), true, true, true); //$NON-NLS-1$
        		return;
        	}
        	if (setQuery.getOperation() != Operation.UNION || !setQuery.isAll()) {
        		handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0001"), true, true, true); //$NON-NLS-1$
        		return;
        	}
        	validateBranch(viewSymbols, setQuery.getLeftQuery());
        	validateBranch(viewSymbols, setQuery.getRightQuery());
        	return;
    	}
    	
    	if (!(command instanceof Query)) {
    		handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0001"), true, true, true); //$NON-NLS-1$
    		return;
        }
    	
    	Query query = (Query)command;

    	if (query.getFrom() == null || query.getInto() != null) {
    		handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0001"), true, true, true); //$NON-NLS-1$
    		return;
    	}
    	
    	if (query.getWith() != null) {
    		handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0002"), true, true, true); //$NON-NLS-1$
    		updateInfo.isSimple = false;
    	}

    	if (query.hasAggregates()) {
    		handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0006"), true, true, true); //$NON-NLS-1$
    		return;
    	}
    	
    	if (query.getLimit() != null) {
    		handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0013"), true, true, true); //$NON-NLS-1$
    		return;
    	}
    	
    	if (query.getSelect().isDistinct()) {
    		handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0008"), true, true, true); //$NON-NLS-1$
    		return;
    	} 
    	
    	updateInfo.view = query;
    	
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
            	
                report.handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0007", viewSymbols.get(i), symbol)); //$NON-NLS-1$
            }
    	}
    	
    	if (query.getFrom().getClauses().size() > 1 || (!(query.getFrom().getClauses().get(0) instanceof UnaryFromClause))) {
    		report.handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0009", query.getFrom())); //$NON-NLS-1$
    		updateInfo.isSimple = false;
    	}
    	List<GroupSymbol> allGroups = query.getFrom().getGroups();
    	HashSet<GroupSymbol> keyPreservingGroups = new HashSet<GroupSymbol>();
    	
		ResolverUtil.findKeyPreserved(query, keyPreservingGroups, metadata);
    	
		for (GroupSymbol groupSymbol : keyPreservingGroups) {
			setUpdateFlags(groupSymbol);
		}

		allGroups.removeAll(keyPreservingGroups);
		if (updateInfo.isSimple) {
			if (!allGroups.isEmpty()) {
				setUpdateFlags(allGroups.iterator().next());
			}
		} else if (this.updateInfo.updateType == UpdateType.INHERENT || this.updateInfo.deleteType == UpdateType.INHERENT) {
			for (GroupSymbol groupSymbol : allGroups) {
				UpdateMapping info = updateInfo.updatableGroups.get(groupSymbol.getCanonicalName());
				if (info == null) {
					continue; // not projected
				}
				report.handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0004", groupSymbol)); //$NON-NLS-1$
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
    	if ((this.updateInfo.insertType == UpdateType.INHERENT && !insertable)) {
    		handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0015"), false, true, false); //$NON-NLS-1$
    	} 
    	if (this.updateInfo.updateType == UpdateType.INHERENT && !updatable) {
    		handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0005"), true, false, true); //$NON-NLS-1$
    	}
    	if (this.updateInfo.deleteType == UpdateType.INHERENT && this.updateInfo.deleteTarget == null) {
    		if (this.updateInfo.isSimple) {
    			this.updateInfo.deleteTarget = this.updateInfo.updatableGroups.values().iterator().next();
    		} else {
    			handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0014"), false, false, true); //$NON-NLS-1$
    		}
    	}
    }

	private void validateBranch(List<ElementSymbol> viewSymbols,
			QueryCommand query) throws QueryMetadataException,
			TeiidComponentException {
		if (!this.updateInfo.insertValidationError) {
    		handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0018"), false, true, false); //$NON-NLS-1$
    	}
		if (!this.updateInfo.isInherentDelete() && !this.updateInfo.isInherentUpdate()) {
			return; //don't bother
		}
		UpdateValidator uv = this;
		if (this.updateInfo.view != null) {
			uv = new UpdateValidator(metadata, null, null, null);
			uv.updateInfo.deleteType = this.updateInfo.deleteType;
			uv.updateInfo.insertType = this.updateInfo.insertType;
			uv.updateInfo.updateType = this.updateInfo.updateType;
		}
		uv.validate(query, viewSymbols);
		if (uv != this) {
			UpdateInfo info = uv.getUpdateInfo();
			this.updateInfo.deleteValidationError |= info.deleteValidationError;
			this.updateInfo.updateValidationError |= info.updateValidationError;
			if (info.view != null) {
				this.updateInfo.unionBranches.add(info);
			} else {
				this.updateInfo.unionBranches.addAll(info.unionBranches);
			}
		}
	}
    
    private void setUpdateFlags(GroupSymbol groupSymbol) throws QueryMetadataException, TeiidComponentException {
    	UpdateMapping info = updateInfo.updatableGroups.get(groupSymbol.getCanonicalName());

		if (info == null) {
			return; // not projected
		}

		if (!metadata.groupSupports(groupSymbol.getMetadataID(), SupportConstants.Group.UPDATE)) {
			report.handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0003", groupSymbol)); //$NON-NLS-1$
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
		if (this.updateInfo.insertType == UpdateType.INHERENT) {
			report.handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0010", element, element.getGroupSymbol())); //$NON-NLS-1$
		}
	    return false;
	}
}
