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

package org.teiid.dqp.internal.process;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.dqp.internal.process.multisource.MultiSourceElement;
import org.teiid.logging.AuditMessage;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Into;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;
import org.teiid.query.validator.AbstractValidationVisitor;


public class AuthorizationValidationVisitor extends AbstractValidationVisitor {
    
	public enum Context {
		QUERY,
		INSERT,
		UPDATE,
		DELETE,
		STORED_PROCEDURE;
    }
    
    private HashMap<String, DataPolicy> allowedPolicies;
    private String userName;

    public AuthorizationValidationVisitor(HashMap<String, DataPolicy> policies, String user) {
        this.allowedPolicies = policies;
        this.userName = user;
    }

    // ############### Visitor methods for language objects ##################
    
    public void visit(Delete obj) {
    	validateEntitlements(obj);
    }

    public void visit(Insert obj) {
    	validateEntitlements(obj);
    }

    public void visit(Query obj) {
    	validateEntitlements(obj);
    }

    public void visit(Update obj) {
    	validateEntitlements(obj);
    }

    public void visit(StoredProcedure obj) {
    	validateEntitlements(obj);
    }
    
    public void visit(Function obj) {
    	if (FunctionLibrary.LOOKUP.equalsIgnoreCase(obj.getName())) {
    		try {
				ResolverUtil.ResolvedLookup lookup = ResolverUtil.resolveLookup(obj, this.getMetadata());
    			List<Symbol> symbols = new LinkedList<Symbol>();
				symbols.add(lookup.getGroup());
				symbols.add(lookup.getKeyElement());
				symbols.add(lookup.getReturnElement());
	    		validateEntitlements(symbols, DataPolicy.PermissionType.READ, Context.QUERY);
			} catch (TeiidComponentException e) {
				handleException(e, obj);
			} catch (TeiidProcessingException e) {
				handleException(e, obj);
			}
    	}
    }

    // ######################### Validation methods #########################

    /**
     * Validate insert entitlements
     */
    protected void validateEntitlements(Insert obj) {
        validateEntitlements(
            obj.getVariables(),
            DataPolicy.PermissionType.CREATE,
            Context.INSERT);
    }

    /**
     * Validate update entitlements
     */
    protected void validateEntitlements(Update obj) {
        // Check that all elements used in criteria have read permission
        if (obj.getCriteria() != null) {
            validateEntitlements(
                ElementCollectorVisitor.getElements(obj.getCriteria(), true),
                DataPolicy.PermissionType.READ,
                Context.UPDATE);
        }

        // The variables from the changes must be checked for UPDATE entitlement
        // validateEntitlements on all the variables used in the update.
        validateEntitlements(obj.getChangeList().getClauseMap().keySet(), DataPolicy.PermissionType.UPDATE, Context.UPDATE);
    }

    /**
     * Validate delete entitlements
     */
    protected void validateEntitlements(Delete obj) {
        // Check that all elements used in criteria have read permission
        if (obj.getCriteria() != null) {
            validateEntitlements(
                ElementCollectorVisitor.getElements(obj.getCriteria(), true),
                DataPolicy.PermissionType.READ,
                Context.DELETE);
        }

        // Check that all elements of group being deleted have delete permission
        validateEntitlements(Arrays.asList(obj.getGroup()), DataPolicy.PermissionType.DELETE, Context.DELETE);
    }

    /**
     * Validate query entitlements
     */
    protected void validateEntitlements(Query obj) {
        // If query contains SELECT INTO, validate INTO portion
        Into intoObj = obj.getInto();
        if ( intoObj != null ) {
            GroupSymbol intoGroup = intoObj.getGroup();
            List<ElementSymbol> intoElements = null;
            try {
                intoElements = ResolverUtil.resolveElementsInGroup(intoGroup, getMetadata());
            } catch (QueryMetadataException err) {
                handleException(err, intoGroup);
            } catch (TeiidComponentException err) {
                handleException(err, intoGroup);
            }
            validateEntitlements(intoElements,
                                 DataPolicy.PermissionType.CREATE,
                                 Context.INSERT);
        }

        // Validate this query's entitlements
        Collection entitledObjects = GroupCollectorVisitor.getGroups(obj, true);
        if (!isXMLCommand(obj)) {
            entitledObjects.addAll(ElementCollectorVisitor.getElements(obj, true));
        }

        if(entitledObjects.size() == 0) {
            return;
        }
        
        validateEntitlements(entitledObjects, DataPolicy.PermissionType.READ, Context.QUERY);
    }

    /**
     * Validate query entitlements
     */
    protected void validateEntitlements(StoredProcedure obj) {
        validateEntitlements(Arrays.asList(obj.getGroup()), DataPolicy.PermissionType.READ, Context.STORED_PROCEDURE);
    }

    private String getActionLabel(DataPolicy.PermissionType actionCode) {
        switch(actionCode) {
            case READ:    return "Read"; //$NON-NLS-1$
            case CREATE:  return "Create"; //$NON-NLS-1$
            case UPDATE:  return "Update"; //$NON-NLS-1$
            case DELETE:  return "Delete"; //$NON-NLS-1$
            default:    return "UNKNOWN"; //$NON-NLS-1$
        }
    }

    /**
     * Check that the user is entitled to access all data elements in the command.
     *
     * @param symbols The collection of <code>Symbol</code>s affected by these actions.
     * @param actionCode The actions to validate for
     * @param auditContext The {@link AuthorizationService} to use when resource auditing is done.
     */
    protected void validateEntitlements(Collection<? extends Symbol> symbols, DataPolicy.PermissionType actionCode, Context auditContext) {
        Map<String, Symbol> nameToSymbolMap = new HashMap<String, Symbol>();
        for (Symbol symbol : symbols) {
            try {
                String fullName = null;
                Object metadataID = null;
                if(symbol instanceof ElementSymbol) {                    
                    metadataID = ((ElementSymbol)symbol).getMetadataID();
                    GroupSymbol groupSymbol = ((ElementSymbol)symbol).getGroupSymbol();
                    if (metadataID instanceof MultiSourceElement || metadataID instanceof TempMetadataID || groupSymbol == null || isSystemModel(groupSymbol.getMetadataID())) {
                        continue;
                    }
                } else if(symbol instanceof GroupSymbol) {
                    GroupSymbol group = (GroupSymbol)symbol;
                    metadataID = group.getMetadataID();
                    if ((metadataID instanceof TempMetadataID && !group.isProcedure()) || isSystemModel(metadataID)) {
                        continue;
                    }
                }
                fullName = getMetadata().getFullName(metadataID);
                nameToSymbolMap.put(fullName, symbol);
            } catch(QueryMetadataException e) {
                handleException(e);
            } catch(TeiidComponentException e) {
                handleException(e);
            }
        }

        if (!nameToSymbolMap.isEmpty()) {
            Collection<String> inaccessibleResources = getInaccessibleResources(actionCode, nameToSymbolMap.keySet(), auditContext);
            if(inaccessibleResources.size() > 0) {                              
            	List<Symbol> inaccessibleSymbols = new ArrayList<Symbol>(inaccessibleResources.size());
            	for (String name : inaccessibleResources) {
                    inaccessibleSymbols.add(nameToSymbolMap.get(name));
                }
                
                // CASE 2362 - do not include the names of the elements for which the user
                // is not authorized in the exception message
                
                handleValidationError(
                    QueryPlugin.Util.getString("ERR.018.005.0095", userName, getActionLabel(actionCode)), //$NON-NLS-1$                    
                    inaccessibleSymbols);
            }
        }

    }
    
    private boolean isSystemModel(Object groupMetadataID) throws QueryMetadataException, TeiidComponentException {
        Object modelId = getMetadata().getModelID(groupMetadataID);
        String modelName = getMetadata().getFullName(modelId);
        return (CoreConstants.SYSTEM_MODEL.equals(modelName) || CoreConstants.ODBC_MODEL.equals(modelName));
    }

    /**
     * Out of resources specified, return the subset for which the specified not have authorization to access.
     */
    public Set<String> getInaccessibleResources(DataPolicy.PermissionType action, Set<String> resources, Context context) {
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_AUDITLOGGING, MessageLevel.DETAIL)) {
	        // Audit - request
	    	AuditMessage msg = new AuditMessage(context.name(), "getInaccessibleResources-request", this.userName, resources.toArray(new String[resources.size()])); //$NON-NLS-1$
	    	LogManager.logDetail(LogConstants.CTX_AUDITLOGGING, msg);
        }
        
        HashSet<String> results = new HashSet<String>(resources);
        
		for(DataPolicy p:this.allowedPolicies.values()) {
			DataPolicyMetadata policy = (DataPolicyMetadata)p;
			
			if (results.isEmpty()) {
				break;
			}
			
			Iterator<String> i = results.iterator();
			while (i.hasNext()) {				
				if (policy.allows(i.next(), action)) {
					i.remove();
				}
			}
		}

		if (LogManager.isMessageToBeRecorded(LogConstants.CTX_AUDITLOGGING, MessageLevel.DETAIL)) {
	        if (results.isEmpty()) {
	        	AuditMessage msg = new AuditMessage(context.name(), "getInaccessibleResources-granted all", this.userName, resources.toArray(new String[resources.size()])); //$NON-NLS-1$
	        	LogManager.logDetail(LogConstants.CTX_AUDITLOGGING, msg);
	        } else {
	        	AuditMessage msg = new AuditMessage(context.name(), "getInaccessibleResources-denied", this.userName, resources.toArray(new String[resources.size()])); //$NON-NLS-1$
	        	LogManager.logDetail(LogConstants.CTX_AUDITLOGGING, msg);
	        }
		}
        return results;
    }    
}
