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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.DataPolicy.PermissionType;
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
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.AlterProcedure;
import org.teiid.query.sql.lang.AlterTrigger;
import org.teiid.query.sql.lang.AlterView;
import org.teiid.query.sql.lang.Create;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.Drop;
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
		CREATE,
		DROP,
		QUERY,
		INSERT,
		UPDATE,
		DELETE,
		FUNCTION,
		ALTER,
		STORED_PROCEDURE;
    }
    
    private HashMap<String, DataPolicy> allowedPolicies;
    private String userName;
    private boolean allowCreateTemporaryTablesDefault = true;
    private boolean allowFunctionCallsByDefault = true;

    public AuthorizationValidationVisitor(HashMap<String, DataPolicy> policies, String user) {
        this.allowedPolicies = policies;
        this.userName = user;
    }
    
    public void setAllowCreateTemporaryTablesDefault(
			boolean allowCreateTemporaryTablesDefault) {
		this.allowCreateTemporaryTablesDefault = allowCreateTemporaryTablesDefault;
	}
    
    public void setAllowFunctionCallsByDefault(boolean allowFunctionCallsDefault) {
		this.allowFunctionCallsByDefault = allowFunctionCallsDefault;
	}

    // ############### Visitor methods for language objects ##################
    
    @Override
    public void visit(Create obj) {
    	Set<String> resources = Collections.singleton(obj.getTable().getName());
    	Collection<GroupSymbol> symbols = Arrays.asList(obj.getTable());
    	validateTemp(resources, symbols, Context.CREATE);
    }
    
    @Override
    public void visit(AlterProcedure obj) {
    	validateEntitlements(Arrays.asList(obj.getTarget()), DataPolicy.PermissionType.ALTER, Context.ALTER);
    }
    
    @Override
    public void visit(AlterTrigger obj) {
    	validateEntitlements(Arrays.asList(obj.getTarget()), DataPolicy.PermissionType.ALTER, obj.isCreate()?Context.CREATE:Context.ALTER);
    }
    
    @Override
    public void visit(AlterView obj) {
    	validateEntitlements(Arrays.asList(obj.getTarget()), DataPolicy.PermissionType.ALTER, Context.ALTER);
    }

	private void validateTemp(Set<String> resources,
			Collection<GroupSymbol> symbols, Context context) {
		logRequest(resources, context);
        
    	boolean allowed = false;
    	for(DataPolicy p:this.allowedPolicies.values()) {
			DataPolicyMetadata policy = (DataPolicyMetadata)p;
			
			if (policy.isAllowCreateTemporaryTables() == null) {
				if (allowCreateTemporaryTablesDefault) {
					allowed = true;
					break;
				}
			} else if (policy.isAllowCreateTemporaryTables()) {
				allowed = true;
				break;
			}
		}
    	
    	logResult(resources, context, allowed);
    	if (!allowed) {
		    handleValidationError(
			        QueryPlugin.Util.getString("ERR.018.005.0095", userName, "CREATE_TEMPORARY_TABLES"), //$NON-NLS-1$  //$NON-NLS-2$
			        symbols);
    	}
	}

	private void logRequest(Set<String> resources, Context context) {
		if (LogManager.isMessageToBeRecorded(LogConstants.CTX_AUDITLOGGING, MessageLevel.DETAIL)) {
	        // Audit - request
	    	AuditMessage msg = new AuditMessage(context.name(), "getInaccessibleResources-request", this.userName, resources.toArray(new String[resources.size()])); //$NON-NLS-1$
	    	LogManager.logDetail(LogConstants.CTX_AUDITLOGGING, msg);
        }
	}
    
    @Override
    public void visit(Drop obj) {
    	Set<String> resources = Collections.singleton(obj.getTable().getName());
    	Collection<GroupSymbol> symbols = Arrays.asList(obj.getTable());
    	validateTemp(resources, symbols, Context.CREATE);
    }
    
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
    	} else if (!allowFunctionCallsByDefault) {
    		String schema = obj.getFunctionDescriptor().getSchema();
    		if (schema != null && !isSystemSchema(schema)) {
    			Map<String, Function> map = new HashMap<String, Function>();
    			map.put(schema + '.' + obj.getFunctionDescriptor().getName(), obj);
    			validateEntitlements(PermissionType.EXECUTE, Context.FUNCTION, map);
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
    	HashSet<ElementSymbol> elements = new HashSet<ElementSymbol>(); 
    	ElementCollectorVisitor.getElements(obj.getChangeList().getClauseMap().values(), elements);
        if (obj.getCriteria() != null) {
            ElementCollectorVisitor.getElements(obj.getCriteria(), elements);
        }
        validateEntitlements(
        		elements,
                DataPolicy.PermissionType.READ,
                Context.UPDATE);

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
        validateEntitlements(Arrays.asList(obj.getGroup()), DataPolicy.PermissionType.EXECUTE, Context.STORED_PROCEDURE);
    }

    /**
     * Check that the user is entitled to access all data elements in the command.
     *
     * @param symbols The collection of <code>Symbol</code>s affected by these actions.
     * @param actionCode The actions to validate for
     * @param auditContext The {@link AuthorizationService} to use when resource auditing is done.
     */
    protected void validateEntitlements(Collection<? extends LanguageObject> symbols, DataPolicy.PermissionType actionCode, Context auditContext) {
        Map<String, LanguageObject> nameToSymbolMap = new HashMap<String, LanguageObject>();
        for (LanguageObject symbol : symbols) {
            try {
                String fullName = null;
                Object metadataID = null;
                if(symbol instanceof ElementSymbol) {                    
                    metadataID = ((ElementSymbol)symbol).getMetadataID();
                    if (metadataID instanceof MultiSourceElement || metadataID instanceof TempMetadataID) {
                        continue;
                    }
                } else if(symbol instanceof GroupSymbol) {
                    GroupSymbol group = (GroupSymbol)symbol;
                    metadataID = group.getMetadataID();
                    if (metadataID instanceof TempMetadataID && !group.isProcedure()) {
                        continue;
                    }
                }
                fullName = getMetadata().getFullName(metadataID);
                Object modelId = getMetadata().getModelID(metadataID);
                String modelName = getMetadata().getFullName(modelId);
                if (isSystemSchema(modelName)) {
                	continue;
                }
                nameToSymbolMap.put(fullName, symbol);
            } catch(QueryMetadataException e) {
                handleException(e);
            } catch(TeiidComponentException e) {
                handleException(e);
            }
        }

        validateEntitlements(actionCode, auditContext, nameToSymbolMap);
	}

	private boolean isSystemSchema(String modelName) {
		return CoreConstants.SYSTEM_MODEL.equalsIgnoreCase(modelName) || CoreConstants.ODBC_MODEL.equalsIgnoreCase(modelName);
	}

	private void validateEntitlements(DataPolicy.PermissionType actionCode,
			Context auditContext, Map<String, ? extends LanguageObject> nameToSymbolMap) {
		if (nameToSymbolMap.isEmpty()) {
			return;
		}
		Collection<String> inaccessibleResources = getInaccessibleResources(actionCode, nameToSymbolMap.keySet(), auditContext);
		if(inaccessibleResources.isEmpty()) {
			return;
		}
		List<LanguageObject> inaccessibleSymbols = new ArrayList<LanguageObject>(inaccessibleResources.size());
		for (String name : inaccessibleResources) {
	        inaccessibleSymbols.add(nameToSymbolMap.get(name));
	    }
	    
	    // CASE 2362 - do not include the names of the elements for which the user
	    // is not authorized in the exception message
	    
	    handleValidationError(
	        QueryPlugin.Util.getString("ERR.018.005.0095", userName, actionCode), //$NON-NLS-1$                    
	        inaccessibleSymbols);
	}

    /**
     * Out of resources specified, return the subset for which the specified not have authorization to access.
     */
    public Set<String> getInaccessibleResources(DataPolicy.PermissionType action, Set<String> resources, Context context) {
        logRequest(resources, context);
        
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

		logResult(resources, context, results.isEmpty());
        return results;
    }

	private void logResult(Set<String> resources, Context context,
			boolean granted) {
		if (LogManager.isMessageToBeRecorded(LogConstants.CTX_AUDITLOGGING, MessageLevel.DETAIL)) {
	        if (granted) {
	        	AuditMessage msg = new AuditMessage(context.name(), "getInaccessibleResources-granted all", this.userName, resources.toArray(new String[resources.size()])); //$NON-NLS-1$
	        	LogManager.logDetail(LogConstants.CTX_AUDITLOGGING, msg);
	        } else {
	        	AuditMessage msg = new AuditMessage(context.name(), "getInaccessibleResources-denied", this.userName, resources.toArray(new String[resources.size()])); //$NON-NLS-1$
	        	LogManager.logDetail(LogConstants.CTX_AUDITLOGGING, msg);
	        }
		}
	}    
}
