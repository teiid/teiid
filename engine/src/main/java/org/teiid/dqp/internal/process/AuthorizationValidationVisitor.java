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

import java.util.*;

import org.teiid.CommandContext;
import org.teiid.PolicyDecider;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.DataPolicy.Context;
import org.teiid.adminapi.DataPolicy.PermissionType;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.dqp.internal.process.multisource.MultiSourceElement;
import org.teiid.logging.AuditMessage;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;
import org.teiid.query.validator.AbstractValidationVisitor;


public class AuthorizationValidationVisitor extends AbstractValidationVisitor {
    
    private CommandContext commandContext;
    private PolicyDecider decider;

    public AuthorizationValidationVisitor(PolicyDecider decider, CommandContext commandContext) {
        this.decider = decider;
        this.commandContext = commandContext;
    }

    // ############### Visitor methods for language objects ##################
    
    @Override
    public void visit(Create obj) {
    	validateTemp(PermissionType.CREATE, obj.getTable().getNonCorrelationName(), false, obj.getTable(), Context.CREATE);
    }
    
    @Override
    public void visit(DynamicCommand obj) {
    	if (obj.getIntoGroup() != null) {
    		validateTemp(PermissionType.CREATE, obj.getIntoGroup().getNonCorrelationName(), false, obj.getIntoGroup(), Context.CREATE);
    	}
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
    
    @Override
    public void visit(ObjectTable objectTable) {
    	String language = ObjectTable.DEFAULT_LANGUAGE;
    	if (objectTable.getScriptingLanguage() != null) {
    		language = objectTable.getScriptingLanguage();
    	}
    	Map<String, LanguageObject> map = new HashMap<String, LanguageObject>();
    	map.put(language, objectTable);
    	validateEntitlements(PermissionType.LANGUAGE, Context.QUERY, map);
    }

	private void validateTemp(DataPolicy.PermissionType action, String resource, boolean schema, LanguageObject object, Context context) {
		Set<String> resources = Collections.singleton(resource);
		logRequest(resources, context);
        
    	boolean allowed = decider.isTempAccessible(action, schema?resource:null, context, commandContext);
    	
    	logResult(resources, context, allowed);
    	if (!allowed) {
		    handleValidationError(
			        QueryPlugin.Util.getString("ERR.018.005.0095", commandContext.getUserName(), "CREATE_TEMPORARY_TABLES"), //$NON-NLS-1$  //$NON-NLS-2$
			        Arrays.asList(object));
    	}
	}

	private void logRequest(Set<String> resources, Context context) {
		if (LogManager.isMessageToBeRecorded(LogConstants.CTX_AUDITLOGGING, MessageLevel.DETAIL)) {
	        // Audit - request
	    	AuditMessage msg = new AuditMessage(context.name(), "getInaccessibleResources-request", resources.toArray(new String[resources.size()]), commandContext); //$NON-NLS-1$
	    	LogManager.logDetail(LogConstants.CTX_AUDITLOGGING, msg);
        }
	}
    
    @Override
    public void visit(Drop obj) {
    	validateTemp(PermissionType.DROP, obj.getTable().getNonCorrelationName(), false, obj.getTable(), Context.DROP);
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
    	} else {
    		String schema = obj.getFunctionDescriptor().getSchema();
    		if (schema != null && !isSystemSchema(schema)) {
    			Map<String, Function> map = new HashMap<String, Function>();
    			map.put(obj.getFunctionDescriptor().getFullName(), obj);
    			validateEntitlements(PermissionType.EXECUTE, Context.FUNCTION, map);
    		}
    	}
    }

    // ######################### Validation methods #########################

    /**
     * Validate insert/merge entitlements
     */
    protected void validateEntitlements(Insert obj) {
    	List<LanguageObject> insert = new LinkedList<LanguageObject>();
    	insert.add(obj.getGroup());
    	insert.addAll(obj.getVariables());
        validateEntitlements(
        		insert,
            DataPolicy.PermissionType.CREATE,
            Context.INSERT);
        if (obj.isUpsert()) {
        	validateEntitlements(
            		insert,
                DataPolicy.PermissionType.UPDATE,
                Context.MERGE);
        }
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
        List<LanguageObject> updated = new LinkedList<LanguageObject>();
        updated.add(obj.getGroup());
        updated.addAll(obj.getChangeList().getClauseMap().keySet());
        validateEntitlements(updated, DataPolicy.PermissionType.UPDATE, Context.UPDATE);
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
            Collection<LanguageObject> intoElements = new LinkedList<LanguageObject>();
            intoElements.add(intoGroup);
            try {
                intoElements.addAll(ResolverUtil.resolveElementsInGroup(intoGroup, getMetadata()));
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
        Collection<LanguageObject> entitledObjects = new ArrayList<LanguageObject>(GroupCollectorVisitor.getGroupsIgnoreInlineViews(obj, true));
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
        Map<String, LanguageObject> nameToSymbolMap = new LinkedHashMap<String, LanguageObject>();
        for (LanguageObject symbol : symbols) {
            try {
                Object metadataID = null;
                if(symbol instanceof ElementSymbol) {                    
                    metadataID = ((ElementSymbol)symbol).getMetadataID();
                    if (metadataID instanceof MultiSourceElement || metadataID instanceof TempMetadataID) {
                        continue;
                    }
                } else if(symbol instanceof GroupSymbol) {
                    GroupSymbol group = (GroupSymbol)symbol;
                    metadataID = group.getMetadataID();
                    if (metadataID instanceof TempMetadataID) {
                    	if (group.isProcedure()) {
                    		Map<String, LanguageObject> procMap = new LinkedHashMap<String, LanguageObject>();
                    		addToNameMap(((TempMetadataID)metadataID).getOriginalMetadataID(), symbol, procMap, getMetadata());
                    		validateEntitlements(PermissionType.EXECUTE, auditContext, procMap);
                    	} else if (group.isTempTable() && group.isImplicitTempGroupSymbol()) {
                    		validateTemp(actionCode, group.getNonCorrelationName(), false, group, auditContext);
                    	}
                        continue;
                    }
                }
                addToNameMap(metadataID, symbol, nameToSymbolMap, getMetadata());
            } catch(QueryMetadataException e) {
                handleException(e);
            } catch(TeiidComponentException e) {
                handleException(e);
            }
        }

        validateEntitlements(actionCode, auditContext, nameToSymbolMap);
	}
    
    static void addToNameMap(Object metadataID, LanguageObject symbol, Map<String, LanguageObject> nameToSymbolMap, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
    	String fullName = metadata.getFullName(metadataID);
        Object modelId = metadata.getModelID(metadataID);
        String modelName = metadata.getFullName(modelId);
        if (!isSystemSchema(modelName)) {
        	//foreign temp table full names are not schema qualified by default
        	if (!metadata.isVirtualModel(modelId)) {
        		GroupSymbol group = null;
        		if (symbol instanceof ElementSymbol) {
        			group = ((ElementSymbol)symbol).getGroupSymbol();
        		} else if (symbol instanceof GroupSymbol) {
        			group = (GroupSymbol)symbol; 
        		}
        		if (group != null && group.isTempGroupSymbol() && !group.isGlobalTable()) {
            		fullName = modelName + AbstractMetadataRecord.NAME_DELIM_CHAR + modelId;
        		}
        	}
        	nameToSymbolMap.put(fullName, symbol);
        }
    }

	static private boolean isSystemSchema(String modelName) {
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
	        QueryPlugin.Util.getString("ERR.018.005.0095", commandContext.getUserName(), actionCode), //$NON-NLS-1$                    
	        inaccessibleSymbols);
	}

    /**
     * Out of the resources specified, return the subset for which the specified not have authorization to access.
     */
    public Set<String> getInaccessibleResources(DataPolicy.PermissionType action, Set<String> resources, Context context) {
        logRequest(resources, context);
        
		Set<String> results = decider.getInaccessibleResources(action, resources, context, commandContext);

		logResult(resources, context, results.isEmpty());
        return results;
    }

	private void logResult(Set<String> resources, Context context,
			boolean granted) {
		if (LogManager.isMessageToBeRecorded(LogConstants.CTX_AUDITLOGGING, MessageLevel.DETAIL)) {
	        if (granted) {
	        	AuditMessage msg = new AuditMessage(context.name(), "getInaccessibleResources-granted all", resources.toArray(new String[resources.size()]), commandContext); //$NON-NLS-1$
	        	LogManager.logDetail(LogConstants.CTX_AUDITLOGGING, msg);
	        } else {
	        	AuditMessage msg = new AuditMessage(context.name(), "getInaccessibleResources-denied", resources.toArray(new String[resources.size()]), commandContext); //$NON-NLS-1$
	        	LogManager.logDetail(LogConstants.CTX_AUDITLOGGING, msg);
	        }
		}
	}    
}
