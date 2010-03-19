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

package org.teiid.dqp.internal.process.validator;

import java.util.ArrayList;
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
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.internal.process.multisource.MultiSourceElement;
import org.teiid.logging.api.AuditMessage;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.query.function.FunctionLibrary;
import com.metamatrix.query.metadata.TempMetadataID;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.sql.lang.Delete;
import com.metamatrix.query.sql.lang.Insert;
import com.metamatrix.query.sql.lang.Into;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.lang.Update;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.Symbol;
import com.metamatrix.query.sql.visitor.ElementCollectorVisitor;
import com.metamatrix.query.sql.visitor.GroupCollectorVisitor;
import com.metamatrix.query.validator.AbstractValidationVisitor;

public class AuthorizationValidationVisitor extends AbstractValidationVisitor {
    
	public enum Context {
		QUERY,
		INSERT,
		UPDATE,
		DELETE,
		STORED_PROCEDURE;
    }
    
    private VDBMetaData vdb;
    private HashMap<String, DataPolicy> allowedPolicies;
    private String userName;
    private boolean useEntitlements;

    public AuthorizationValidationVisitor(VDBMetaData vdb, boolean useEntitlements, HashMap<String, DataPolicy> policies, String user) {
        this.vdb = vdb;
        this.allowedPolicies = policies;
        this.userName = user;
        this.useEntitlements = useEntitlements;
    }

    // ############### Visitor methods for language objects ##################
    
    @Override
    public void visit(GroupSymbol obj) {
    	try {
    		Object modelID = getMetadata().getModelID(obj.getMetadataID());
    		this.validateModelVisibility(modelID, obj);
	    } catch(QueryMetadataException e) {
	        handleException(e, obj);
	    } catch(MetaMatrixComponentException e) {
	        handleException(e, obj);
	    }
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
    	this.validateModelVisibility(obj.getModelID(), obj.getGroup());
    	validateEntitlements(obj);
    }
    
    public void visit(Function obj) {
    	if (FunctionLibrary.LOOKUP.equalsIgnoreCase(obj.getName())) {
    		try {
				ResolverUtil.ResolvedLookup lookup = ResolverUtil.resolveLookup(obj, this.getMetadata());
	    		validateModelVisibility(getMetadata().getModelID(lookup.getGroup().getMetadataID()), lookup.getGroup());
    			List<Symbol> symbols = new LinkedList<Symbol>();
				symbols.add(lookup.getGroup());
				symbols.add(lookup.getKeyElement());
				symbols.add(lookup.getReturnElement());
	    		validateEntitlements(symbols, DataPolicy.PermissionType.READ, Context.QUERY);
			} catch (MetaMatrixComponentException e) {
				handleException(e, obj);
			} catch (MetaMatrixProcessingException e) {
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
        HashSet deleteVars = new HashSet();
        deleteVars.add(obj.getGroup());
        validateEntitlements(deleteVars, DataPolicy.PermissionType.DELETE, Context.DELETE);
    }

    /**
     * Validate query entitlements
     */
    protected void validateEntitlements(Query obj) {
        // If query contains SELECT INTO, validate INTO portion
        Into intoObj = obj.getInto();
        if ( intoObj != null ) {
            GroupSymbol intoGroup = intoObj.getGroup();
            List intoElements = null;
            try {
                intoElements = ResolverUtil.resolveElementsInGroup(intoGroup, getMetadata());
            } catch (QueryMetadataException err) {
                handleException(err, intoGroup);
            } catch (MetaMatrixComponentException err) {
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
        List symbols = new ArrayList(1);
        symbols.add(obj.getGroup());
        validateEntitlements(symbols, DataPolicy.PermissionType.READ, Context.STORED_PROCEDURE);
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
    protected void validateEntitlements(Collection symbols, DataPolicy.PermissionType actionCode, Context auditContext) {
        Map nameToSymbolMap = new HashMap();
        Iterator symbolIter = symbols.iterator();
        while(symbolIter.hasNext()) {
            Object symbol = symbolIter.next();
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
                nameToSymbolMap.put(fullName, symbol);
            } catch(QueryMetadataException e) {
                handleException(e);
            } catch(MetaMatrixComponentException e) {
                handleException(e);
            }
        }

        if (!nameToSymbolMap.isEmpty()) {
            Collection inaccessibleResources = getInaccessibleResources(actionCode, nameToSymbolMap.keySet(), auditContext);
            if(inaccessibleResources.size() > 0) {                              
                List inaccessibleSymbols = new ArrayList(inaccessibleResources.size());
                Iterator nameIter = inaccessibleResources.iterator();
                while(nameIter.hasNext()) {
                    String name = (String) nameIter.next();
                    inaccessibleSymbols.add(nameToSymbolMap.get(name));
                }
                
                // CASE 2362 - do not include the names of the elements for which the user
                // is not authorized in the exception message
                
                handleValidationError(
                    DQPPlugin.Util.getString("ERR.018.005.0095", new Object[]{DQPWorkContext.getWorkContext().getConnectionID(), getActionLabel(actionCode)}), //$NON-NLS-1$                    
                    inaccessibleSymbols);
            }
        }

    }

    protected void validateModelVisibility(Object modelID, GroupSymbol group) {
        if(modelID instanceof TempMetadataID){
        	return;
        }
        try {
		    String modelName = getMetadata().getFullName(modelID);
		    ModelMetaData model = vdb.getModel(modelName);
		    if(!model.isVisible()) {
		        handleValidationError(DQPPlugin.Util.getString("ERR.018.005.0088", getMetadata().getFullName(group.getMetadataID()))); //$NON-NLS-1$
		    }
        } catch (MetaMatrixComponentException e) {
			handleException(e, group);
		}
    }

    
    /**
     * Out of resources specified, return the subset for which the specified not have authorization to access.
     */
    public Set<String> getInaccessibleResources(DataPolicy.PermissionType action, Set<String> resources, Context context) {
    	
        LogManager.logDetail(com.metamatrix.common.log.LogConstants.CTX_AUTHORIZATION, new Object[]{"getInaccessibleResources(", this.userName, ", ", context, ", ", resources, ")"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        
        if (!this.useEntitlements) {
        	return Collections.EMPTY_SET;
        }
        
        // Audit - request
    	AuditMessage msg = new AuditMessage(context.name(), "getInaccessibleResources-request", this.userName, resources.toArray(new String[resources.size()])); //$NON-NLS-1$
    	LogManager.log(MessageLevel.INFO, com.metamatrix.common.log.LogConstants.CTX_AUDITLOGGING, msg);
        
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

        if (results.isEmpty()) {
        	msg = new AuditMessage(context.name(), "getInaccessibleResources-granted all", this.userName, resources.toArray(new String[resources.size()])); //$NON-NLS-1$
        	LogManager.log(MessageLevel.INFO, com.metamatrix.common.log.LogConstants.CTX_AUDITLOGGING, msg);
        } else {
        	msg = new AuditMessage(context.name(), "getInaccessibleResources-denied", this.userName, resources.toArray(new String[resources.size()])); //$NON-NLS-1$
        	LogManager.log(MessageLevel.INFO, com.metamatrix.common.log.LogConstants.CTX_AUDITLOGGING, msg);
        }
        return results;
    }    
}
