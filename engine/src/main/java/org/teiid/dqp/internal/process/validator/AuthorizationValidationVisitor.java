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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.dqp.internal.process.multisource.MultiSourceElement;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.service.AuthorizationService;
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

    /**
     * This virtual procedure must be accessible to all users in order to read the WSDL
     * for web services.
     */
    public final static String GET_UPDATED_CHARACTER_VDB_RESOURCE = "System.getUpdatedCharacterVDBResource"; //$NON-NLS-1$

    /**
     * Set of procedures which must be available to all users.     
     */
    private static Set globallyAccessibleProcedures = new HashSet();
    
    private String connectionID;
    private AuthorizationService authInterface;
    
    
    static {
        globallyAccessibleProcedures.add(GET_UPDATED_CHARACTER_VDB_RESOURCE);
    }
    
    
    public AuthorizationValidationVisitor(
        String connectionID,
        AuthorizationService authInterface) {
        super();

        this.connectionID = connectionID;
        this.authInterface = authInterface;
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
    			List<Symbol> symbols = new LinkedList<Symbol>();
				ResolverUtil.ResolvedLookup lookup = ResolverUtil.resolveLookup(obj, this.getMetadata());
				symbols.add(lookup.getGroup());
				symbols.add(lookup.getKeyElement());
				symbols.add(lookup.getReturnElement());
				validateEntitlements(symbols, AuthorizationService.ACTION_READ, AuthorizationService.CONTEXT_QUERY);
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
            AuthorizationService.ACTION_CREATE,
            AuthorizationService.CONTEXT_INSERT);
    }

    /**
     * Validate update entitlements
     */
    protected void validateEntitlements(Update obj) {
        // Check that all elements used in criteria have read permission
        if (obj.getCriteria() != null) {
            validateEntitlements(
                ElementCollectorVisitor.getElements(obj.getCriteria(), true),
                AuthorizationService.ACTION_READ,
                AuthorizationService.CONTEXT_UPDATE);
        }

        // The variables from the changes must be checked for UPDATE entitlement
        // validateEntitlements on all the variables used in the update.
        validateEntitlements(obj.getChangeList().getClauseMap().keySet(), AuthorizationService.ACTION_UPDATE, AuthorizationService.CONTEXT_UPDATE);
    }

    /**
     * Validate delete entitlements
     */
    protected void validateEntitlements(Delete obj) {
        // Check that all elements used in criteria have read permission
        if (obj.getCriteria() != null) {
            validateEntitlements(
                ElementCollectorVisitor.getElements(obj.getCriteria(), true),
                AuthorizationService.ACTION_READ,
                AuthorizationService.CONTEXT_DELETE);
        }

        // Check that all elements of group being deleted have delete permission
        HashSet deleteVars = new HashSet();
        deleteVars.add(obj.getGroup());
        validateEntitlements(deleteVars, AuthorizationService.ACTION_DELETE, AuthorizationService.CONTEXT_DELETE);
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
                                 AuthorizationService.ACTION_CREATE,
                                 AuthorizationService.CONTEXT_INSERT);
        }

        // Validate this query's entitlements
        Collection entitledObjects = GroupCollectorVisitor.getGroups(obj, true);
        if (!isXMLCommand(obj)) {
            entitledObjects.addAll(ElementCollectorVisitor.getElements(obj, true));
        }

        if(entitledObjects.size() == 0) {
            return;
        }
        
        validateEntitlements(entitledObjects, AuthorizationService.ACTION_READ, AuthorizationService.CONTEXT_QUERY);
    }

    
    /**
     * Add a procedure to the list of globally 
     * @param name
     * @since 4.2
     */
    public static void addGloballyAccessibleProcedure(String name) {
        globallyAccessibleProcedures.add(name);  
    }
    
    public static void removeGloballyAccessibleProcedure(String name) {
        globallyAccessibleProcedures.remove(name);   
    }
    
    
    /**
     * Validate query entitlements
     */
    protected void validateEntitlements(StoredProcedure obj) {
        //Skip entitlement checking for any globally available procedures.
        if (globallyAccessibleProcedures.contains(obj.getProcedureName())) {
            return;
        }
        
        
        List symbols = new ArrayList(1);
        symbols.add(obj.getGroup());
        validateEntitlements(symbols, AuthorizationService.ACTION_READ, AuthorizationService.CONTEXT_PROCEDURE);
    }

    private String getActionLabel(int actionCode) {
        switch(actionCode) {
            case AuthorizationService.ACTION_READ:    return "Read"; //$NON-NLS-1$
            case AuthorizationService.ACTION_CREATE:    return "Create"; //$NON-NLS-1$
            case AuthorizationService.ACTION_UPDATE:    return "Update"; //$NON-NLS-1$
            case AuthorizationService.ACTION_DELETE:    return "Delete"; //$NON-NLS-1$
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
    protected void validateEntitlements(Collection symbols, int actionCode, int auditContext) {
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
            try {
                Collection inaccessibleResources = this.authInterface.getInaccessibleResources(connectionID, actionCode, nameToSymbolMap.keySet(), auditContext);
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
                        DQPPlugin.Util.getString("ERR.018.005.0095", new Object[]{this.connectionID, getActionLabel(actionCode)}), //$NON-NLS-1$                    
                        inaccessibleSymbols);
                }
            } catch(MetaMatrixComponentException e) {
                handleException(e);
            }
        }

    }

}
