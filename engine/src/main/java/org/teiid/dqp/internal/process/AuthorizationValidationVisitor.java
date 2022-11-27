/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
        //eventually may need to check scheam level permissions proactively
        validateTemp(PermissionType.CREATE, obj.getTable().getNonCorrelationName(), obj.getTable(), Context.CREATE);
    }

    @Override
    public void visit(DynamicCommand obj) {
        if (obj.getIntoGroup() != null && obj.getIntoGroup().isTempTable() && obj.getIntoGroup().isImplicitTempGroupSymbol()) {
            validateTemp(PermissionType.CREATE, obj.getIntoGroup().getNonCorrelationName(), obj.getIntoGroup(), Context.CREATE);
        } //else if not a temp table, then this will be checked by the dynamic sql instruction
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

        String[] resources = new String[] {language};

        logRequest(resources, Context.QUERY);

        boolean result = decider.isLanguageAllowed(language, commandContext);

        logResult(resources, Context.QUERY, result);

        if (!result) {
            handleValidationError(
                    QueryPlugin.Util.getString("ERR.018.005.0095", commandContext.getUserName(), "LANGUAGE"), //$NON-NLS-1$  //$NON-NLS-2$
                    Arrays.asList(objectTable));
        }
    }

    private void validateTemp(DataPolicy.PermissionType action, String resource, LanguageObject object, Context context) {
        String[] resources = new String[] {resource};
        logRequest(resources, context);

        boolean allowed = decider.isTempAccessible(action, null, context, commandContext);

        logResult(resources, context, allowed);
        if (!allowed) {
            handleValidationError(
                    QueryPlugin.Util.getString("ERR.018.005.0095", commandContext.getUserName(), "CREATE_TEMPORARY_TABLES"), //$NON-NLS-1$  //$NON-NLS-2$
                    Arrays.asList(object));
        }
    }

    private void logRequest(Set<AbstractMetadataRecord> resources, Context context) {
        logRequest(resources.stream().map(r -> r.getFullName()).toArray(String[]::new), context);
    }

    private void logRequest(String[] resources, Context context) {
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_AUDITLOGGING, MessageLevel.DETAIL)) {
            // Audit - request
            AuditMessage msg = new AuditMessage(context.name(),
                    "getInaccessibleResources-request", resources, commandContext); //$NON-NLS-1$
            LogManager.logDetail(LogConstants.CTX_AUDITLOGGING, msg);
        }
    }

    @Override
    public void visit(Drop obj) {
        validateTemp(PermissionType.DROP, obj.getTable().getNonCorrelationName(), obj.getTable(), Context.DROP);
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
                Map<AbstractMetadataRecord, Function> map = new HashMap<AbstractMetadataRecord, Function>();
                map.put(obj.getFunctionDescriptor().getMethod(), obj);
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
        entitledObjects.addAll(ElementCollectorVisitor.getElements(obj, true));

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
     * @param auditContext The {@link Context} to use when resource auditing is done.
     */
    protected void validateEntitlements(Collection<? extends LanguageObject> symbols, DataPolicy.PermissionType actionCode, Context auditContext) {
        Map<AbstractMetadataRecord, LanguageObject> recordToSymbolMap = new LinkedHashMap<AbstractMetadataRecord, LanguageObject>();
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
                            Map<AbstractMetadataRecord, LanguageObject> procMap = new LinkedHashMap<AbstractMetadataRecord, LanguageObject>();
                            addToMap(((TempMetadataID)metadataID).getOriginalMetadataID(), symbol, procMap, getMetadata());
                            validateEntitlements(PermissionType.EXECUTE, auditContext, procMap);
                        } else if (group.isTempTable() && group.isImplicitTempGroupSymbol()) {
                            validateTemp(actionCode, group.getNonCorrelationName(), group, auditContext);
                        }
                        continue;
                    }
                }
                addToMap(metadataID, symbol, recordToSymbolMap, getMetadata());
            } catch(QueryMetadataException e) {
                handleException(e);
            } catch(TeiidComponentException e) {
                handleException(e);
            }
        }

        validateEntitlements(actionCode, auditContext, recordToSymbolMap);
    }

    static void addToMap(Object metadataID, LanguageObject symbol, Map<AbstractMetadataRecord, LanguageObject> recordToSymbolMap, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
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
                    //use the interface to get the non-temp version of the table
                    metadataID = metadata.getGroupIDForElementID(metadataID);
                }
            }
            if (metadataID instanceof AbstractMetadataRecord) {
                recordToSymbolMap.put((AbstractMetadataRecord)metadataID, symbol);
            }
        }
    }

    static private boolean isSystemSchema(String modelName) {
        return CoreConstants.SYSTEM_MODEL.equalsIgnoreCase(modelName)
        || CoreConstants.ODBC_MODEL.equalsIgnoreCase(modelName);
    }

    private void validateEntitlements(DataPolicy.PermissionType actionCode,
            Context auditContext, Map<AbstractMetadataRecord, ? extends LanguageObject> recordToSymbolMap) {
        if (recordToSymbolMap.isEmpty()) {
            return;
        }
        Collection<AbstractMetadataRecord> inaccessibleResources = getInaccessibleResources(actionCode, recordToSymbolMap, auditContext);
        if(inaccessibleResources.isEmpty()) {
            return;
        }
        List<LanguageObject> inaccessibleSymbols = new ArrayList<LanguageObject>(inaccessibleResources.size());
        for (AbstractMetadataRecord record : inaccessibleResources) {
            inaccessibleSymbols.add(recordToSymbolMap.get(record));
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
    public Set<AbstractMetadataRecord> getInaccessibleResources(DataPolicy.PermissionType action, Map<AbstractMetadataRecord, ? extends LanguageObject> resources, Context context) {
        logRequest(resources.keySet(), context);

        Set<AbstractMetadataRecord> results = decider.getInaccessibleResources(action, resources.keySet(), context, commandContext);

        logResult(resources.keySet(), context, results.isEmpty());
        return results;
    }

    private void logResult(Set<AbstractMetadataRecord> resources, Context context, boolean granted) {
        logResult(resources.stream().map(r -> r.getFullName()).toArray(String[]::new), context, granted);
    }

    private void logResult(String[] resources, Context context,
            boolean granted) {
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_AUDITLOGGING, MessageLevel.DETAIL)) {
            if (granted) {
                AuditMessage msg = new AuditMessage(context.name(), "getInaccessibleResources-granted all", resources, commandContext); //$NON-NLS-1$
                LogManager.logDetail(LogConstants.CTX_AUDITLOGGING, msg);
            } else {
                AuditMessage msg = new AuditMessage(context.name(), "getInaccessibleResources-denied", resources, commandContext); //$NON-NLS-1$
                LogManager.logDetail(LogConstants.CTX_AUDITLOGGING, msg);
            }
        }
    }
}
