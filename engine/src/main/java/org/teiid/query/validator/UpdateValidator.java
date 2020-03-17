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

package org.teiid.query.validator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SupportConstants;
import org.teiid.query.optimizer.relational.PartitionAnalyzer;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.SymbolMap;

/**
 * <p> This visitor is used to validate updates through virtual groups. The command defining
 * the virtual group is always a <code>Query</code>. This object visits various parts of
 * this <code>Query</code> and verifies if the virtual group definition will allows it to be
 * updated.
 */
public class UpdateValidator {

    public enum UpdateType {
        /**
         * The default handling should be used
         */
        INHERENT,
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
        private String updateValidationError;
        private UpdateType deleteType;
        private String deleteValidationError;
        private UpdateType insertType;
        private String insertValidationError;
        private Query view;
        private Map<ElementSymbol, List<Set<Constant>>> partitionInfo;
        private List<UpdateInfo> unionBranches = new LinkedList<UpdateInfo>();

        public Map<ElementSymbol, List<Set<Constant>>> getPartitionInfo() {
            return partitionInfo;
        }

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
            if (updateCols.isEmpty() && this.updatableGroups.size() > 1) {
                return null;
            }
            for (UpdateMapping entry : this.updatableGroups.values()) {
                if (((insert && entry.insertAllowed) || (!insert && entry.updateAllowed)) && entry.updatableViewSymbols.keySet().containsAll(updateCols)) {
                    return entry;
                }
            }
            return null;
        }

        public UpdateMapping findInsertUpdateMapping(Insert insert, boolean rewrite) throws QueryValidatorException {
            if (getUnionBranches().isEmpty()) {
                return findUpdateMapping(insert.getVariables(), true);
            }
            if (insert.getQueryExpression() != null) {
                //TODO: this could be done in a loop, see about adding a validation
                 throw new QueryValidatorException(QueryPlugin.Event.TEIID30239, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30239, insert.getGroup()));
            }
            int partition = -1;
            List<ElementSymbol> filteredColumns = new LinkedList<ElementSymbol>();
            for (Map.Entry<ElementSymbol, List<Set<Constant>>> entry : partitionInfo.entrySet()) {
                int index = insert.getVariables().indexOf(entry.getKey());
                if (index == -1) {
                    continue;
                }
                Expression value = (Expression)insert.getValues().get(index);
                if (!(value instanceof Constant)) {
                    continue;
                }
                for (int i = 0; i < entry.getValue().size(); i++) {
                    if (entry.getValue().get(i).contains(value)) {
                        if (entry.getValue().get(i).size() == 1) {
                            filteredColumns.add(entry.getKey());
                        }
                        if (partition == -1) {
                            partition = i;
                        } else if (partition != i) {
                            throw new QueryValidatorException(QueryPlugin.Event.TEIID30240, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30240, insert.getGroup(), insert.getVariables()));
                        }
                    }
                }
            }
            if (partition == -1) {
                 throw new QueryValidatorException(QueryPlugin.Event.TEIID30241, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30241, insert.getGroup(), insert.getVariables()));
            }
            UpdateInfo info = this;
            if (partition > 0) {
                info = info.getUnionBranches().get(partition - 1);
            }
            List<ElementSymbol> variables = new ArrayList<ElementSymbol>(insert.getVariables());
            variables.removeAll(filteredColumns);
            UpdateMapping mapping = info.findUpdateMapping(variables, true);
            if (rewrite && mapping != null && !filteredColumns.isEmpty()) {
                for (ElementSymbol elementSymbol : filteredColumns) {
                    if (mapping.getUpdatableViewSymbols().containsKey(elementSymbol)) {
                        continue;
                    }
                    int index = insert.getVariables().indexOf(elementSymbol);
                    insert.getVariables().remove(index);
                    if (rewrite) {
                        insert.getValues().remove(index);
                    }
                }
            }
            return mapping;
        }

        public Query getViewDefinition() {
            return view;
        }

        public String getDeleteValidationError() {
            return deleteValidationError;
        }

        public String getInsertValidationError() {
            return insertValidationError;
        }

        public String getUpdateValidationError() {
            return updateValidationError;
        }

        public List<UpdateInfo> getUnionBranches() {
            return unionBranches;
        }

        private void setUpdateValidationError(String updateValidationError) {
            if (this.updateValidationError == null) {
                this.updateValidationError = updateValidationError;
            }
        }

        private void setInsertValidationError(String insertValidationError) {
            if (this.insertValidationError == null) {
                this.insertValidationError = insertValidationError;
            }
        }

        private void setDeleteValidationError(String deleteValidationError) {
            if (this.deleteValidationError == null) {
                this.deleteValidationError = deleteValidationError;
            }
        }

    }

    private QueryMetadataInterface metadata;
    private UpdateInfo updateInfo = new UpdateInfo();

    private ValidatorReport report = new ValidatorReport();
    private ValidatorReport insertReport = new ValidatorReport();
    private ValidatorReport updateReport = new ValidatorReport();
    private ValidatorReport deleteReport = new ValidatorReport();

    public UpdateValidator(QueryMetadataInterface qmi, UpdateType insertType, UpdateType updateType, UpdateType deleteType) {
        this.metadata = qmi;
        this.updateInfo.deleteType = deleteType;
        this.updateInfo.insertType = insertType;
        this.updateInfo.updateType = updateType;
    }

    public UpdateInfo getUpdateInfo() {
        return updateInfo;
    }

    public ValidatorReport getReport() {
        return report;
    }

    public ValidatorReport getDeleteReport() {
        return deleteReport;
    }

    public ValidatorReport getInsertReport() {
        return insertReport;
    }

    public ValidatorReport getUpdateReport() {
        return updateReport;
    }

    private void handleValidationError(String error, boolean update, boolean insert, boolean delete) {
        if (update && insert && delete) {
            report.handleValidationError(error);
            updateInfo.setUpdateValidationError(error);
            updateInfo.setInsertValidationError(error);
            updateInfo.setDeleteValidationError(error);
        } else {
            if (update) {
                updateReport.handleValidationError(error);
                updateInfo.setUpdateValidationError(error);
            }
            if (insert) {
                insertReport.handleValidationError(error);
                updateInfo.setInsertValidationError(error);
            }
            if (delete) {
                deleteReport.handleValidationError(error);
                updateInfo.setDeleteValidationError(error);
            }
        }
    }

    public void validate(Command command, List<ElementSymbol> viewSymbols) throws QueryMetadataException, TeiidComponentException {
        if (this.updateInfo.deleteType != UpdateType.INHERENT && this.updateInfo.updateType != UpdateType.INHERENT && this.updateInfo.insertType != UpdateType.INHERENT) {
            return;
        }
        if (command instanceof SetQuery) {
            SetQuery setQuery = (SetQuery)command;
            if (setQuery.getLimit() != null) {
                handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0013"), true, true, true); //$NON-NLS-1$
                return;
            }
            LinkedList<Query> queries = new LinkedList<Query>();
            if (!PartitionAnalyzer.extractQueries((SetQuery)command, queries)) {
                handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0001"), true, true, true); //$NON-NLS-1$
                return;
            }
            Map<ElementSymbol, List<Set<Constant>>> partitions = PartitionAnalyzer.extractPartionInfo((SetQuery)command, viewSymbols);
            this.updateInfo.partitionInfo = partitions;
            if (partitions.isEmpty()) {
                handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0018"), false, true, false); //$NON-NLS-1$
            }
            boolean first = true;
            for (Query query : queries) {
                UpdateInfo ui = this.updateInfo;
                if (!first) {
                    this.updateInfo = new UpdateInfo();
                    this.updateInfo.deleteType = ui.deleteType;
                    this.updateInfo.insertType = ui.insertType;
                    this.updateInfo.updateType = ui.updateType;
                }
                internalValidate(query, viewSymbols);
                //accumulate the errors on the first branch - will be checked at resolve time
                if (this.updateInfo.getDeleteValidationError() != null) {
                    ui.setDeleteValidationError(this.updateInfo.getDeleteValidationError());
                }
                if (this.updateInfo.getUpdateValidationError() != null) {
                    ui.setUpdateValidationError(this.updateInfo.getUpdateValidationError());
                }
                if (this.updateInfo.getInsertValidationError() != null) {
                    ui.setInsertValidationError(this.updateInfo.getInsertValidationError());
                }
                if (!first) {
                    ui.unionBranches.add(this.updateInfo);
                    this.updateInfo = ui;
                } else {
                    first = false;
                }
            }
            return;
        }
        internalValidate(command, viewSymbols);
        if (this.updateInfo.deleteType != UpdateType.INHERENT) {
            this.deleteReport.getItems().clear();
            this.updateInfo.deleteValidationError = null;
        }
        if (this.updateInfo.updateType != UpdateType.INHERENT) {
            this.updateReport.getItems().clear();
            this.updateInfo.updateValidationError = null;
        }
        if (this.updateInfo.insertType != UpdateType.INHERENT) {
            this.insertReport.getItems().clear();
            this.updateInfo.insertValidationError = null;
        }
    }

    private void internalValidate(Command command, List<ElementSymbol> viewSymbols) throws QueryMetadataException, TeiidComponentException {
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
            String warning = QueryPlugin.Util.getString("ERR.015.012.0002"); //$NON-NLS-1$
            updateReport.handleValidationWarning(warning);
            deleteReport.handleValidationWarning(warning);
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

        List<Expression> projectedSymbols = query.getSelect().getProjectedSymbols();

        for (int i = 0; i < projectedSymbols.size(); i++) {
            Expression symbol = projectedSymbols.get(i);
            Expression ex = SymbolMap.getExpression(symbol);

            if (!metadata.elementSupports(viewSymbols.get(i).getMetadataID(), SupportConstants.Element.UPDATE)) {
                continue;
            }
            if (ex instanceof ElementSymbol) {
                ElementSymbol es = (ElementSymbol)ex;
                String groupName = es.getGroupSymbol().getName();
                UpdateMapping info = updateInfo.updatableGroups.get(groupName);
                if (es.getGroupSymbol().getDefinition() != null) {
                    ElementSymbol clone = es.clone();
                    clone.setOutputName(null);
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
            String warning = QueryPlugin.Util.getString("ERR.015.012.0009", query.getFrom()); //$NON-NLS-1$
            updateReport.handleValidationWarning(warning);
            deleteReport.handleValidationWarning(warning);
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
        } else {
            for (GroupSymbol groupSymbol : allGroups) {
                UpdateMapping info = updateInfo.updatableGroups.get(groupSymbol.getName());
                if (info == null) {
                    continue; // not projected
                }
                String warning = QueryPlugin.Util.getString("ERR.015.012.0004", info.correlatedName); //$NON-NLS-1$
                report.handleValidationWarning(warning);
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
            handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0005"), true, false, false); //$NON-NLS-1$
        }
        if (this.updateInfo.deleteType == UpdateType.INHERENT) {
            if (this.updateInfo.deleteTarget == null) {
                if (this.updateInfo.isSimple && updatable) {
                    this.updateInfo.deleteTarget = this.updateInfo.updatableGroups.values().iterator().next();
                } else {
                    handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0014"), false, false, true); //$NON-NLS-1$
                }
            }
            if (this.updateInfo.deleteTarget != null) {
                GroupSymbol group = this.updateInfo.deleteTarget.group;
                if (!this.updateInfo.isSimple && metadata.getPrimaryKey(group.getMetadataID()) == null &&
                        metadata.getUniqueKeysInGroup(group.getMetadataID()).isEmpty()) {
                    handleValidationError(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31267, viewSymbols.iterator().next().getGroupSymbol(), group), false, false, true);
                }
            }
        }
    }

    private void setUpdateFlags(GroupSymbol groupSymbol) throws QueryMetadataException, TeiidComponentException {
        UpdateMapping info = updateInfo.updatableGroups.get(groupSymbol.getName());

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
     * FROM clause of the query but not specified in its SELECT clause
     * @param element The <code>ElementSymbol</code> being validated
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     */
    private boolean validateInsertElement(ElementSymbol element) throws QueryMetadataException, TeiidComponentException {
        // checking if the elements not specified in the query are required.
        if(metadata.elementSupports(element.getMetadataID(), SupportConstants.Element.NULL)
            || metadata.elementSupports(element.getMetadataID(), SupportConstants.Element.DEFAULT_VALUE)
            || metadata.elementSupports(element.getMetadataID(), SupportConstants.Element.AUTO_INCREMENT)
            || !metadata.elementSupports(element.getMetadataID(), SupportConstants.Element.UPDATE)) {
            return true;
        }
        if (this.updateInfo.insertType == UpdateType.INHERENT) {
            insertReport.handleValidationWarning(QueryPlugin.Util.getString("ERR.015.012.0010", element, element.getGroupSymbol())); //$NON-NLS-1$
        }
        return false;
    }
}
