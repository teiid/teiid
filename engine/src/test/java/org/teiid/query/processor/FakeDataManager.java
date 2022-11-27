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

package org.teiid.query.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.events.EventDistributor;
import org.teiid.logging.LogManager;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.ReferenceCollectorVisitor;
import org.teiid.query.util.CommandContext;


public class FakeDataManager implements ProcessorDataManager {

    static class TupleInfo {
        List<ElementSymbol> elements;
        List<?>[] data;

        public TupleInfo(List<ElementSymbol> elements, List<?>[] data) {
            this.elements = elements;
            this.data = data;
        }
    }

    private Map<String, TupleInfo> tuples = new HashMap<String, TupleInfo>();
    private Map<String, List<List<?>>[]> procTuples = new HashMap<String, List<List<?>>[]>();

    private static final String LOG_CONTEXT = "FAKE_DATA_MANAGER"; //$NON-NLS-1$

    //used to test blocked exception. If true,
    //the first time nextTuple is called on FakeTupleSource,
    //it will throws BlockedExceptiom
    private boolean blockOnce;

    // ---- Cached code table stuff ----

    // upper table name + upper key col name + upper ret col name -> map of values
    private Map<String, Map> codeTableValues = new HashMap<String, Map>();

    // throw Blocked on first request
    private boolean throwBlocked = false;

    // upper table name + upper key col name + upper ret col name -> flag of whether this table has blocked yet
    private Map<String, Boolean> blockedState = new HashMap<String, Boolean>();

    // Track history to verify it later
    private List<String> queries = new ArrayList<String>();
    private boolean recordingCommands = true;

    /**
     * Return string form of all queries run against this FDM
     * @return List<String> recorded commands
     */
    public List<String> getQueries() {
        return this.queries;
    }

    /**
     * Clears the list of recorded commands and returns a copy
     * @return a copy of the recorded commands prior to clearing the list
     */
    public List<String> clearQueries() {
        List<String> rc = new ArrayList<String>(this.getQueries());
        this.queries.clear();
        return rc;
    }

    public void registerProcTuples(String proc, List[] data) {
        procTuples.put(proc, data);
    }

    public TupleSource registerRequest(CommandContext context, Command command, String modelName, RegisterRequestParameter parameterObject)
        throws TeiidComponentException {

        LogManager.logTrace(LOG_CONTEXT, new Object[]{"Register Request:", command, ",processorID:", context.getRequestId(), ",model name:", modelName,",TupleSourceID nodeID:",new Integer(parameterObject.nodeID)}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$

        if (this.recordingCommands) {
            if (! (command instanceof BatchedUpdateCommand) ) {
                this.queries.add(command.toString());
            }
        }

        if (ReferenceCollectorVisitor.getReferences(command).size() > 0) {
            throw new IllegalArgumentException("Found references in the command registered with the DataManager."); //$NON-NLS-1$
        }
        // Get group ID from atomic command
        GroupSymbol group = null;
        if(command instanceof Query){
            group = getQueryGroup((Query)command);
        }else if(command instanceof SetQuery) {
            SetQuery union = (SetQuery) command;
            group = getQueryGroup(union.getProjectedQuery());
        } else if (command instanceof StoredProcedure) {
            Object id = ((StoredProcedure) command).getProcedureID();
            List<List<?>>[] data = procTuples.get(id);
            if (data == null) {
                throw new AssertionError("Undefined results for " + command); //$NON-NLS-1$
            }
            FakeTupleSource ts= new FakeTupleSource(command.getProjectedSymbols(), data);
            if(this.blockOnce){
                ts.setBlockOnce();
            }
            return ts;
        } else if (command instanceof ProcedureContainer) {
            group = ((ProcedureContainer) command).getGroup();
        } else if ( command instanceof BatchedUpdateCommand ) {
            BatchedUpdateCommand buc = (BatchedUpdateCommand)command;
            if ( buc.getUpdateCommands().get(0) instanceof Update ) {
                group = ((Update)buc.getUpdateCommands().get(0)).getGroup();
            }
            if (this.recordingCommands) {
                for ( Iterator<Command> it = ((BatchedUpdateCommand) command).getUpdateCommands().iterator(); it.hasNext(); ) {
                    this.queries.add(it.next().toString());
                }
            }
        }

        TupleInfo tupleInfo = tuples.get(group.getNonCorrelationName().toUpperCase());
        List<? extends Expression> elements = tupleInfo.elements;
        List<?>[] data = tupleInfo.data;

        List<Expression> projectedSymbols = command.getProjectedSymbols();
        int[] columnMap = getColumnMap(tupleInfo.elements, projectedSymbols);

        /*
        *  updateCommands is used to hold a list of commands that
        *  either came from a BatchedUpdateCommand or a signle
        *  command from an Update command.
        */
        List<Command> updateCommands = new ArrayList<Command>();

        // Apply query criteria to tuples
        if(command instanceof Query){
            Query query = (Query)command;
            if(query.getCriteria() != null) {
                // Build lookupMap from BOTH all the elements and the projected symbols - both may be needed here
                Map lookupMap = new HashMap();
                for(int i=0; i<elements.size(); i++) {
                    Expression element = elements.get(i);
                    mapElementToIndex(lookupMap, element, i, group);
                }
                for(int i=0; i<projectedSymbols.size(); i++) {
                    Expression element = projectedSymbols.get(i);
                    mapElementToIndex(lookupMap, element, columnMap[i], group);
                }

                List filteredTuples = new ArrayList();
                for(int i=0; i<data.length; i++) {
                    try {
                        if(new Evaluator(lookupMap, null, null).evaluate(query.getCriteria(), data[i])) {
                            filteredTuples.add(data[i]);
                        }
                    } catch(ExpressionEvaluationException e) {
                        throw new TeiidComponentException(e, e.getMessage());
                    }
                }

                data = new List[filteredTuples.size()];
                filteredTuples.toArray(data);
            }
        } else if ( command instanceof Insert || command instanceof Update || command instanceof Delete) {
            // add single update command to a list to be executed
            updateCommands.add(command);
        } else if ( command instanceof BatchedUpdateCommand ) {
            // add all update commands to a list to be executed
            updateCommands.addAll(((BatchedUpdateCommand) command).getUpdateCommands());
        }

        // if we had update commands added to the list, execute them now
        if ( updateCommands.size() > 0 ) {
            List<List<Integer>> filteredTuples = new ArrayList<List<Integer>>();
            for ( int c = 0; c < updateCommands.size(); c++ ) {
                Command cmd = updateCommands.get(c);
                if (cmd instanceof FilteredCommand) {
                    FilteredCommand update = (FilteredCommand)cmd;
                    if ( update.getCriteria() != null ) {
                        // Build lookupMap from BOTH all the elements and the projected symbols - both may be needed here
                        Map<Object, Integer> lookupMap = new HashMap<Object, Integer>();
                        for(int i=0; i<elements.size(); i++) {
                            Expression element = elements.get(i);
                            mapElementToIndex(lookupMap, element, new Integer(i), group);
                        }
                        for(int i=0; i<projectedSymbols.size(); i++) {
                            Expression element = projectedSymbols.get(i);
                            mapElementToIndex(lookupMap, element, new Integer(columnMap[i]), group);
                        }

                        int updated = 0;
                        for(int i=0; i<data.length; i++) {
                            try {
                                if(new Evaluator(lookupMap, null, null).evaluate(update.getCriteria(), data[i])) {
                                    updated++;
                                }
                            } catch(ExpressionEvaluationException e) {
                                throw new TeiidComponentException(e, e.getMessage());
                            }
                        }
                        List<Integer> updateTuple = new ArrayList<Integer>(1);
                        updateTuple.add( new Integer(updated) );
                        filteredTuples.add(updateTuple);
                    }
                } else {
                    filteredTuples.add(Arrays.asList(1)); //TODO: check for bulk
                }
            }
            data = new List[filteredTuples.size()];
            filteredTuples.toArray(data);
            elements = Command.getUpdateCommandSymbol();
            columnMap[0] = 0;
        }

        FakeTupleSource ts= new FakeTupleSource(elements, data, projectedSymbols, columnMap);
        if(this.blockOnce){
            ts.setBlockOnce();
        }
        return ts;
    }

    private GroupSymbol getQueryGroup(Query query) throws TeiidComponentException {
        GroupSymbol group;
        From from = query.getFrom();
        List groups = from.getGroups();
        if(groups.size() != 1) {
            throw new TeiidComponentException("Cannot build fake tuple source for command: " + query);     //$NON-NLS-1$
        }
        group = (GroupSymbol) groups.get(0);
        Iterator projSymbols = query.getSelect().getProjectedSymbols().iterator();
        while (projSymbols.hasNext()) {
            Object symbol = projSymbols.next();
            if (symbol instanceof ElementSymbol){
                ElementSymbol elementSymbol = (ElementSymbol)symbol;
                GroupSymbol g = elementSymbol.getGroupSymbol();
                if (!g.equals(group)){
                    throw new TeiidComponentException("Illegal symbol " + elementSymbol + " in SELECT of command: " + query);    //$NON-NLS-1$ //$NON-NLS-2$
                }
                if (elementSymbol.getMetadataID() == null){
                    throw new TeiidComponentException("Illegal null metadata ID in ElementSymbol " + elementSymbol + " in SELECT of command: " + query);    //$NON-NLS-1$ //$NON-NLS-2$
                } else if (elementSymbol.getMetadataID() instanceof TempMetadataID){
                    throw new TeiidComponentException("Illegal TempMetadataID in ElementSymbol " + elementSymbol + " in SELECT of command: " + query);    //$NON-NLS-1$ //$NON-NLS-2$
                }
            }
        }
        return group;
    }

    /**
     * @param lookupMap
     * @param element
     * @param group
     */
    private void mapElementToIndex(Map lookupMap, Expression element, Integer index, GroupSymbol group) {
        ElementSymbol elementSymbol = (ElementSymbol)SymbolMap.getExpression(element);
        if (group.getDefinition() != null){
            String groupAlias = group.getName();
            elementSymbol = elementSymbol.clone();
            elementSymbol.getGroupSymbol().setName(groupAlias);
        }
        lookupMap.put(elementSymbol, index);
    }

    //   columnMap[expectedElementIndex] = allElementIndex
    private int[] getColumnMap(List<ElementSymbol> allElements, List<? extends Expression> expectedElements) {
        int[] map = new int[expectedElements.size()];

        for(int i=0; i<expectedElements.size(); i++) {
            Expression symbol = SymbolMap.getExpression(expectedElements.get(i));

            boolean foundMatch = false;

            if (symbol instanceof ElementSymbol) {
                // Find matching short name in all elements
                for(int j=0; j<allElements.size(); j++) {
                    ElementSymbol es = allElements.get(j);
                    if(es.getShortName().equalsIgnoreCase(((ElementSymbol)symbol).getShortName())) {
                        map[i] = j;
                        foundMatch = true;
                        break;
                    }
                }
            }

            if(! foundMatch) {
                map[i] = -1;
            }
        }

        return map;
    }


    public void setThrowBlocked(boolean throwBlocked) {
        this.throwBlocked = throwBlocked;
    }

    public void defineCodeTable(String tableName, String keyCol, String retCol, Map values) {
        String key = tableName.toUpperCase() + keyCol.toUpperCase() + retCol.toUpperCase();
        this.codeTableValues.put(key, values);
        this.blockedState.put(key, Boolean.FALSE);
    }

    public Object lookupCodeValue(
        CommandContext context,
        String codeTableName,
        String returnElementName,
        String keyElementName,
        Object keyValue)
        throws BlockedException, TeiidComponentException {

            String tableKey = codeTableName.toUpperCase() + keyElementName.toUpperCase() + returnElementName.toUpperCase();
            if(! codeTableValues.containsKey(tableKey)) {
                throw new TeiidComponentException("Unknown code table: " + codeTableName); //$NON-NLS-1$
            }

            if(throwBlocked) {
                if(blockedState.get(tableKey).equals(Boolean.FALSE)) {
                    blockedState.put(tableKey, Boolean.TRUE);
                    throw BlockedException.INSTANCE;
                }
            }

            Map values = codeTableValues.get(tableKey);
            return values.get(keyValue);
    }

    public void setBlockOnce() {
        blockOnce = true;
    }

    /**
     * Are commands/queries that are registered with the data manager being
     * recorded?
     * <p>
     * Recorded commands can be retrieved by {@link #getQueries()}
     *
     * @return whether or not commands should be recorded
     */
    public boolean isRecordingCommands() {
        return recordingCommands;
    }

    /**
     * Indicate whether or not commands/queries registered with the data
     * manager are to be recorded in {@link #queries}.
     * <p>
     * Recorded commands can be retrieved by {@link #getQueries()}
     *
     * @param shouldRecord should commands be recorded?
     */
    public void setRecordingCommands(boolean shouldRecord) {
        this.recordingCommands = shouldRecord;
    }

    public void registerTuples(QueryMetadataInterface metadata, String groupName, List<?>[] data) throws QueryResolverException, TeiidComponentException {
        GroupSymbol group = new GroupSymbol(groupName);
        ResolverUtil.resolveGroup(group, metadata);
        List<ElementSymbol> elementSymbols = ResolverUtil.resolveElementsInGroup(group, metadata);
        tuples.put(group.getName().toUpperCase(), new TupleInfo(elementSymbols, data));
    }

    @Override
    public EventDistributor getEventDistributor() {
        // TODO Auto-generated method stub
        return null;
    }
}