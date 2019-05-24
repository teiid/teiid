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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.dqp.internal.datamgr.ConnectorWorkItem;
import org.teiid.dqp.internal.datamgr.LanguageBridgeFactory;
import org.teiid.events.EventDistributor;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.util.CommandContext;



/**
 * @since 4.2
 */
public class HardcodedDataManager implements
                                 ProcessorDataManager {

    // sql string to data
    private Map<String, List<?>[]> data = new HashMap<String, List<?>[]>();

    // valid models - if null, any is assumed valid
    private Set<String> validModels;

    private boolean mustRegisterCommands = true;

    private boolean fullBatchedUpdate = false;

    private boolean blockOnce;

    // Collect all commands run against this class
    protected List<Command> commandHistory = new ArrayList<Command>(); // Commands
    private List<org.teiid.language.Command> pushdownCommands = new ArrayList<org.teiid.language.Command>(); // Commands

    private LanguageBridgeFactory lbf;

    public HardcodedDataManager() {
        this(true);
    }

    public HardcodedDataManager(QueryMetadataInterface metadata) {
        this(true);
        this.lbf = new LanguageBridgeFactory(metadata);
    }

    public HardcodedDataManager(QueryMetadataInterface metadata, CommandContext context, SourceCapabilities capabilities) {
        this(true);
        this.lbf = new LanguageBridgeFactory(metadata);
        ConnectorWorkItem.initLanguageBridgeFactory(lbf, context, capabilities);
    }

    public HardcodedDataManager(boolean mustRegisterCommands) {
        this.mustRegisterCommands = mustRegisterCommands;
    }

    public LanguageBridgeFactory getLanguageBridgeFactory() {
        return lbf;
    }

    public void addData(String sql, List<?>... rows) {
        data.put(sql, rows);
    }

    public void clearData() {
        this.data.clear();
        this.commandHistory.clear();
    }

    public void setBlockOnce(boolean blockOnce) {
        this.blockOnce = blockOnce;
    }

    /**
     * Set of model names that are valid.  Invalid ones will throw an error.
     * @param models
     * @since 4.2
     */
    public void setValidModels(Set<String> models) {
        this.validModels = models;
    }

    /**
     * Return collection of Command that has occurred on this data manager
     * @return
     * @since 4.2
     */
    public List<Command> getCommandHistory() {
        return this.commandHistory;
    }

    public List<org.teiid.language.Command> getPushdownCommands() {
        return pushdownCommands;
    }

    /**
     * @see org.teiid.query.processor.ProcessorDataManager#lookupCodeValue(org.teiid.query.util.CommandContext, java.lang.String, java.lang.String, java.lang.String, java.lang.Object)
     * @since 4.2
     */
    public Object lookupCodeValue(CommandContext context,
                                  String codeTableName,
                                  String returnElementName,
                                  String keyElementName,
                                  Object keyValue) throws BlockedException,
                                                  TeiidComponentException {
        return null;
    }

    /**
     * @see org.teiid.query.processor.ProcessorDataManager#registerRequest(CommandContext, org.teiid.query.sql.lang.Command, java.lang.String, RegisterRequestParameter)
     * @since 4.2
     */
    public TupleSource registerRequest(CommandContext context,
                                Command command,
                                String modelName,
                                RegisterRequestParameter parameterObject) throws TeiidComponentException {

        if(modelName != null && validModels != null && ! validModels.contains(modelName)) {
            throw new TeiidComponentException("Detected query against invalid model: " + modelName + ": " + command);  //$NON-NLS-1$//$NON-NLS-2$
        }
        this.commandHistory.add(command);

        List<Expression> projectedSymbols = command.getProjectedSymbols();

        String commandString = null;
        if (lbf == null) {
            if (command instanceof BatchedUpdateCommand && fullBatchedUpdate) {
                commandString = ((BatchedUpdateCommand)command).getStringForm(true);
            } else {
                commandString = command.toString();
            }
        } else {
            org.teiid.language.Command cmd = lbf.translate(command);
            this.pushdownCommands.add(cmd);
            commandString = cmd.toString();
        }

        List<?>[] rows = getData(commandString);
        if(rows == null) {
            if (mustRegisterCommands) {
                throw new TeiidComponentException("Unknown command: " + commandString);  //$NON-NLS-1$
            }
            // Create one row of nulls
            rows = new List[1];
            rows[0] = new ArrayList();

            for(int i=0; i<projectedSymbols.size(); i++) {
                rows[0].add(null);
            }
        }

        FakeTupleSource source = new FakeTupleSource(projectedSymbols, rows);
        if (blockOnce) {
            source.setBlockOnce();
        }
        return source;
    }

    public void setMustRegisterCommands(boolean mustRegisterCommands) {
        this.mustRegisterCommands = mustRegisterCommands;
    }

    public void clearCodeTables() {

    }

    @Override
    public EventDistributor getEventDistributor() {
        return null;
    }

    public void setFullBatchedUpdate(boolean fullBatchedUpdate) {
        this.fullBatchedUpdate = fullBatchedUpdate;
    }

    protected List<?>[] getData(String commandString) {
        return data.get(commandString);
    }
}
