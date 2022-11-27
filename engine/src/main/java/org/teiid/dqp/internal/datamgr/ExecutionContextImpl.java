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

package org.teiid.dqp.internal.datamgr;

import java.io.Serializable;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import javax.security.auth.Subject;

import org.teiid.GeneratedKeys;
import org.teiid.adminapi.Session;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.dqp.internal.process.RequestWorkItem;
import org.teiid.dqp.message.RequestID;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.language.Insert;
import org.teiid.language.NamedTable;
import org.teiid.metadata.Column;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.CacheDirective;
import org.teiid.translator.CacheDirective.Scope;
import org.teiid.translator.ExecutionContext;


/**
 */
public class ExecutionContextImpl implements ExecutionContext {
    //  Access Node ID
    private String partID;
    // currentConnector ID
    private String connectorName;
    // Execute count of the query
    private String executeCount;
    // keep the execution object alive during the processing. default:false
    private boolean keepAlive = false;

    private boolean isTransactional;

    private int batchSize = BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE;
    private List<Exception> warnings = new LinkedList<Exception>();
    private Session session;
    private boolean dataAvailable;
    private Collection<String> generalHint;
    private Collection<String> hint;
    private CommandContext commandContext;
    private CacheDirective cacheDirective;
    private RuntimeMetadata runtimeMetadata;
    private ConnectorWorkItem workItem;
    private Scope scope;
    private List<Column> generatedKeyColumns;
    private GeneratedKeys generatedKeys;

    public ExecutionContextImpl(String vdbName, Object vdbVersion,  Serializable executionPayload,
            String originalConnectionID, String connectorName, long requestId, String partId, String execCount) {
        commandContext = new CommandContext();
        commandContext.setVdbName(vdbName);
        commandContext.setVdbVersion(vdbVersion.toString());
        commandContext.setCommandPayload(executionPayload);
        commandContext.setConnectionID(originalConnectionID);
        commandContext.setRequestId(new RequestID(originalConnectionID, requestId));
        this.connectorName = connectorName;
        this.partID = partId;
        this.executeCount = execCount;
    }

    public ExecutionContextImpl(CommandContext commandContext, String connectorName, String partId, String execCount, ConnectorWorkItem workItem) {
        this.connectorName = connectorName;
        this.partID = partId;
        this.executeCount = execCount;
        this.commandContext = commandContext;
        this.workItem = workItem;
    }

    @Override
    public org.teiid.CommandContext getCommandContext() {
        return this.commandContext;
    }

    public String getConnectorIdentifier() {
        return this.connectorName;
    }

    @Override
    public String getRequestId() {
        return this.commandContext.getRequestId();
    }

    @Override
    public String getPartIdentifier() {
        return this.partID;
    }

    @Override
    public String getExecutionCountIdentifier() {
        return this.executeCount;
    }
    @Override
    public String getVdbName() {
        return this.commandContext.getVdbName();
    }
    @Override
    public String getVdbVersion() {
        return this.commandContext.getVdbVersion();
    }
    @Override
    public Subject getSubject() {
        return this.commandContext.getSubject();
    }

    @Override
    public Serializable getCommandPayload() {
        return this.commandContext.getCommandPayload();
    }

    @Override
    public String getConnectionId() {
        return this.commandContext.getConnectionId();
    }
    @Override
    public void keepExecutionAlive(boolean alive) {
        this.keepAlive = alive;
    }

    boolean keepExecutionAlive() {
        return this.keepAlive;
    }

    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }
        if(! (obj instanceof ExecutionContext)) {
            return false;
        }
        ExecutionContext other = (ExecutionContext) obj;
        return EquivalenceUtil.areEqual(this.getRequestId(), other.getRequestId()) &&
        EquivalenceUtil.areEqual(this.getPartIdentifier(), other.getPartIdentifier());
    }

    public int hashCode() {
        return HashCodeUtil.hashCode(HashCodeUtil.hashCode(0, getRequestId()), partID);
    }

    public String toString() {
        String userName = null;
        if (this.getSubject() != null) {
            for(Principal p:this.getSubject().getPrincipals()) {
                userName = p.getName();
            }
        }
        return "ExecutionContext<vdb=" + this.getVdbName() + ", version=" + this.getVdbVersion() + ", user=" + userName + ">"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Override
    public boolean isTransactional() {
        return isTransactional;
    }

    void setTransactional(boolean isTransactional) {
        this.isTransactional = isTransactional;
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * Add an exception as a warning to this Execution.
     */
    @Override
    public void addWarning(Exception ex) {
        if (ex == null) {
            return;
        }
        this.warnings.add(ex);
    }

    public List<Exception> getWarnings() {
        List<Exception> result = new ArrayList<Exception>(warnings);
        warnings.clear();
        return result;
    }

    @Override
    public Session getSession() {
        return this.session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    @Override
    public synchronized void dataAvailable() {
        RequestWorkItem requestWorkItem = this.commandContext.getWorkItem();
        dataAvailable = true;
        if (requestWorkItem != null) {
            requestWorkItem.moreWork();
        }
    }

    public synchronized boolean isDataAvailable() {
        boolean result = dataAvailable;
        dataAvailable = false;
        return result;
    }

    @Override
    public String getGeneralHint() {
        return StringUtil.join(generalHint, " "); //$NON-NLS-1$
    }

    @Override
    public String getSourceHint() {
        return StringUtil.join(hint, " "); //$NON-NLS-1$
    }

    @Override
    public Collection<String> getGeneralHints() {
        return generalHint;
    }

    @Override
    public Collection<String> getSourceHints() {
        return hint;
    }

    public void setGeneralHints(Collection<String> generalHint) {
        this.generalHint = generalHint;
    }

    public void setHints(Collection<String> hint) {
        this.hint = hint;
    }

    @Override
    public String getConnectionID() {
        return getConnectionId();
    }

    @Override
    public Serializable getExecutionPayload() {
        return getCommandPayload();
    }

    @Override
    public String getRequestID() {
        return getRequestId();
    }

    @Override
    public CacheDirective getCacheDirective() {
        return cacheDirective;
    }

    public void setCacheDirective(CacheDirective directive) {
        this.cacheDirective = directive;
    }

    public void setRuntimeMetadata(RuntimeMetadataImpl queryMetadata) {
        this.runtimeMetadata = queryMetadata;
    }

    @Override
    public RuntimeMetadata getRuntimeMetadata() {
        return this.runtimeMetadata;
    }

    @Override
    public void logCommand(Object... command) {
        if (this.workItem != null) {
            this.workItem.logCommand(command);
        }
    }

    @Override
    public Scope getScope() {
        return scope;
    }

    @Override
    public void setScope(Scope scope) {
        this.scope = scope;
    }

    @Override
    public List<Column> getGeneratedKeyColumns() {
        return this.generatedKeyColumns;
    }

    public void setGeneratedKeyColumns(List<Column> generatedKeyColumns) {
        this.generatedKeyColumns = generatedKeyColumns;
    }

    @Override
    public GeneratedKeys returnGeneratedKeys() {
        if (generatedKeys == null && generatedKeyColumns != null) {
            String[] colNames = new String[generatedKeyColumns.size()];
            Class<?>[] colTypes = new Class<?>[generatedKeyColumns.size()];
            for (int i = 0; i < colNames.length; i++) {
                Column c = generatedKeyColumns.get(i);
                colNames[i] = c.getName();
                colTypes[i] = c.getJavaType();
            }
            generatedKeys = this.commandContext.returnGeneratedKeys(colNames, colTypes);
        }
        return generatedKeys;
    }

    public void setGeneratedKeyColumns(org.teiid.language.Command translatedCommand) {
        if (!(translatedCommand instanceof Insert)) {
            return;
        }
        Insert insert = (Insert)translatedCommand;
        NamedTable nt = insert.getTable();
        Table t = nt.getMetadataObject();
        if (t == null) {
            return;
        }
        KeyRecord key = t.getPrimaryKey();
        if (key == null) {
            return;
        }
        List<Column> generated = null;
        for (Column c : key.getColumns()) {
            if (c.isAutoIncremented()) {
                if (generated == null) {
                    generated = new ArrayList<>(1);
                }
                generated.add(c);
            }
        }
        setGeneratedKeyColumns(generated);
    }

    @Override
    public int getTransactionIsolation() {
        TransactionContext tc = this.commandContext.getTransactionContext();
        if (tc != null) {
            return tc.getIsolationLevel();
        }
        return ConnectionImpl.DEFAULT_ISOLATION;
    }

}
