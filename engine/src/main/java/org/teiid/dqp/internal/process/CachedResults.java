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

import java.io.Serializable;

import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.cache.Cachable;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleBufferCache;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.util.Assertion;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.parser.ParseInfo;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.lang.Command;


public class CachedResults implements Serializable, Cachable {
    private static final long serialVersionUID = -5603182134635082207L;

    private transient Command command;
    private transient TupleBuffer results;

    private String uuid;
    private boolean hasLobs;
    private int rowLimit;

    private AccessInfo accessInfo = new AccessInfo();

    public String getId() {
        return this.uuid;
    }

    public TupleBuffer getResults() {
        return results;
    }

    public void setResults(TupleBuffer results, ProcessorPlan plan) {
        this.results = results;
        this.uuid = results.getId();
        this.hasLobs = results.isLobs();
        if (plan != null) {
            this.accessInfo.populate(plan.getContext(), true);
        }
    }

    public void setCommand(Command command) {
        this.command = command;
    }

    public synchronized Command getCommand(String sql, QueryMetadataInterface metadata, ParseInfo info) throws QueryParserException, QueryResolverException, TeiidComponentException {
        if (command == null) {
            command = QueryParser.getQueryParser().parseCommand(sql, info);
        }
        QueryResolver.resolveCommand(command, metadata);
        return command;
    }

    @Override
    public boolean prepare(TupleBufferCache bufferManager) {
        Assertion.assertTrue(!this.results.isForwardOnly());
        bufferManager.distributeTupleBuffer(this.results.getId(), results);
        return true;
    }

    @Override
    public synchronized boolean restore(TupleBufferCache bufferManager) {
        if (this.results == null) {
            if (this.hasLobs) {
                return false; //the lob store is local only and not distributed
            }
            TupleBuffer buffer = bufferManager.getTupleBuffer(this.uuid);
            if (buffer != null) {
                this.results = buffer;
            }

            try {
                this.accessInfo.restore();
            } catch (TeiidException e) {
                LogManager.logWarning(LogConstants.CTX_DQP, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30025));
                return false;
            }
        }
        return (this.results != null);
    }

    @Override
    public AccessInfo getAccessInfo() {
        return accessInfo;
    }

    public int getRowLimit() {
        return rowLimit;
    }

    public void setRowLimit(int rowLimit) {
        this.rowLimit = rowLimit;
    }

}
