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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.teiid.client.SourceWarning;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.Assertion;
import org.teiid.dqp.internal.datamgr.ConnectorWork;
import org.teiid.dqp.internal.process.DQPCore.CompletionListener;
import org.teiid.dqp.message.AtomicRequestMessage;
import org.teiid.dqp.message.AtomicResultsMessage;
import org.teiid.events.EventDistributor;
import org.teiid.metadata.Table;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.processor.relational.RelationalNodeUtil;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.CacheDirective.Scope;


/**
 * This tuple source impl can only be used once; once it is closed, it
 * cannot be reopened and reused.
 *
 * TODO: the handling of DataNotAvailable is awkward.
 * In the multi-threaded case we'd like to not even
 * notify the parent plan and just schedule the next poll.
 */
public class DataTierTupleSource implements TupleSource, CompletionListener<AtomicResultsMessage> {

    // Construction state
    private final AtomicRequestMessage aqr;
    private final RequestWorkItem workItem;
    private final ConnectorWork cwi;
    private final DataTierManagerImpl dtm;

    private int limit = -1;

    // Data state
    private int index;
    private int rowsProcessed;
    private AtomicResultsMessage arm;
    private AtomicBoolean closing = new AtomicBoolean();
    private AtomicBoolean closed = new AtomicBoolean();
    private volatile boolean canceled;
    private volatile boolean cancelAsynch;
    private boolean executed;
    private volatile boolean done;
    private boolean explicitClose;

    private volatile FutureWork<AtomicResultsMessage> futureResult;
    private volatile boolean running;

    boolean errored;
    Scope scope; //this is to avoid synchronization

    private long waitUntil;
    private Future<Void> scheduledFuture;

    public DataTierTupleSource(AtomicRequestMessage aqr, RequestWorkItem workItem, ConnectorWork cwi, DataTierManagerImpl dtm, int limit) {
        this.aqr = aqr;
        this.workItem = workItem;
        this.cwi = cwi;
        this.dtm = dtm;
        this.limit = limit;
        Assertion.isNull(workItem.getConnectorRequest(aqr.getAtomicRequestID()));
        workItem.addConnectorRequest(aqr.getAtomicRequestID(), this);
    }

    void addWork() {
        futureResult = workItem.addWork(new Callable<AtomicResultsMessage>() {
            @Override
            public AtomicResultsMessage call() throws Exception {
                try {
                    return getResults();
                } finally {
                    if (closing.get() && closed.compareAndSet(false, true)) {
                        cwi.close();
                    }
                }
            }
        }, this, 100);
    }

    public List<?> nextTuple() throws TeiidComponentException, TeiidProcessingException {
        if (waitUntil > 0 && waitUntil > System.currentTimeMillis()) {
            if (!this.cwi.isDataAvailable()) {
                throw BlockedException.block(aqr.getAtomicRequestID(), "Blocking until", waitUntil); //$NON-NLS-1$
            }
            this.waitUntil = 0;
        }
        while (true) {
            if (arm == null) {
                if (isDone()) {
                    //sanity check
                    return null; //TODO: could throw an illegal state exception
                }
                boolean partial = false;
                AtomicResultsMessage results = null;
                boolean noResults = false;
                try {
                    if (futureResult != null || !aqr.isSerial()) {
                        results = asynchGet();
                    } else {
                        results = getResults();
                    }
                    //check for update events
                    if (index == 0 && this.dtm.detectChangeEvents()) {
                        Command command = aqr.getCommand();
                        int commandIndex = 0;
                        if (RelationalNodeUtil.isUpdate(command)) {
                            long ts = System.currentTimeMillis();
                            checkForUpdates(results, command, dtm.getEventDistributor(), commandIndex, ts);
                        } else if (command instanceof BatchedUpdateCommand) {
                            long ts = System.currentTimeMillis();
                            BatchedUpdateCommand bac = (BatchedUpdateCommand)command;
                            for (Command uc : bac.getUpdateCommands()) {
                                checkForUpdates(results, uc, dtm.getEventDistributor(), commandIndex++, ts);
                            }
                        }
                    }
                } catch (TranslatorException e) {
                    errored = true;
                    results = exceptionOccurred(e);
                    partial = true;
                } catch (BlockedException e) {
                    noResults = true;
                    throw e;
                } catch (DataNotAvailableException e) {
                    noResults = true;
                    handleDataNotAvailable(e);
                    continue;
                } finally {
                    if (!noResults && results == null) {
                        errored = true;
                    }
                }
                receiveResults(results, partial);
            }
            if (index < arm.getResults().length) {
                if (limit-- == 0) {
                    this.done = true;
                    arm = null;
                    return null;
                }
                return this.arm.getResults()[index++];
            }
            arm = null;
            if (isDone()) {
                return null;
            }
        }
    }

    private void handleDataNotAvailable(DataNotAvailableException e)
            throws BlockedException {
        if (e.getWaitUntil() != null) {
            long timeDiff = e.getWaitUntil().getTime() - System.currentTimeMillis();
            if (timeDiff <= 0) {
                //already met the time
                return;
            }
            if (e.isStrict()) {
                this.waitUntil = e.getWaitUntil().getTime();
            }
            scheduleMoreWork(timeDiff);
        } else if (e.getRetryDelay() >= 0) {
            if (e.isStrict()) {
                this.waitUntil = System.currentTimeMillis() + e.getRetryDelay();
            }
            scheduleMoreWork(e.getRetryDelay());
        } else if (this.cwi.isDataAvailable()) {
            return; //no polling, but data is already available
        } else if (e.isStrict()) {
            //no polling, wait indefinitely
            this.waitUntil = Long.MAX_VALUE;
        }
        throw BlockedException.block(aqr.getAtomicRequestID(), "Blocking on DataNotAvailableException", aqr.getAtomicRequestID()); //$NON-NLS-1$
    }

    private void scheduleMoreWork(long timeDiff) {
        if (scheduledFuture != null) {
            this.scheduledFuture.cancel(false);
        }
        scheduledFuture = workItem.scheduleWork(timeDiff);
    }

    private void checkForUpdates(AtomicResultsMessage results, Command command,
            EventDistributor distributor, int commandIndex, long ts) {
        if (!RelationalNodeUtil.isUpdate(command) || !(command instanceof ProcedureContainer)) {
            return;
        }
        ProcedureContainer pc = (ProcedureContainer)command;
        GroupSymbol gs = pc.getGroup();
        Integer zero = Integer.valueOf(0);
        if (results.getResults().length <= commandIndex || zero.equals(results.getResults()[commandIndex].get(0))) {
            return;
        }
        Object metadataId = gs.getMetadataID();
        if (metadataId == null) {
            return;
        }
        if (!(metadataId instanceof Table)) {
            if (metadataId instanceof TempMetadataID) {
                TempMetadataID tid = (TempMetadataID)metadataId;
                if (tid.getTableData().getModel() != null) {
                    tid.getTableData().dataModified((Integer)results.getResults()[commandIndex].get(0));
                }
            }
            return;
        }
        Table t = (Table)metadataId;
        t.setLastDataModification(ts);
        if (distributor != null) {
            distributor.dataModification(this.workItem.getDqpWorkContext().getVdbName(), this.workItem.getDqpWorkContext().getVdbVersion(), t.getParent().getName(), t.getName());
        }
    }

    private AtomicResultsMessage asynchGet()
            throws BlockedException, TeiidProcessingException,
            TeiidComponentException, TranslatorException {
        if (futureResult == null) {
            addWork();
        }
        if (!futureResult.isDone()) {
            throw BlockedException.block(aqr.getAtomicRequestID(), "Blocking on source query", aqr.getAtomicRequestID()); //$NON-NLS-1$
        }
        FutureWork<AtomicResultsMessage> currentResults = futureResult;
        futureResult = null;
        AtomicResultsMessage results = null;
        try {
            results = currentResults.get();
            if (results.getFinalRow() < 0) {
                addWork();
            }
        } catch (CancellationException e) {
            throw new TeiidProcessingException(e);
        } catch (InterruptedException e) {
             throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30503, e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TeiidProcessingException) {
                throw (TeiidProcessingException)e.getCause();
            }
            if (e.getCause() instanceof TeiidComponentException) {
                throw (TeiidComponentException)e.getCause();
            }
            if (e.getCause() instanceof TranslatorException) {
                throw (TranslatorException)e.getCause();
            }
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException)e.getCause();
            }
            //shouldn't happen
            throw new RuntimeException(e);
        }
        return results;
    }

    AtomicResultsMessage getResults()
            throws BlockedException, TeiidComponentException,
            TranslatorException {
        AtomicResultsMessage results = null;
        if (cancelAsynch) {
            return null;
        }
        running = true;
        try {
            if (!executed) {
                cwi.execute();
                executed = true;
            }
            results = cwi.more();
        } finally {
            running = false;
        }
        return results;
    }

    public boolean isQueued() {
        FutureWork<AtomicResultsMessage> future = futureResult;
        return !running && future != null && !future.isDone();
    }

    public boolean isDone() {
        return done;
    }

    public boolean isRunning() {
        return running;
    }

    public void fullyCloseSource() {
        cancelFutures();
        cancelAsynch = true;
        if (closing.compareAndSet(false, true)) {
            if (!done && !errored) {
                this.cwi.cancel(false);
            }
            workItem.closeAtomicRequest(this.aqr.getAtomicRequestID());
            if (aqr.isSerial() || futureResult == null) {
                this.cwi.close();
            } else {
                futureResult.addCompletionListener(new CompletionListener<AtomicResultsMessage>() {

                    @Override
                    public void onCompletion(FutureWork<AtomicResultsMessage> future) {
                        if (running) {
                            return; //-- let the other thread close
                        }
                        if (closed.compareAndSet(false, true)) {
                            cwi.close();
                        }
                    }
                });
            }
        }
    }

    public boolean isCanceled() {
        return canceled;
    }

    public void cancelRequest() {
        this.canceled = true;
        this.cwi.cancel(true);
        cancelFutures();
    }

    /**
     * @see TupleSource#closeSource()
     */
    public void closeSource() {
        cancelFutures();
        cancelAsynch = true;
        if (!explicitClose) {
            fullyCloseSource();
        }
    }

    private void cancelFutures() {
        if (this.scheduledFuture != null) {
            this.scheduledFuture.cancel(true);
            this.scheduledFuture = null;
        }
        if (this.futureResult != null) {
            this.futureResult.cancel(false);
        }
    }

    AtomicResultsMessage exceptionOccurred(TranslatorException exception) throws TeiidComponentException, TeiidProcessingException {
        if(workItem.requestMsg.supportsPartialResults()) {
            AtomicResultsMessage emptyResults = new AtomicResultsMessage(new List[0]);
            emptyResults.setWarnings(Arrays.asList((Exception)exception));
            emptyResults.setFinalRow(this.rowsProcessed);
            return emptyResults;
        }
        fullyCloseSource();
        if (exception.getCause() instanceof TeiidComponentException) {
            throw (TeiidComponentException)exception.getCause();
        }
        if (exception.getCause() instanceof TeiidProcessingException) {
            throw (TeiidProcessingException)exception.getCause();
        }
         throw new TeiidProcessingException(QueryPlugin.Event.TEIID30504, exception, this.getConnectorName() + ": " + exception.getMessage()); //$NON-NLS-1$
    }

    void receiveResults(AtomicResultsMessage response, boolean partial) {
        this.arm = response;
        this.scope = response.getScope();
        if (this.scope != null) {
            this.aqr.getCommandContext().setDeterminismLevel(CachingTupleSource.getDeterminismLevel(this.scope));
        }
        explicitClose |= !arm.supportsImplicitClose();
        rowsProcessed += response.getResults().length;
        index = 0;
        if (response.getWarnings() != null) {
            for (Exception warning : response.getWarnings()) {
                SourceWarning sourceFailure = new SourceWarning(this.aqr.getModelName(), aqr.getConnectorName(), warning, partial);
                this.aqr.getCommandContext().addWarning(sourceFailure);
            }
        }
        if (response.getFinalRow() >= 0) {
            done = true;
        }
    }

    public AtomicRequestMessage getAtomicRequestMessage() {
        return aqr;
    }

    public String getConnectorName() {
        return this.aqr.getConnectorName();
    }

    public boolean isTransactional() {
        return this.aqr.isTransactional();
    }

    @Override
    public void onCompletion(FutureWork<AtomicResultsMessage> future) {
        if (!cancelAsynch) {
            workItem.moreWork(); //this is not necessary in some situations with DataNotAvailable
        }
    }

    public boolean isExplicitClose() {
        return explicitClose;
    }

    public Future<Void> getScheduledFuture() {
        return scheduledFuture;
    }

}
