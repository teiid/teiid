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

import java.lang.ref.WeakReference;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.client.BatchSerializer;
import org.teiid.client.RequestMessage;
import org.teiid.client.RequestMessage.ShowPlan;
import org.teiid.client.ResizingArrayList;
import org.teiid.client.ResultsMessage;
import org.teiid.client.lob.LobChunk;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.client.plan.PlanNode;
import org.teiid.client.util.ExceptionUtil;
import org.teiid.client.util.ResultsReceiver;
import org.teiid.client.xa.XATransactionException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.dqp.internal.datamgr.ThreadCpuTimer;
import org.teiid.dqp.internal.process.AuthorizationValidator.CommandType;
import org.teiid.dqp.internal.process.DQPCore.CompletionListener;
import org.teiid.dqp.internal.process.SessionAwareCache.CacheID;
import org.teiid.dqp.internal.process.ThreadReuseExecutor.PrioritizedRunnable;
import org.teiid.dqp.message.AtomicRequestID;
import org.teiid.dqp.message.RequestID;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.dqp.service.TransactionContext.Scope;
import org.teiid.dqp.service.TransactionService;
import org.teiid.jdbc.EnhancedTimer.Task;
import org.teiid.jdbc.SQLStates;
import org.teiid.logging.CommandLogMessage.Event;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.parser.ParseInfo;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.BatchCollector;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.processor.QueryProcessor.ExpiredTimeSliceException;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.util.CommandContext;
import org.teiid.query.util.GeneratedKeysImpl;
import org.teiid.query.util.Options;
import org.teiid.query.util.TeiidTracingUtil;

import io.opentracing.Span;

/**
 * Compiles results and other information for the client.  There is quite a bit of logic
 * surrounding forming batches to prevent buffer growth, send multiple batches at a time,
 * partial batches, etc.  There is also special handling for the update count case, which
 * needs to read the entire result before sending it back to the client.
 */
public class RequestWorkItem extends AbstractWorkItem implements PrioritizedRunnable {

    public static final String REQUEST_KEY = "teiid-request"; //$NON-NLS-1$

    //TODO: this could be configurable
    private static final int OUTPUT_BUFFER_MAX_BATCHES = 8;
    private static final int CLIENT_FETCH_MAX_BATCHES = 3;

    public static final class MoreWorkTask implements Runnable {

        WeakReference<RequestWorkItem> ref;

        public MoreWorkTask(RequestWorkItem workItem) {
            ref = new WeakReference<RequestWorkItem>(workItem);
        }

        @Override
        public void run() {
            RequestWorkItem item = ref.get();
            if (item != null) {
                item.moreWork();
            }
        }
    }

    private final class WorkWrapper<T> implements
            DQPCore.CompletionListener<T> {

        boolean submitted;
        FutureWork<T> work;

        public WorkWrapper(FutureWork<T> work) {
            this.work = work;
        }

        @Override
        public void onCompletion(FutureWork<T> future) {
            WorkWrapper<?> nextWork = null;
            synchronized (queue) {
                if (!submitted) {
                    return;
                }
                synchronized (RequestWorkItem.this) {
                    if (isProcessing()) {
                        totalThreads--;
                        moreWork();
                        return;
                    }
                }
                nextWork = queue.pollFirst();
                if (nextWork == null) {
                    totalThreads--;
                } else {
                    nextWork.submitted = true;
                }
            }
            if (nextWork != null) {
                dqpCore.addWork(nextWork.work);
            }
        }
    }

    private enum ProcessingState {NEW, PROCESSING, CLOSE}
    private volatile ProcessingState state = ProcessingState.NEW;

    private enum TransactionState {NONE, ACTIVE, DONE}
    private TransactionState transactionState = TransactionState.NONE;

    private int totalThreads;
    private LinkedList<WorkWrapper<?>> queue = new LinkedList<WorkWrapper<?>>();

    /*
     * Obtained at construction time
     */
    protected final DQPCore dqpCore;
    final RequestMessage requestMsg;
    final RequestID requestID;
    private Request request; //provides the processing plan, held on a temporary basis
    private Options options;
    private final int processorTimeslice;
    private CacheID cid;
    private final TransactionService transactionService;
    private final DQPWorkContext dqpWorkContext;
    boolean active;

    /*
     * obtained during new
     */
    private volatile QueryProcessor processor;
    private BatchCollector collector;
    private Command originalCommand;
    private AnalysisRecord analysisRecord;
    private TransactionContext transactionContext;
    TupleBuffer resultsBuffer;
    private boolean returnsUpdateCount;

    /*
     * maintained during processing
     */
    private Throwable processingException;
    private Map<AtomicRequestID, DataTierTupleSource> connectorInfo = Collections.synchronizedMap(new HashMap<AtomicRequestID, DataTierTupleSource>(4));
    private volatile boolean doneProducingBatches;
    private volatile boolean isClosed;
    private volatile boolean isCanceled;
    private String cancelReason;
    private volatile boolean closeRequested;

    //results request
    private ResultsReceiver<ResultsMessage> resultsReceiver;
    private int begin;
    private int end;
    private TupleBatch savedBatch;
    private Map<Integer, LobWorkItem> lobStreams = Collections.synchronizedMap(new HashMap<Integer, LobWorkItem>(4));

    /**The time when command begins processing on the server.*/
    private long processingTimestamp = System.currentTimeMillis();

    protected boolean useCallingThread;
    private volatile boolean hasThread;

    private Future<Void> cancelTask;
    private Future<Void> moreWorkTask;

    private boolean explicitSourceClose;
    private int schemaSize;

    AtomicLong dataBytes = new AtomicLong();
    private long planningStart;
    private long planningEnd;

    private ThreadCpuTimer timer = new ThreadCpuTimer();

    private Span span;

    public RequestWorkItem(DQPCore dqpCore, RequestMessage requestMsg, Request request, ResultsReceiver<ResultsMessage> receiver, RequestID requestID, DQPWorkContext workContext) {
        this.requestMsg = requestMsg;
        this.requestID = requestID;
        this.processorTimeslice = dqpCore.getProcessorTimeSlice();
        this.transactionService = dqpCore.getTransactionService();
        this.dqpCore = dqpCore;
        this.request = request;
        if (request != null) {
            this.options = request.options;
        }
        this.dqpWorkContext = workContext;
        this.requestResults(1, requestMsg.getFetchSize(), receiver);
    }

    private boolean isForwardOnly() {
        return this.cid == null && requestMsg.getCursorType() == ResultSet.TYPE_FORWARD_ONLY;
    }

    /**
     * Ask for results.
     * @param beginRow
     * @param endRow
     */
    synchronized void requestResults(int beginRow, int endRow, ResultsReceiver<ResultsMessage> receiver) {
        if (this.resultsReceiver != null) {
            throw new IllegalStateException("Results already requested"); //$NON-NLS-1$\
        }
        this.resultsReceiver = receiver;
        this.begin = beginRow;
        this.end = endRow;
    }

    @Override
    protected boolean isDoneProcessing() {
        return isClosed;
    }

    @Override
    public void run() {
        io.opentracing.Scope scope = null;
        if (this.span != null) {
            scope = TeiidTracingUtil.getInstance().activateSpan(this.span);
        }
        hasThread = true;
        timer.start();
        LogManager.putMdc(REQUEST_KEY, requestID.toString());
        try {
            while (!isDoneProcessing()) {
                super.run();
                if (!useCallingThread) {
                    break;
                }
                //should use the calling thread
                synchronized (this) {
                    if (this.resultsReceiver == null) {
                        break; //allow results to be processed by calling thread
                    }
                    if (this.getThreadState() == ThreadState.MORE_WORK) {
                        continue;
                    }
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        try {
                            requestCancel("Interrupted"); //$NON-NLS-1$
                        } catch (TeiidComponentException e1) {
                             throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30543, e1);
                        }
                    }
                }
            }
        } finally {
            timer.stop();
            LogManager.removeMdc(REQUEST_KEY);
            hasThread = false;
            if (scope != null) {
                scope.close();
            }
        }
    }

    @Override
    protected void resumeProcessing() {
        if (!this.useCallingThread) {
            dqpCore.addWork(this);
        }
    }

    /**
     * Special call from request threads to allow resumption of processing by
     * the calling thread.
     */
    public void doMoreWork() {
        boolean run = false;
        synchronized (this) {
            moreWork();
            if (!useCallingThread || this.getThreadState() != ThreadState.MORE_WORK) {
                return;
            }
            run = !hasThread;
        }
        if (run) {
            //run outside of the lock
            LogManager.logDetail(LogConstants.CTX_DQP, "Restarting processing using the calling thread", requestID); //$NON-NLS-1$
            run();
        }
    }

    @Override
    protected void process() {
        LogManager.logDetail(LogConstants.CTX_DQP, "Request Thread", requestID, "with state", state); //$NON-NLS-1$ //$NON-NLS-2$
        try {
            if (this.state == ProcessingState.NEW) {
                state = ProcessingState.PROCESSING;
                processNew();
                if (isCanceled) {
                    setCanceledException();
                    state = ProcessingState.CLOSE;
                }
            }

            resume();

            if (this.state == ProcessingState.PROCESSING) {
                if (!this.closeRequested) {
                    processMore();
                } else {
                    this.state = ProcessingState.CLOSE;
                }
            }
        } catch (BlockedException e) {
            if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
                LogManager.logDetail(LogConstants.CTX_DQP, "Request Thread", requestID, "- processor blocked"); //$NON-NLS-1$ //$NON-NLS-2$
            }
            if (e == BlockedException.BLOCKED_ON_MEMORY_EXCEPTION || e instanceof ExpiredTimeSliceException) {
                //requeue
                this.moreWork();
            }
        } catch (Throwable e) {
            handleThrowable(e);
        } finally {
            if (isClosed) {
                /*
                 * since there may be a client waiting notify them of a problem
                 */
                if (this.processingException == null) {
                    this.processingException = new IllegalStateException("Request is already closed"); //$NON-NLS-1$
                }
                sendError();
            } else if (this.state == ProcessingState.CLOSE) {
                close();
            }
            suspend();
        }
    }

    private void setCanceledException() {
        this.processingException = new TeiidProcessingException(QueryPlugin.Event.TEIID30160, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30160, this.requestID));
    }

    private void handleThrowable(Throwable e) {
        e = logError(e);

        this.processingException = e;
        this.state = ProcessingState.CLOSE;
    }

    private Throwable logError(Throwable e) {
        if (!isCanceled()) {
            dqpCore.logMMCommand(this, Event.ERROR, null, null);
            //Case 5558: Differentiate between system level errors and
            //processing errors.  Only log system level errors as errors,
            //log the processing errors as warnings only
            if (this.options.isSanitizeMessages() && !LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
                e = ExceptionUtil.sanitize(e, true);
            }
            if(e instanceof TeiidProcessingException) {
                Throwable cause = e;
                while (cause.getCause() != null && cause.getCause() != cause) {
                    cause = cause.getCause();
                }
                StackTraceElement[] elems = cause.getStackTrace();
                Object elem = null;
                String causeMsg = cause.getMessage();
                if (elems.length > 0) {
                    StackTraceElement ste = cause.getStackTrace()[0];
                    if (ste.getLineNumber() > 0 && ste.getFileName() != null) {
                        elem = ste.getFileName() + ":" + ste.getLineNumber(); //$NON-NLS-1$
                    } else {
                        elem = ste;
                    }
                    String msg = e.getMessage();
                    if (causeMsg != null && cause != e && !msg.contains(causeMsg)) {
                        elem = "'" + causeMsg + "' " + elem; //$NON-NLS-1$ //$NON-NLS-2$
                    }
                } else if (cause != e && causeMsg != null) {
                    elem = "'" + cause.getMessage() + "'"; //$NON-NLS-1$ //$NON-NLS-2$
                }
                String msg = QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30020, e.getMessage(), requestID, e.getClass().getSimpleName(), elem);
                if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
                    LogManager.logWarning(LogConstants.CTX_DQP, e, msg);
                } else {
                    LogManager.logWarning(LogConstants.CTX_DQP, msg + QueryPlugin.Util.getString("stack_info")); //$NON-NLS-1$
                }
            } else {
                LogManager.logError(LogConstants.CTX_DQP, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30019, requestID));
            }
        } else {
            LogManager.logDetail(LogConstants.CTX_DQP, e, "Request Thread", requestID, "- error occurred after cancel"); //$NON-NLS-1$ //$NON-NLS-2$
        }
        return e;
    }

    private void resume() throws XATransactionException {
        if (this.transactionState == TransactionState.ACTIVE) {
            this.transactionService.resume(this.transactionContext);
        }
    }

    private boolean isSuspendable() {
        return this.transactionContext.getTransaction() != null
                && !(this.useCallingThread && (this.transactionContext.getTransactionType() == Scope.GLOBAL
                        || this.transactionContext.getTransactionType() == Scope.INHERITED));
    }

    private void suspend() {
        if (this.transactionState != TransactionState.NONE && isSuspendable()) {
            try {
                this.transactionService.suspend(this.transactionContext);
            } catch (XATransactionException e) {
                LogManager.logDetail(LogConstants.CTX_DQP, e, "Error suspending active transaction"); //$NON-NLS-1$
            }
        }
    }

    protected void processMore() throws BlockedException, TeiidException {
        if (!doneProducingBatches) {
            synchronized (queue) {
                while (!queue.isEmpty() && totalThreads < dqpCore.getUserRequestSourceConcurrency()) {
                    WorkWrapper<?> w = queue.removeFirst();
                    dqpCore.addWork(w.work);
                    w.submitted = true;
                    totalThreads++;
                }
            }
            this.processor.getContext().setTimeSliceEnd(System.currentTimeMillis() + this.processorTimeslice);
            sendResultsIfNeeded(null);
            synchronized (this) {
                if (resultsReceiver == null && cursorRequestExpected()) {
                    //if we proceed we'll possibly violate the precondition of sendResultsIfNeeded, so wait until results are asked for
                    return;
                }
            }
            try {
                CommandContext.pushThreadLocalContext(this.processor.getContext());
                this.resultsBuffer = collector.collectTuples();
            } finally {
                CommandContext.popThreadLocalContext();
            }
            if (!doneProducingBatches) {
                done();
            }
        }
        if (this.transactionState == TransactionState.ACTIVE) {
            this.transactionState = TransactionState.DONE;
            if (transactionContext.getTransactionType() == TransactionContext.Scope.REQUEST) {
                /*
                 * TEIID-14 if we are done producing batches, then proactively close transactional
                 * executions even ones that were intentionally kept alive. this may
                 * break the read of a lob from a transactional source under a transaction
                 * if the source does not support holding the clob open after commit
                 */
                for (DataTierTupleSource connectorRequest : getConnectorRequests()) {
                    if (connectorRequest.isTransactional()) {
                        connectorRequest.fullyCloseSource();
                    }
                }
                this.transactionService.commit(transactionContext);
            } else {
                suspend();
            }
        }
        sendResultsIfNeeded(null);
    }

    /**
     * Client close is currently implemented as asynch.
     * Any errors that occur will not make it to the client, instead we just log them here.
     */
    protected void close() {
        long rowcount = -1;
        try {
            cancelCancelTask();
            if (moreWorkTask != null) {
                moreWorkTask.cancel(false);
                moreWorkTask = null;
            }
            if (this.resultsBuffer != null) {
                if (this.processor != null) {
                    try {
                        CommandContext.pushThreadLocalContext(this.processor.getContext());
                        this.processor.closeProcessing();

                        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
                            LogManager.logDetail(LogConstants.CTX_DQP, "Removing tuplesource for the request " + requestID); //$NON-NLS-1$
                        }
                        rowcount = resultsBuffer.getRowCount();
                        if (this.cid == null || !this.doneProducingBatches) {
                            resultsBuffer.remove();
                        }

                        for (DataTierTupleSource connectorRequest : getConnectorRequests()) {
                            connectorRequest.fullyCloseSource();
                        }

                        CommandContext cc = this.processor.getContext();
                        cc.close();
                    } catch (Throwable t) {
                        handleThrowable(t); //guard against unexpected exceptions in close
                    } finally {
                        CommandContext.popThreadLocalContext();
                    }
                }

                this.resultsBuffer = null;

                if (!this.lobStreams.isEmpty()) {
                    List<LobWorkItem> lobs = null;
                    synchronized (lobStreams) {
                        lobs = new ArrayList<LobWorkItem>(this.lobStreams.values());
                    }
                    for (LobWorkItem lobWorkItem : lobs) {
                        lobWorkItem.close();
                    }
                }
            }

            boolean isActive = false;
            if (this.transactionState == TransactionState.ACTIVE) {
                isActive = true;
                this.transactionState = TransactionState.DONE;
            }

            if (transactionContext != null) {
                if (transactionContext.getTransactionType() == TransactionContext.Scope.REQUEST) {
                    if (!isActive) {
                        LogManager.logWarning(LogConstants.CTX_DQP, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31200, transactionContext.getTransactionId()));
                    }
                    try {
                        this.transactionService.rollback(transactionContext);
                    } catch (XATransactionException e1) {
                        LogManager.logWarning(LogConstants.CTX_DQP, e1, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30028));
                    }
                } else if (transactionContext.getTransactionType() != TransactionContext.Scope.NONE) {
                    suspend();
                }
            }

            synchronized (this) {
                if (this.processingException == null && this.resultsReceiver != null) {
                    //sanity check to ensure that something will be sent to the client
                    setCanceledException();
                }
            }
        } catch (Throwable t) {
            handleThrowable(t);
        } finally {
            isClosed = true;
            dqpCore.removeRequest(this);

            if (this.processingException != null) {
                sendError();
            }
            dqpCore.logMMCommand(this, Event.END, rowcount, this.timer.stop());
        }
    }

    private void cancelCancelTask() {
        if (this.cancelTask != null) {
            this.cancelTask.cancel(false);
            this.cancelTask = null;
        }
    }

    protected void processNew() throws TeiidProcessingException, TeiidComponentException {
        planningStart = System.currentTimeMillis();
        SessionAwareCache<CachedResults> rsCache = dqpCore.getRsCache();

        boolean cachable = false;
        CacheID cacheId = null;

        if (rsCache != null) {
            boolean canUseCache = true;
            if (requestMsg.getRequestOptions().isContinuous()) {
                canUseCache = false;
                LogManager.logDetail(LogConstants.CTX_DQP, requestID, "Command is continuous, result set caching will not be used"); //$NON-NLS-1$
            } else if (!requestMsg.useResultSetCache() && getCacheHint() == null) {
                canUseCache = false;
                LogManager.logDetail(LogConstants.CTX_DQP, requestID, "Command has no cache hint and result set cache mode is not on."); //$NON-NLS-1$
            }
            if (canUseCache) {
                ParseInfo pi = Request.createParseInfo(requestMsg, this.dqpWorkContext.getSession());
                cacheId = new CacheID(this.dqpWorkContext, pi, requestMsg.getCommandString());
                cachable = cacheId.setParameters(requestMsg.getParameterValues());
                if (cachable) {
                    //allow cache to be transactionally aware
                    if (rsCache.isTransactional()) {
                        TransactionContext tc = request.getTransactionContext(false);
                        if (tc != null && tc.getTransactionType() != Scope.NONE) {
                            initTransactionState(tc);
                            resume();
                        }
                    }
                    CachedResults cr = rsCache.get(cacheId);
                    //check that there are enough cached results
                    //TODO: possibly ignore max rows for caching
                    if (cr != null && (cr.getRowLimit() == 0 || (requestMsg.getRowLimit() != 0 && requestMsg.getRowLimit() <= cr.getRowLimit()))) {
                        request.initMetadata();
                        this.originalCommand = cr.getCommand(requestMsg.getCommandString(), request.metadata, pi);
                        if (!request.validateAccess(requestMsg.getCommands(), this.originalCommand, CommandType.CACHED)) {
                            LogManager.logDetail(LogConstants.CTX_DQP, requestID, "Using result set cached results", cacheId); //$NON-NLS-1$
                            this.resultsBuffer = cr.getResults();
                            doneProducingBatches();
                            return;
                        }
                        LogManager.logDetail(LogConstants.CTX_DQP, requestID, "Cached result command to be modified, will not use the cached results", cacheId); //$NON-NLS-1$
                    }
                } else {
                    LogManager.logDetail(LogConstants.CTX_DQP, requestID, "Parameters are not serializable - cache cannot be used for", cacheId); //$NON-NLS-1$
                }
            }
        } else {
            LogManager.logDetail(LogConstants.CTX_DQP, requestID, "Result set caching is disabled."); //$NON-NLS-1$
        }
        try {
            request.processRequest();
        } finally {
            analysisRecord = request.analysisRecord;
        }
        originalCommand = request.userCommand;
        if (cachable && (requestMsg.useResultSetCache() || originalCommand.getCacheHint() != null) && rsCache != null && originalCommand.areResultsCachable()) {
            this.cid = cacheId;
            //turn on the collection of data objects used
            request.processor.getContext().setDataObjects(new HashSet<Object>(4));
        }
        request.processor.getContext().setWorkItem(this);
        processor = request.processor;
        planningEnd = System.currentTimeMillis();
        this.dqpCore.logMMCommand(this, Event.PLAN, null, null);
        collector = new BatchCollector(processor, processor.getBufferManager(), this.request.context, isForwardOnly()) {

            int maxRows = 0;

            @Override
            protected void flushBatchDirect(TupleBatch batch, boolean add) throws TeiidComponentException,TeiidProcessingException {
                resultsBuffer = getTupleBuffer();
                if (maxRows == 0) {
                    maxRows = OUTPUT_BUFFER_MAX_BATCHES * resultsBuffer.getBatchSize();
                }
                if (cid != null) {
                    super.flushBatchDirect(batch, add);
                }
                synchronized (lobStreams) {
                    if (cid == null && resultsBuffer.isLobs()) {
                        super.flushBatchDirect(batch, false);
                    }
                    if (batch.getTerminationFlag()) {
                        done();
                    }
                    add = sendResultsIfNeeded(batch);
                    if (cid != null) {
                        return;
                    }
                    super.flushBatchDirect(batch, add);
                    if (!add && !processor.hasBuffer()) {
                        resultsBuffer.setRowCount(batch.getEndRow());
                    }
                    if (transactionState != TransactionState.ACTIVE && (requestMsg.getRequestOptions().isContinuous() || (useCallingThread && isForwardOnly()))) {
                        synchronized (this) {
                            if (resultsReceiver == null) {
                                throw BlockedException.block(requestID, "Blocking to allow asynch processing"); //$NON-NLS-1$
                            }
                        }
                        if (add && !returnsUpdateCount) {
                            throw new AssertionError("Should not add batch to buffer"); //$NON-NLS-1$
                        }
                    }
                    if (add) {
                        flowControl(batch);
                    }
                }
            }

            private void flowControl(TupleBatch batch)
                    throws BlockedException {
                if (processor.hasBuffer()
                        || batch.getTerminationFlag()
                        || transactionState == TransactionState.ACTIVE) {
                    return;
                }
                synchronized (this) {
                    if (!isForwardOnly() && resultsReceiver != null && begin > resultsBuffer.getRowCount()) {
                        return; //a valid request beyond the processed range
                    }
                }

                if (resultsBuffer.getManagedRowCount() < maxRows) {
                    return; //continue to buffer
                }

                int timeOut = 500;
                if (!connectorInfo.isEmpty()) {
                    if (explicitSourceClose) {
                        for (DataTierTupleSource ts : getConnectorRequests()) {
                            if (!ts.isExplicitClose()) {
                                timeOut = 100;
                                break;
                            }
                        }
                    } else {
                        timeOut = 100;
                    }
                }
                if (dqpCore.blockOnOutputBuffer(RequestWorkItem.this)) {
                    if (moreWorkTask != null) {
                        moreWorkTask.cancel(false);
                        moreWorkTask = null;
                    }
                    if (getThreadState() != ThreadState.MORE_WORK) {
                        //we schedule the work to ensure that an idle client won't just indefinitely hold resources
                        moreWorkTask = scheduleWork(timeOut);
                    }
                    throw BlockedException.block(requestID, "Blocking due to full results TupleBuffer", //$NON-NLS-1$
                            this.getTupleBuffer(), "rows", this.getTupleBuffer().getManagedRowCount(), "batch size", this.getTupleBuffer().getBatchSize()); //$NON-NLS-1$ //$NON-NLS-2$
                }
                if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
                    LogManager.logDetail(LogConstants.CTX_DQP, requestID, "Exceeding buffer limit since there are pending active plans or this is using the calling thread."); //$NON-NLS-1$
                }
            }
        };
        if (!request.addedLimit && this.requestMsg.getRowLimit() > 0 && this.requestMsg.getRowLimit() < Integer.MAX_VALUE) {
            //covers maxrows for commands that already have a limit, are prepared, or are a stored procedure
            this.collector.setRowLimit(this.requestMsg.getRowLimit());
            this.collector.setSaveLastRow(request.isReturingParams());
        }
        this.resultsBuffer = collector.getTupleBuffer();
        if (this.resultsBuffer == null) {
            //This is just a dummy result it will get replaced by collector source
            resultsBuffer = this.processor.getBufferManager().createTupleBuffer(this.originalCommand.getProjectedSymbols(), this.request.context.getConnectionId(), TupleSourceType.FINAL);
        } else if (this.requestMsg.getRequestOptions().isContinuous()) {
            //TODO: this is based upon continuous being an embedded connection otherwise we have to do something like
            //forcing inlining, but truncating or erroring over a given size (similar to odbc handling)
            resultsBuffer.removeLobTracking();
        }
        initTransactionState(request.transactionContext);
        if (requestMsg.isNoExec()) {
            doneProducingBatches();
            resultsBuffer.close();
            this.cid = null;
        }
        this.returnsUpdateCount = request.returnsUpdateCount;
        if (this.returnsUpdateCount && this.requestMsg.getRequestOptions().isContinuous()) {
            throw new IllegalStateException("Continuous requests are not allowed to be updates."); //$NON-NLS-1$
        }
        request = null;
    }

    private void initTransactionState(TransactionContext tc) {
        transactionContext = tc;
        if (this.transactionContext != null && this.transactionContext.getTransactionType() != Scope.NONE) {
            if (this.requestMsg.getRequestOptions().isContinuous()) {
                throw new IllegalStateException("Continuous requests are not allowed to be transactional."); //$NON-NLS-1$
            }
            this.transactionState = TransactionState.ACTIVE;
        }
    }

    private CacheHint getCacheHint() {
        if (requestMsg.getCommand() != null) {
            return ((Command)requestMsg.getCommand()).getCacheHint();
        }
        return QueryParser.getQueryParser().parseCacheHint(requestMsg.getCommandString());
    }

    private void addToCache() {
        if (!doneProducingBatches || cid == null) {
            return;
        }
        Determinism determinismLevel = processor.getContext().getDeterminismLevel();
        CachedResults cr = new CachedResults();
        cr.setCommand(originalCommand);
        cr.setResults(resultsBuffer, processor.getProcessorPlan());
        if (requestMsg.getRowLimit() > 0 && resultsBuffer.getRowCount() == requestMsg.getRowLimit() + (collector.isSaveLastRow()?1:0)) {
            cr.setRowLimit(requestMsg.getRowLimit());
        }
        if (originalCommand.getCacheHint() != null) {
            LogManager.logDetail(LogConstants.CTX_DQP, requestID, "Using cache hint", originalCommand.getCacheHint()); //$NON-NLS-1$
            if (originalCommand.getCacheHint().getMinRows() != null && resultsBuffer.getRowCount() <= originalCommand.getCacheHint().getMinRows()) {
                LogManager.logDetail(LogConstants.CTX_DQP, requestID, "Not caching result as there are fewer rows than needed", resultsBuffer.getRowCount()); //$NON-NLS-1$
                return;
            }
            resultsBuffer.setPrefersMemory(originalCommand.getCacheHint().isPrefersMemory());
            if (originalCommand.getCacheHint().getDeterminism() != null) {
                determinismLevel = originalCommand.getCacheHint().getDeterminism();
                LogManager.logTrace(LogConstants.CTX_DQP, new Object[] { "Cache hint modified the query determinism from ",processor.getContext().getDeterminismLevel(), " to ", determinismLevel }); //$NON-NLS-1$ //$NON-NLS-2$
            }
            //if not updatable, then remove the access info
            if (!originalCommand.getCacheHint().isUpdatable(true)) {
                cr.getAccessInfo().setSensitiveToMetadataChanges(false);
                cr.getAccessInfo().getObjectsAccessed().clear();
            }
        }

        if (determinismLevel.compareTo(Determinism.SESSION_DETERMINISTIC) <= 0) {
            LogManager.logInfo(LogConstants.CTX_DQP, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30008, originalCommand));
        }
        try {
            this.resultsBuffer.persistLobs();
        } catch (TeiidException e) {
            LogManager.logDetail(LogConstants.CTX_DQP, e, QueryPlugin.Util.getString("failed_to_cache")); //$NON-NLS-1$
        }
        dqpCore.getRsCache().put(cid, determinismLevel, cr, originalCommand.getCacheHint() != null?originalCommand.getCacheHint().getTtl():null);
    }

    public SessionAwareCache<CachedResults> getRsCache() {
        return dqpCore.getRsCache();
    }

    /**
     * Send results if they have been requested.  This should only be called from the processing thread.
     * @return true if the batch should be buffered
     */
    protected boolean sendResultsIfNeeded(TupleBatch batch) throws TeiidComponentException, TeiidProcessingException {
        ResultsMessage response = null;
        ResultsReceiver<ResultsMessage> receiver = null;
        boolean result = true;
        synchronized (this) {
            if (this.resultsReceiver == null) {
                if (cursorRequestExpected()) {
                    if (batch != null) {
                        throw new AssertionError("batch has no handler"); //$NON-NLS-1$
                    }
                    throw BlockedException.block(requestID, "Blocking until client is ready"); //$NON-NLS-1$
                }
                return result;
            }
            if (!this.requestMsg.getRequestOptions().isContinuous()) {
                if ((this.begin > (batch != null?batch.getEndRow():this.resultsBuffer.getRowCount()) && !doneProducingBatches)
                        || (this.transactionState == TransactionState.ACTIVE) || (returnsUpdateCount && !doneProducingBatches)) {
                    return result;
                }

                if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
                    LogManager.logDetail(LogConstants.CTX_DQP, "[RequestWorkItem.sendResultsIfNeeded] requestID:", requestID, "resultsID:", this.resultsBuffer, "done:", doneProducingBatches );   //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                }

                boolean fromBuffer = false;
                int count = this.end - this.begin + 1;
                if (returnsUpdateCount) {
                    count = Integer.MAX_VALUE;
                }
                if (batch == null || !(batch.containsRow(this.begin) || (batch.getTerminationFlag() && batch.getEndRow() <= this.begin))) {
                    if (savedBatch != null && savedBatch.containsRow(this.begin)) {
                        batch = savedBatch;
                    } else {
                        batch = resultsBuffer.getBatch(begin);
                        //fetch more than 1 batch from the buffer
                        boolean first = true;
                        int rowSize = resultsBuffer.getRowSizeEstimate();
                        int batches = CLIENT_FETCH_MAX_BATCHES;
                        if (rowSize > 0) {
                            int totalSize = rowSize * resultsBuffer.getBatchSize();
                            if (schemaSize == 0) {
                                schemaSize = this.dqpCore.getBufferManager().getSchemaSize(this.originalCommand.getProjectedSymbols());
                            }
                            int multiplier = schemaSize/totalSize;
                            if (multiplier > 1) {
                                batches *= multiplier;
                            }
                        }
                        if (returnsUpdateCount) {
                            batches = Integer.MAX_VALUE;
                        }
                        for (int i = 1; i < batches && batch.getRowCount() + resultsBuffer.getBatchSize() <= count && !batch.getTerminationFlag(); i++) {
                            TupleBatch next = resultsBuffer.getBatch(batch.getEndRow() + 1);
                            if (next.getRowCount() == 0) {
                                break;
                            }
                            if (first) {
                                first = false;
                                TupleBatch old = batch;
                                batch = new TupleBatch(batch.getBeginRow(), new ResizingArrayList<List<?>>(batch.getTuples()));
                                batch.setTermination(old.getTermination());
                            }
                            batch.getTuples().addAll(next.getTuples());
                            batch.setTermination(next.getTermination());
                        }
                    }
                    savedBatch = null;
                    fromBuffer = true;
                }
                if (batch.getRowCount() > count) {
                    long beginRow = isForwardOnly()?begin:Math.min(this.begin, batch.getEndRow() - count + 1);
                    long endRow = Math.min(beginRow + count - 1, batch.getEndRow());
                    boolean last = false;
                    if (endRow == batch.getEndRow()) {
                        last = batch.getTerminationFlag();
                    } else if (isForwardOnly()) {
                        savedBatch = batch;
                    }
                    List<List<?>> memoryRows = batch.getTuples();
                    batch = new TupleBatch(beginRow, memoryRows.subList((int)(beginRow - batch.getBeginRow()), (int)(endRow - batch.getBeginRow() + 1)));
                    batch.setTerminationFlag(last);
                } else if (!fromBuffer){
                    result = !isForwardOnly();
                }
            } else if (batch == null) {
                return result;
            } else {
                result = false;
            }
            long finalRowCount = (this.resultsBuffer.isFinal()&&!this.requestMsg.getRequestOptions().isContinuous())?this.resultsBuffer.getRowCount():(batch.getTerminationFlag()?batch.getEndRow():-1);
            if (batch.getBeginRow() > Integer.MAX_VALUE || batch.getEndRow() > Integer.MAX_VALUE) {
                throw new TeiidProcessingException(QueryPlugin.Event.TEIID31174, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31174));
            }
            response = createResultsMessage(batch.getTuples(), this.originalCommand.getProjectedSymbols());
            response.setFirstRow((int)batch.getBeginRow());
            if (batch.getTermination() == TupleBatch.ITERATION_TERMINATED) {
                response.setLastRow((int)batch.getEndRow() - 1);
            } else {
                response.setLastRow((int)batch.getEndRow());
            }
            response.setUpdateResult(this.returnsUpdateCount);
            if (this.returnsUpdateCount) {
                //batch updates can have special exceptions in addition to update count results
                Throwable t = this.processor.getContext().getBatchUpdateException();
                if (t != null) {
                    t = logError(t);
                    response.setException(t);
                }

                //swap the result for the generated keys
                if (this.processor.getContext().getReturnAutoGeneratedKeys() != null
                        && finalRowCount == 1
                        && this.processor.getContext().getGeneratedKeys() != null
                        && handleGeneratedKeys(response)) {
                    finalRowCount = response.getLastRow();
                } else if (finalRowCount == 0 && response.getException() == null) {
                    //anon block or other construct not setting an explicit update count
                    response.setResults(Arrays.asList(Arrays.asList(0)));
                    if (response.getColumnNames().length != 1) {
                        response.setColumnNames(new String[] {"Count"}); //$NON-NLS-1$
                        response.setDataTypes(new String[] {"integer"}); //$NON-NLS-1$
                    }
                    finalRowCount = 1;
                }
            }
            // set final row
            response.setFinalRow((int)Math.min(finalRowCount, Integer.MAX_VALUE));
            if (response.getLastRow() == finalRowCount) {
                response.setDelayDeserialization(false);
            }

            setWarnings(response);

            // If it is stored procedure, set parameters
            if (originalCommand instanceof StoredProcedure) {
                StoredProcedure proc = (StoredProcedure)originalCommand;
                if (proc.returnParameters()) {
                    response.setParameters(getParameterInfo(proc));
                }
            }
            /*
             * mark the results sent at this point.
             * communication exceptions will be treated as non-recoverable
             */
            receiver = this.resultsReceiver;
            this.resultsReceiver = null;
        }
        cancelCancelTask();
        if ((!this.dqpWorkContext.getSession().isEmbedded() && requestMsg.isDelaySerialization() && this.requestMsg.getShowPlan() == ShowPlan.ON)
                || this.requestMsg.getShowPlan() == ShowPlan.DEBUG
                || LogManager.isMessageToBeRecorded(LogConstants.CTX_COMMANDLOGGING, MessageLevel.TRACE)) {
            int bytes;
            try {
                boolean keep = !this.dqpWorkContext.getSession().isEmbedded() && requestMsg.isDelaySerialization();
                bytes = response.serialize(keep);
                if (keep) {
                    response.setDelayDeserialization(true);
                }
                dataBytes.addAndGet(bytes);
                LogManager.logDetail(LogConstants.CTX_DQP, "Sending results for", requestID, "start row", //$NON-NLS-1$ //$NON-NLS-2$
                        response.getFirstRow(), "end row", response.getLastRow(), bytes, "bytes"); //$NON-NLS-1$ //$NON-NLS-2$
            } catch (Exception e) {
                //do nothing.  there is a low level serialization error that we will let happen
                //later since it would be inconvenient here
            }
        }
        setAnalysisRecords(response);
        receiver.receiveResults(response);
        return result;
    }

    private boolean handleGeneratedKeys(ResultsMessage response) throws QueryMetadataException, TeiidComponentException {
        GeneratedKeysImpl keys = this.processor.getContext().getGeneratedKeys();
        if (keys.getKeys().isEmpty()) {
            return false;
        }
        List<ElementSymbol> keyCols = this.processor.getContext().getReturnAutoGeneratedKeys();
        //match the key cols with the result
        ElementSymbol col = keyCols.get(0);

        String[] columnNames = keys.getColumnNames();

        if (keyCols.size() != columnNames.length) {
            return false;
        }

        if (!col.getGroupSymbol().isTempTable()
                && this.processor.getContext().getMetadata().isVirtualGroup(col.getGroupSymbol().getMetadataID())) {
            if (keyCols.size() != 1 && ((Insert)originalCommand).getUpdateInfo().isInherentInsert()) {
                //TODO: we need to ensure the column names line up correctly
                return false;
            }
            columnNames = new String[columnNames.length];
            columnNames[0] = col.getShortName();
        }

        response.setColumnNames(columnNames);
        String[] dataTypes = new String[columnNames.length];
        for(int i=0; i<dataTypes.length; i++) {
            dataTypes[i] = DataTypeManager.getDataTypeName(keys.getColumnTypes()[i]);
        }
        response.setUpdateCount((Integer)response.getResultsList().get(0).get(0));
        response.setDataTypes(dataTypes);
        response.setResults(keys.getKeys());
        response.setLastRow(keys.getKeys().size());
        return true;
    }

    private boolean cursorRequestExpected() {
        return this.transactionState != TransactionState.ACTIVE && (requestMsg.getRequestOptions().isContinuous() || (useCallingThread && isForwardOnly()));
    }

    private void setWarnings(ResultsMessage response) {
        boolean sanitize = this.options.isSanitizeMessages() && !LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL);

        // send any warnings with the response object
        List<Throwable> responseWarnings = new ArrayList<Throwable>();
        if (this.processor != null) {
            List<Exception> currentWarnings = processor.getAndClearWarnings();
            if (currentWarnings != null) {
                if (sanitize) {
                    for (Exception e : currentWarnings) {
                        responseWarnings.add(ExceptionUtil.sanitize(e, false));
                    }
                } else {
                    responseWarnings.addAll(currentWarnings);
                }
            }
        }
        response.setWarnings(responseWarnings);
    }

    public ResultsMessage createResultsMessage(List<? extends List<?>> batch, List<? extends Expression> columnSymbols) {
        String[] columnNames = new String[columnSymbols.size()];
        String[] dataTypes = new String[columnSymbols.size()];

        boolean pgColumnNames = Boolean.TRUE.equals(this.dqpWorkContext.getSession().getSessionVariables().get("pg_column_names")); //$NON-NLS-1$

        byte clientSerializationVersion = this.dqpWorkContext.getClientVersion().getClientSerializationVersion();
        for(int i=0; i<columnSymbols.size(); i++) {
            Expression symbol = columnSymbols.get(i);
            String name = MetaDataProcessor.getColumnName(pgColumnNames, symbol);
            columnNames[i] = name;
            dataTypes[i] = DataTypeManager.getDataTypeName(symbol.getType());
            if (!this.dqpWorkContext.getSession().isEmbedded()) {
                dataTypes[i] = BatchSerializer.getClientSafeType(dataTypes[i], clientSerializationVersion);
            }
        }
        ResultsMessage result = new ResultsMessage(batch, columnNames, dataTypes);

        result.setClientSerializationVersion(clientSerializationVersion);
        result.setDelayDeserialization(this.requestMsg.isDelaySerialization() && this.originalCommand.returnsResultSet());
        return result;
    }

    private void setAnalysisRecords(ResultsMessage response) {
        if(analysisRecord != null) {
            if (requestMsg.getShowPlan() != ShowPlan.OFF) {
                if (processor != null) {
                    PlanNode node = getQueryPlan();
                    response.setPlanDescription(node);
                }
                if (analysisRecord.getAnnotations() != null && !analysisRecord.getAnnotations().isEmpty()) {
                    response.setAnnotations(analysisRecord.getAnnotations());
                    analysisRecord.getAnnotations().clear();
                }
            }
            if (requestMsg.getShowPlan() == ShowPlan.DEBUG) {
                response.setDebugLog(analysisRecord.getDebugLog());
                analysisRecord.stopDebugLog();
            }
        }
    }

    public PlanNode getQueryPlan() {
        PlanNode node = processor.getProcessorPlan().getDescriptionProperties();
        node.addProperty(AnalysisRecord.PROP_DATA_BYTES_SENT, String.valueOf(dataBytes.get()));
        if (planningEnd != 0) {
            node.addProperty(AnalysisRecord.PROP_PLANNING_TIME, String.valueOf(planningEnd - planningStart));
        }
        return node;
    }

    private void sendError() {
        ResultsReceiver<ResultsMessage> receiver = null;
        synchronized (this) {
            receiver = this.resultsReceiver;
            this.resultsReceiver = null;
            if (receiver == null) {
                LogManager.logDetail(LogConstants.CTX_DQP, processingException, "Unable to send error to client as results were already sent.", requestID); //$NON-NLS-1$
                return;
            }
        }
        LogManager.logDetail(LogConstants.CTX_DQP, processingException, "Sending error to client", requestID); //$NON-NLS-1$
        ResultsMessage response = new ResultsMessage();
        Throwable exception = this.processingException;
        if (this.options.isSanitizeMessages() && !LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
            //we still convey an exception hierarchy here because the client logic looks for certian exception types
            exception = ExceptionUtil.sanitize(exception, false);
        }
        if (isCanceled) {
            exception = addCancelCode(exception);
        }
        setWarnings(response);
        response.setException(exception);
        setAnalysisRecords(response);
        receiver.receiveResults(response);
    }

    private Throwable addCancelCode(Throwable exception) {
        String reason = null;
        synchronized (this) {
            reason = this.cancelReason;
        }
        if (exception instanceof TeiidException) {
            TeiidException te = (TeiidException)exception;
            if (SQLStates.QUERY_CANCELED.equals(te.getCode()) && EquivalenceUtil.areEqual(reason, te.getMessage())) {
                return exception;
            }
        }
        TeiidProcessingException tpe = new TeiidProcessingException(reason);
        tpe.initCause(exception);
        tpe.setCode(SQLStates.QUERY_CANCELED);
        return tpe;
    }

    private static List<ParameterInfo> getParameterInfo(StoredProcedure procedure) {
        List<ParameterInfo> paramInfos = new ArrayList<ParameterInfo>();

        for (SPParameter param : procedure.getParameters()) {
            ParameterInfo info = new ParameterInfo(param.getParameterType(), param.getResultSetColumns().size());
            paramInfos.add(info);
        }

        return paramInfos;
    }

    public void processLobChunkRequest(String id, int streamRequestId, ResultsReceiver<LobChunk> chunckReceiver) {
        LobWorkItem workItem = null;
        synchronized (lobStreams) {
            workItem = this.lobStreams.get(streamRequestId);
            if (workItem == null) {
                workItem = new LobWorkItem(this, dqpCore, id, streamRequestId);
                lobStreams.put(streamRequestId, workItem);
            }
        }
        workItem.setResultsReceiver(chunckReceiver);
        if (this.dqpWorkContext.useCallingThread()) {
            workItem.run();
        } else {
            dqpCore.addWork(workItem);
        }
    }

    public void removeLobStream(int streamRequestId) {
        this.lobStreams.remove(streamRequestId);
    }

    public boolean requestCancel(String reason) throws TeiidComponentException {
        synchronized (this) {
            if (this.isCanceled || this.closeRequested) {
                LogManager.logDetail(LogConstants.CTX_DQP, "Ignoring cancel for", requestID, "since request is already cancelled/closed"); //$NON-NLS-1$ //$NON-NLS-2$
                return false;
            }
            this.isCanceled = true;
            this.cancelReason = QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30563, requestID, reason);
        }
        LogManager.logDetail(LogConstants.CTX_DQP, cancelReason);
        if (this.processor != null) {
            this.processor.requestCanceled();
        }

        // Cancel Connector atomic requests
        try {
            for (DataTierTupleSource connectorRequest : getConnectorRequests()) {
                connectorRequest.cancelRequest();
            }
        } finally {
            try {
                if (transactionService != null) {
                    try {
                        transactionService.cancelTransactions(requestID.getConnectionID(), true);
                    } catch (XATransactionException err) {
                         throw new TeiidComponentException(QueryPlugin.Event.TEIID30544, err);
                    }
                }
            } finally {
                this.moreWork();
            }
        }
        return true;
    }

    public void requestClose() throws TeiidComponentException {
        if (this.state == ProcessingState.CLOSE || this.closeRequested) {
            if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
                LogManager.logDetail(LogConstants.CTX_DQP, "Request already closing" + requestID); //$NON-NLS-1$
            }
            return;
        }
        if (!this.doneProducingBatches) {
            this.requestCancel("To immediately halt work as close was called"); //$NON-NLS-1$ //pending work should be canceled for fastest clean up
        } else {
            //it's safe to transition directly to close
            this.state = ProcessingState.CLOSE;
        }
        this.closeRequested = true;
        this.doMoreWork();
    }

    public boolean isCloseRequested() {
        return closeRequested;
    }

    public void requestMore(int batchFirst, int batchLast, ResultsReceiver<ResultsMessage> receiver) {
        if (span != null) {
            span.log("requested more results"); //$NON-NLS-1$
        }
        this.requestResults(batchFirst, batchLast, receiver);
        this.doMoreWork();
    }

    public void closeAtomicRequest(AtomicRequestID atomicRequestId) {
        connectorInfo.remove(atomicRequestId);
        LogManager.logTrace(LogConstants.CTX_DQP, new Object[] {"closed atomic-request:", atomicRequestId});  //$NON-NLS-1$
    }

    public void addConnectorRequest(AtomicRequestID atomicRequestId, DataTierTupleSource connInfo) {
        this.explicitSourceClose |= connInfo.isExplicitClose();
        connectorInfo.put(atomicRequestId, connInfo);
    }

    boolean isCanceled() {
        return isCanceled;
    }

    Command getOriginalCommand() throws TeiidProcessingException {
        if (this.originalCommand == null) {
            if (this.processingException != null) {
                 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30545, this.processingException);
            }
            throw new IllegalStateException("Original command is not available"); //$NON-NLS-1$
        }
        return this.originalCommand;
    }

    void setOriginalCommand(Command originalCommand) {
        this.originalCommand = originalCommand;
    }

    TransactionContext getTransactionContext() {
        return transactionContext;
    }

    Collection<DataTierTupleSource> getConnectorRequests() {
        synchronized (this.connectorInfo) {
            return new ArrayList<DataTierTupleSource>(this.connectorInfo.values());
        }
    }

    DataTierTupleSource getConnectorRequest(AtomicRequestID id) {
        return this.connectorInfo.get(id);
    }

    @Override
    public String toString() {
        return this.requestID.toString();
    }

    public DQPWorkContext getDqpWorkContext() {
        return dqpWorkContext;
    }

    public long getProcessingTimestamp() {
        return processingTimestamp;
    }

    private void done() {
        doneProducingBatches();
        addToCache();
        //TODO: we could perform more tracking to know what source lobs are in use
        if (this.resultsBuffer.getLobCount() == 0) {
            for (DataTierTupleSource connectorRequest : getConnectorRequests()) {
                connectorRequest.fullyCloseSource();
            }
        }
    }

    private void doneProducingBatches() {
        this.doneProducingBatches = true;
        synchronized (queue) {
            queue.clear();
        }
        dqpCore.finishProcessing(this);
    }

    @Override
    public int getPriority() {
        return (closeRequested || isCanceled) ? 0 : 1000;
    }

    @Override
    public long getCreationTime() {
        return processingTimestamp;
    }

    public <T> FutureWork<T> addRequestWork(Callable<T> callable) {
        FutureWork<T> work = new FutureWork<T>(callable, 100);
        work.addCompletionListener(new CompletionListener<T>() {
            @Override
            public void onCompletion(FutureWork<T> future) {
                RequestWorkItem.this.moreWork();
            }
        });
        work.setRequestId(this.requestID.toString());
        dqpCore.addWork(work);
        return work;
    }

    <T> FutureWork<T> addWork(Callable<T> callable, CompletionListener<T> listener, int priority) {
        FutureWork<T> work = new FutureWork<T>(callable, priority);
        work.setRequestId(this.requestID.toString());
        WorkWrapper<T> wl = new WorkWrapper<T>(work);
        work.addCompletionListener(wl);
        work.addCompletionListener(listener);
        synchronized (queue) {
            if (totalThreads < dqpCore.getUserRequestSourceConcurrency()) {
                dqpCore.addWork(work);
                totalThreads++;
                wl.submitted = true;
            } else {
                queue.add(wl);
                LogManager.logDetail(LogConstants.CTX_DQP, this.requestID, " reached max source concurrency of ", dqpCore.getUserRequestSourceConcurrency()); //$NON-NLS-1$
            }
        }
        return work;
    }

    public Future<Void> scheduleWork(long delay) {
        return dqpCore.scheduleWork(new MoreWorkTask(this), delay);
    }

    public void setCancelTask(Task cancelTask) {
        this.cancelTask = cancelTask;
    }

    public QueryProcessor getProcessor() {
        return processor;
    }

    public RequestID getRequestID() {
        return requestID;
    }

    public void setTracingSpan(Span span) {
        this.span = span;
    }

    public Span getTracingSpan() {
        return this.span;
    }

    public DQPCore getDqpCore() {
        return dqpCore;
    }

}