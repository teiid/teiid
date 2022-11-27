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

package org.teiid.query.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import javax.security.auth.Subject;

import org.teiid.CommandListener;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryProcessingException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.FileStoreInputStreamFactory;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.util.ArgCheck;
import org.teiid.core.util.ExecutorUtils;
import org.teiid.core.util.LRUCache;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.dqp.internal.process.AuthorizationValidator;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.internal.process.PreparedPlan;
import org.teiid.dqp.internal.process.RequestWorkItem;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.dqp.internal.process.SessionAwareCache.CacheID;
import org.teiid.dqp.internal.process.TupleSourceCache;
import org.teiid.dqp.message.RequestID;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.dqp.service.TransactionService;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.jdbc.LocalProfile;
import org.teiid.jdbc.TeiidConnection;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.net.ServerConnection;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.parser.ParseInfo;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.tempdata.GlobalTableStore;
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.translator.ReusableExecution;

/**
 * Defines the context that a command is processing in.  For example, this defines
 * who is processing the command and why.  Also, this class (or subclasses) provide
 * a means to pass context-specific information between users of the query processor
 * framework.
 */
public class CommandContext implements Cloneable, org.teiid.CommandContext {

    private static final int MAX_WARNINGS = 1000;

    private static ThreadLocal<LinkedList<CommandContext>> threadLocalContext = new ThreadLocal<LinkedList<CommandContext>>() {
        @Override
        protected LinkedList<CommandContext> initialValue() {
            return new LinkedList<CommandContext>();
        }
    };

    private static class VDBState {
        private String vdbName = ""; //$NON-NLS-1$
        private String vdbVersion = ""; //$NON-NLS-1$
        private QueryMetadataInterface metadata;
        private GlobalTableStore globalTables;
        private SessionMetadata session;
        private ClassLoader classLoader;
        private DQPWorkContext dqpWorkContext;
    }

    private static class LookupKey implements Comparable<LookupKey> {
        String matTableName;
        Comparable keyValue;

        public LookupKey(String matTableName, Object keyValue) {
            this.matTableName = matTableName;
            this.keyValue = (Comparable) keyValue;
        }

        @Override
        public int compareTo(LookupKey arg0) {
            int comp = matTableName.compareTo(arg0.matTableName);
            if (comp != 0) {
                return comp;
            }
            return keyValue.compareTo(arg0.keyValue);
        }
    }

    private static class GlobalState implements Cloneable {
        private WeakReference<RequestWorkItem> processorID;

        /** Identify a group of related commands, which typically get cleaned up together */
        private String connectionID;

        private int processorBatchSize = BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE;

        private String userName;

        private Serializable commandPayload;

        /** Indicate whether statistics should be collected for relational node processing*/
        private boolean collectNodeStatistics;

        private Random random = null;

        private TimeZone timezone = TimeZone.getDefault();

        private QueryProcessor.ProcessorFactory queryProcessorFactory;

        private Set<String> groups;
        private Map<String, String> aliasMapping;

        private long timeSliceEnd = Long.MAX_VALUE;

        private long timeoutEnd = Long.MAX_VALUE;

        private BufferManager bufferManager;

        private SessionAwareCache<PreparedPlan> planCache;

        private boolean resultSetCacheEnabled = true;

        private int userRequestSourceConcurrency;
        private Subject subject;
        private HashSet<Object> dataObjects;

        private RequestID requestId;

        private TransactionContext transactionContext;
        private TransactionService transactionService;
        private Executor executor = ExecutorUtils.getDirectExecutor();
        Map<Object, List<ReusableExecution<?>>> reusableExecutions;
        Set<CommandListener> commandListeners = null;
        private LRUCache<String, DecimalFormat> decimalFormatCache;
        private LRUCache<String, SimpleDateFormat> dateFormatCache;
        private LRUCache<Entry<String,Integer>, Pattern> patternCache;
        private AtomicLong reuseCount = null;

        private List<Exception> warnings = null;

        private Options options = null;
        private List<ElementSymbol> returnAutoGeneratedKeys;
        private GeneratedKeysImpl generatedKeys;
        private long reservedBuffers;

        private AuthorizationValidator authorizationValidator;

        private Map<LookupKey, TupleSource> lookups;
        private TempTableStore sessionTempTableStore;

        private Set<FileStoreInputStreamFactory> created = Collections.newSetFromMap(new WeakHashMap<FileStoreInputStreamFactory, Boolean>());

        private LRUCache<AbstractMetadataRecord, Boolean> accessible;

        private Throwable batchUpdateException;

        public boolean parallel;

        private long timestamp = System.currentTimeMillis();

        private boolean readOnly = true;
    }

    private GlobalState globalState = new GlobalState();

    private VariableContext variableContext = new VariableContext();
    private TempTableStore tempTableStore;
    private LinkedList<String> recursionStack;
    private boolean nonBlocking;
    private HashSet<Object> planningObjects;
    private HashSet<Object> dataObjects = this.globalState.dataObjects;
    private TupleSourceCache tupleSourceCache;
    private VDBState vdbState = new VDBState();
    private Determinism[] determinismLevel = new Determinism[] {Determinism.DETERMINISTIC};
    private AtomicBoolean cancelled = new AtomicBoolean();
    private AtomicBoolean parentCancelled;

    private Long currentTimestamp;
    private boolean atomicBlock;

    private Collection<TempMetadataID> accessed;

    /**
     * Construct a new context.
     */
    public CommandContext(String connectionID, String userName, Serializable commandPayload,
        String vdbName, Object vdbVersion, boolean collectNodeStatistics) {
        setConnectionID(connectionID);
        setUserName(userName);
        setCommandPayload(commandPayload);
        setVdbName(vdbName);
        setVdbVersion(vdbVersion.toString());
        setCollectNodeStatistics(collectNodeStatistics);
    }

    /**
     * Construct a new context.
     */
    public CommandContext(Object processorID, String connectionID, String userName,
        String vdbName, Object vdbVersion) {

        this(connectionID, userName, null, vdbName, vdbVersion,
            false);

    }

    public CommandContext() {
    }

    private CommandContext(GlobalState state) {
        this.globalState = state;
        this.dataObjects = this.globalState.dataObjects;
    }

    public Determinism getDeterminismLevel() {
        return determinismLevel[0];
    }

    public Determinism resetDeterminismLevel(boolean detach) {
        Determinism result = determinismLevel[0];
        if (detach) {
            determinismLevel = new Determinism[1];
        }
        determinismLevel[0] = Determinism.DETERMINISTIC;
        return result;

    }

    public Determinism resetDeterminismLevel() {
        return resetDeterminismLevel(false);
    }

    public void setDeterminismLevel(Determinism level) {
        if (determinismLevel[0] == null || level.compareTo(determinismLevel[0]) < 0) {
            determinismLevel[0] = level;
        }
    }

    /**
     * @return
     */
    public RequestWorkItem getWorkItem() {
        if (globalState.processorID == null) {
            return null;
        }
        return globalState.processorID.get();
    }

    /**
     * @param object
     */
    public void setWorkItem(RequestWorkItem object) {
        ArgCheck.isNotNull(object);
        globalState.processorID = new WeakReference<RequestWorkItem>(object);
    }

    public CommandContext clone() {
        CommandContext clone = new CommandContext(this.globalState);
        clone.variableContext = this.variableContext;
        clone.tempTableStore = this.tempTableStore;
        if (this.recursionStack != null) {
            clone.recursionStack = new LinkedList<String>(this.recursionStack);
        }
        clone.setNonBlocking(this.nonBlocking);
        clone.tupleSourceCache = this.tupleSourceCache;
        clone.vdbState = this.vdbState;
        clone.determinismLevel = this.determinismLevel;
        if (this.parentCancelled != null) {
            clone.parentCancelled = this.parentCancelled;
        } else {
            clone.parentCancelled = this.cancelled;
        }
        clone.currentTimestamp = this.currentTimestamp;
        return clone;
    }

    public void setNewVDBState(DQPWorkContext newWorkContext) {
        this.vdbState = new VDBState();
        VDBMetaData vdb = newWorkContext.getVDB();
        GlobalTableStore actualGlobalStore = vdb.getAttachment(GlobalTableStore.class);
        this.vdbState.globalTables = actualGlobalStore;
        this.vdbState.session = newWorkContext.getSession();
        this.vdbState.classLoader = vdb.getAttachment(ClassLoader.class);
        this.vdbState.vdbName = vdb.getName();
        this.vdbState.vdbVersion = vdb.getVersion();
        this.vdbState.dqpWorkContext = newWorkContext;
        TempMetadataAdapter metadata = new TempMetadataAdapter(vdb.getAttachment(QueryMetadataInterface.class), globalState.sessionTempTableStore.getMetadataStore());
        metadata.setSession(true);
        this.vdbState.metadata = metadata;
    }

    public String toString() {
        return "CommandContext: " + globalState.processorID; //$NON-NLS-1$
    }

    public String getConnectionId() {
        return globalState.connectionID;
    }

    public String getConnectionID() {
        return globalState.connectionID;
    }

    public String getUserName() {
        return globalState.userName;
    }

    public String getVdbName() {
        return vdbState.vdbName;
    }

    public String getVdbVersion() {
        return vdbState.vdbVersion;
    }

    /**
     * Sets the connectionID.
     * @param connectionID The connectionID to set
     */
    public void setConnectionID(String connectionID) {
        this.globalState.connectionID = connectionID;
    }

    /**
     * Sets the userName.
     * @param userName The userName to set
     */
    public void setUserName(String userName) {
        this.globalState.userName = userName;
    }

    /**
     * Sets the vdbName.
     * @param vdbName The vdbName to set
     */
    public void setVdbName(String vdbName) {
        this.vdbState.vdbName = vdbName;
    }

    /**
     * Sets the vdbVersion.
     * @param vdbVersion The vdbVersion to set
     */
    public void setVdbVersion(Object vdbVersion) {
        this.vdbState.vdbVersion = vdbVersion.toString();
    }

    public Serializable getCommandPayload() {
        return this.globalState.commandPayload;
    }
    public void setCommandPayload(Serializable commandPayload) {
        this.globalState.commandPayload = commandPayload;
    }

    /**
     * @param collectNodeStatistics The collectNodeStatistics to set.
     * @since 4.2
     */
    public void setCollectNodeStatistics(boolean collectNodeStatistics) {
        this.globalState.collectNodeStatistics = collectNodeStatistics;
    }

    public boolean getCollectNodeStatistics() {
        return this.globalState.collectNodeStatistics;
    }

    @Override
    public int getProcessorBatchSize() {
        return this.globalState.processorBatchSize;
    }

    public int getProcessorBatchSize(List<Expression> schema) {
        return this.globalState.bufferManager.getProcessorBatchSize(schema);
    }

    public void setProcessorBatchSize(int processorBatchSize) {
        this.globalState.processorBatchSize = processorBatchSize;
    }

    public double getNextRand() {
        if (globalState.random == null) {
            globalState.random = new Random();
        }
        return globalState.random.nextDouble();
    }

    public double getNextRand(long seed) {
        if (globalState.random == null) {
            globalState.random = new Random();
        }
        globalState.random.setSeed(seed);
        return globalState.random.nextDouble();
    }

    void setRandom(Random random) {
        this.globalState.random = random;
    }

    public void pushCall(String value) throws QueryProcessingException {
        if (recursionStack == null) {
            recursionStack = new LinkedList<String>();
        } else if (recursionStack.contains(value)) {
             throw new QueryProcessingException(QueryPlugin.Event.TEIID30347, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30347, value));
        }

        recursionStack.push(value);
    }

    public int getCallStackDepth() {
        if (this.recursionStack == null) {
            return 0;
        }
        return this.recursionStack.size();
    }

    public void popCall() {
        if (recursionStack != null) {
            recursionStack.pop();
        }
    }

    public void setAuthoriziationValidator(AuthorizationValidator authorizationValidator) {
        this.globalState.authorizationValidator = authorizationValidator;
    }

    public TempTableStore getTempTableStore() {
        return tempTableStore;
    }

    public void setTempTableStore(TempTableStore tempTableStore) {
        this.tempTableStore = tempTableStore;
        if (globalState.sessionTempTableStore == null) {
            globalState.sessionTempTableStore = tempTableStore;
        }
    }

    public TempTableStore getSessionTempTableStore() {
        return globalState.sessionTempTableStore;
    }

    public void setSessionTempTableStore(TempTableStore tempTableStore) {
        this.globalState.sessionTempTableStore = tempTableStore;
    }

    public TimeZone getServerTimeZone() {
        return globalState.timezone;
    }

    public QueryProcessor.ProcessorFactory getQueryProcessorFactory() {
        return this.globalState.queryProcessorFactory;
    }

    public void setQueryProcessorFactory(QueryProcessor.ProcessorFactory queryProcessorFactory) {
        this.globalState.queryProcessorFactory = queryProcessorFactory;
    }

    public VariableContext getVariableContext() {
        return variableContext;
    }

    public void setVariableContext(VariableContext variableContext) {
        this.variableContext = variableContext;
    }

    public void pushVariableContext(VariableContext toPush) {
        toPush.setParentContext(this.variableContext);
        this.variableContext = toPush;
    }

    public Object getFromContext(Expression expression) throws TeiidComponentException {
        if (variableContext == null || !(expression instanceof ElementSymbol)) {
            throw new TeiidComponentException(QueryPlugin.Event.TEIID30328, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30328, expression, QueryPlugin.Util.getString("Evaluator.no_value"))); //$NON-NLS-1$
        }
        Object value = variableContext.getValue(expression);
        if (value == null && !variableContext.containsVariable(expression)) {
            throw new TeiidComponentException(QueryPlugin.Event.TEIID30328, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30328, expression, QueryPlugin.Util.getString("Evaluator.no_value"))); //$NON-NLS-1$
        }
        return value;
    }

    public Set<String> getGroups() {
        if (globalState.groups == null) {
            globalState.groups = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
        }
        return globalState.groups;
    }

    public Map<String, String> getAliasMapping() {
        if (globalState.aliasMapping == null) {
            globalState.aliasMapping = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
        }
        return globalState.aliasMapping;
    }

    public long getTimeSliceEnd() {
        return globalState.timeSliceEnd;
    }

    public long getTimeoutEnd() {
        return globalState.timeoutEnd;
    }

    public void setTimeSliceEnd(long timeSliceEnd) {
        globalState.timeSliceEnd = timeSliceEnd;
    }

    public void setTimeoutEnd(long timeoutEnd) {
        globalState.timeoutEnd = timeoutEnd;
    }

    public void setMetadata(QueryMetadataInterface metadata) {
        vdbState.metadata = metadata;
    }

    public QueryMetadataInterface getMetadata() {
        return vdbState.metadata;
    }

    public BufferManager getBufferManager() {
        return globalState.bufferManager;
    }

    public void setBufferManager(BufferManager bm) {
        globalState.bufferManager = bm;
    }

    public GlobalTableStore getGlobalTableStore() {
        return vdbState.globalTables;
    }

    public void setGlobalTableStore(GlobalTableStore tempTableStore) {
        vdbState.globalTables = tempTableStore;
    }

    public boolean isNonBlocking() {
        return nonBlocking;
    }

    public void setNonBlocking(boolean nonBlocking) {
        this.nonBlocking = nonBlocking;
    }

    public void setPreparedPlanCache(SessionAwareCache<PreparedPlan> cache) {
        this.globalState.planCache = cache;
    }

    public PreparedPlan getPlan(String key) {
        if (this.globalState.planCache == null) {
            return null;
        }
        CacheID id = new CacheID(new ParseInfo(), key, getVdbName(), getVdbVersion(), getConnectionId(), getUserName());
        PreparedPlan pp = this.globalState.planCache.get(id);
        if (pp != null) {
            if (id.getSessionId() != null) {
                setDeterminismLevel(Determinism.USER_DETERMINISTIC);
            } else if (id.getUserName() != null) {
                setDeterminismLevel(Determinism.SESSION_DETERMINISTIC);
            }
            return pp;
        }
        return null;
    }

    public void putPlan(String key, PreparedPlan plan, Determinism determinismLevel) {
        if (this.globalState.planCache == null) {
            return;
        }
        CacheID id = new CacheID(new ParseInfo(), key, getVdbName(), getVdbVersion(), getConnectionId(), getUserName());
        this.globalState.planCache.put(id, determinismLevel, plan, null);
    }

    public boolean isResultSetCacheEnabled() {
        return globalState.resultSetCacheEnabled;
    }

    public void setResultSetCacheEnabled(boolean resultSetCacheEnabled) {
        this.globalState.resultSetCacheEnabled = resultSetCacheEnabled;
    }

    public int getUserRequestSourceConcurrency() {
        return this.globalState.userRequestSourceConcurrency;
    }

    public void setUserRequestSourceConcurrency(int userRequestSourceConcurrency) {
        this.globalState.userRequestSourceConcurrency = userRequestSourceConcurrency;
    }

    @Override
    public Subject getSubject() {
        return this.globalState.subject;
    }

    public void setSubject(Subject subject) {
        this.globalState.subject = subject;
    }

    public void accessedPlanningObject(Object id) {
        if (this.planningObjects == null) {
            this.planningObjects = new HashSet<Object>();
        }
        this.planningObjects.add(id);
    }

    public Set<Object> getPlanningObjects() {
        if (this.planningObjects == null) {
            return Collections.emptySet();
        }
        return planningObjects;
    }

    public void accessedDataObject(Object id) {
        if (this.dataObjects != null) {
            this.dataObjects.add(id);
        }
    }

    public Set<Object> getDataObjects() {
        return dataObjects;
    }

    public void setDataObjects(HashSet<Object> dataObjectsAccessed) {
        this.dataObjects = dataObjectsAccessed;
    }

    @Override
    public SessionMetadata getSession() {
        return this.vdbState.session;
    }

    public void setSession(SessionMetadata session) {
        this.vdbState.session = session;
    }

    @Override
    public String getRequestId() {
        return this.globalState.requestId != null ? this.globalState.requestId.toString() : null;
    }

    public void setRequestId(RequestID requestId) {
        this.globalState.requestId = requestId;
    }

    public void setDQPWorkContext(DQPWorkContext workContext) {
        this.vdbState.dqpWorkContext = workContext;
    }

    @Override
    public Map<String, DataPolicy> getAllowedDataPolicies() {
        if (this.vdbState.dqpWorkContext == null) {
            return null;
        }
        return this.vdbState.dqpWorkContext.getAllowedDataPolicies();
    }

    @Override
    public VDBMetaData getVdb() {
        if (this.vdbState.dqpWorkContext == null) {
            return null;
        }
        return this.vdbState.dqpWorkContext.getVDB();
    }

    public DQPWorkContext getDQPWorkContext() {
        return this.vdbState.dqpWorkContext;
    }

    public TransactionContext getTransactionContext() {
        return globalState.transactionContext;
    }

    public void setTransactionContext(TransactionContext transactionContext) {
        globalState.transactionContext = transactionContext;
    }

    public TransactionService getTransactionServer() {
        return globalState.transactionService;
    }

    public void setTransactionService(TransactionService transactionService) {
        globalState.transactionService = transactionService;
    }

    public Executor getExecutor() {
        return this.globalState.executor;
    }

    /**
     * Submit work that will notify the request work item of more work when complete
     * @param callable
     * @return
     */
    public <V> Future<V> submit(Callable<V> callable) {
        if (getWorkItem() != null) {
            return getWorkItem().addRequestWork(callable);
        }
        return ForkJoinPool.commonPool().submit(callable);
    }

    public void setExecutor(Executor e) {
        this.globalState.executor = e;
    }

    public ReusableExecution<?> getReusableExecution(Object key) {
        synchronized (this.globalState) {
            if (this.globalState.reusableExecutions == null) {
                return null;
            }
            List<ReusableExecution<?>> reusableExecutions = this.globalState.reusableExecutions.get(key);
            if (reusableExecutions != null && !reusableExecutions.isEmpty()) {
                return reusableExecutions.remove(0);
            }
            return null;
        }
    }

    public void putReusableExecution(Object key, ReusableExecution<?> execution) {
        synchronized (this.globalState) {
            if (this.globalState.reusableExecutions == null) {
                this.globalState.reusableExecutions = new HashMap<Object, List<ReusableExecution<?>>>();
            }
            List<ReusableExecution<?>> reusableExecutions = this.globalState.reusableExecutions.get(key);
            if (reusableExecutions == null) {
                reusableExecutions = new LinkedList<ReusableExecution<?>>();
                this.globalState.reusableExecutions.put(key, reusableExecutions);
            }
            reusableExecutions.add(execution);
        }
    }

    public void close() {
        synchronized (this.globalState) {
            if (this.globalState.reservedBuffers > 0) {
                long toRelease = this.globalState.reservedBuffers;
                this.globalState.reservedBuffers = 0;
                this.globalState.bufferManager.releaseOrphanedBuffers(toRelease);
            }
            if (this.globalState.reusableExecutions != null) {
                for (List<ReusableExecution<?>> reusableExecutions : this.globalState.reusableExecutions.values()) {
                    for (ReusableExecution<?> reusableExecution : reusableExecutions) {
                        try {
                            reusableExecution.dispose();
                        } catch (Exception e) {
                            LogManager.logWarning(LogConstants.CTX_DQP, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30030));
                        }
                    }
                }
                this.globalState.reusableExecutions.clear();
            }
            if (this.globalState.commandListeners != null) {
                for (CommandListener listener : this.globalState.commandListeners) {
                    try {
                        listener.commandClosed(this);
                    } catch (Exception e) {
                        LogManager.logWarning(LogConstants.CTX_DQP, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30031));
                    }
                }
                this.globalState.commandListeners.clear();
            }
            if (this.globalState.lookups != null) {
                for (TupleSource ts : this.globalState.lookups.values()) {
                    ts.closeSource();
                }
                this.globalState.lookups = null;
            }
            if (this.globalState.created != null) {
                for (FileStoreInputStreamFactory isf : this.globalState.created) {
                    if (isf.isTemporary()) {
                        //TODO: we could also elect to not free memory backed lobs
                        isf.free();
                    }
                }
                this.globalState.created.clear();
            }
        }
    }

    @Override
    public void addListener(CommandListener listener) {
        if (listener != null) {
            synchronized (this.globalState) {
                if (this.globalState.commandListeners == null) {
                    this.globalState.commandListeners = Collections.newSetFromMap(new IdentityHashMap<CommandListener, Boolean>());
                }
                this.globalState.commandListeners.add(listener);
            }
        }
    }

    @Override
    public void removeListener(CommandListener listener) {
        if (listener != null) {
            synchronized (this.globalState) {
                if (this.globalState.commandListeners != null) {
                    this.globalState.commandListeners.remove(listener);
                }
            }
        }
    }

    public static DecimalFormat getDecimalFormat(CommandContext context, String format) {
        DecimalFormat result = null;
        if (context != null) {
            if (context.globalState.decimalFormatCache == null) {
                context.globalState.decimalFormatCache = new LRUCache<String, DecimalFormat>(32);
            } else {
                result = context.globalState.decimalFormatCache.get(format);
            }
        }
        if (result == null) {
            result = new DecimalFormat(format); //TODO: could be locale sensitive
            result.setParseBigDecimal(true);
            if (context != null) {
                context.globalState.decimalFormatCache.put(format, result);
            }
        }
        return result;
    }

    public static SimpleDateFormat getDateFormat(CommandContext context, String format) {
        SimpleDateFormat result = null;
        if (context != null) {
            if (context.globalState.dateFormatCache == null) {
                context.globalState.dateFormatCache = new LRUCache<String, SimpleDateFormat>(32);
            } else {
                result = context.globalState.dateFormatCache.get(format);
            }
        }
        if (result == null) {
            result = new SimpleDateFormat(format); //TODO: could be locale sensitive
            if (context != null) {
                context.globalState.dateFormatCache.put(format, result);
            }
        }
        return result;
    }

    /**
     * Compile a regular expression into a {@link java.util.regex.Pattern} and cache it in
     * the {@link CommandContext} for future use.
     *
     * @param context
     * @param regex Regular expression.
     * @param flags Bitmask flags like {@link java.util.regex.Pattern#CASE_INSENSITIVE}.
     * @return Compiled regex.
     */
    public static Pattern getPattern(CommandContext context, String regex, int flags) {
        Pattern result = null;
        if (context != null) {
            if (context.globalState.patternCache == null) {
                context.globalState.patternCache = new LRUCache<Entry<String,Integer>,Pattern>(32);
            } else {
                result = context.globalState.patternCache.get(new SimpleEntry(result, flags));
            }
        }
        if (result == null) {
            result = Pattern.compile(regex, flags);
            if (context != null) {
                context.globalState.patternCache.put(new SimpleEntry(result, flags), result);
            }
        }
        return result;
    }

    public void incrementReuseCount() {
        globalState.reuseCount.getAndIncrement();
    }

    @Override
    public long getReuseCount() {
        if (globalState.reuseCount == null) {
            return 0;
        }
        return globalState.reuseCount.get();
    }

    @Override
    public boolean isContinuous() {
        return globalState.reuseCount != null;
    }

    public void setContinuous() {
        this.globalState.reuseCount = new AtomicLong();
    }

    @Override
    public ClassLoader getVDBClassLoader() {
        return this.vdbState.classLoader;
    }

    public void setVDBClassLoader(ClassLoader classLoader) {
        this.vdbState.classLoader = classLoader;
    }

    /**
     * Get all warnings found while processing this plan.  These warnings may
     * be detected throughout the plan lifetime, which means new ones may arrive
     * at any time.  This method returns all current warnings and clears
     * the current warnings list.  The warnings are in order they were detected.
     * @return Current list of warnings, never null
     */
    public List<Exception> getAndClearWarnings() {
        if (globalState.warnings == null) {
            return null;
        }
        synchronized (this.globalState) {
            List<Exception> copied = globalState.warnings;
            globalState.warnings = null;
            return copied;
        }
    }

    public void addWarning(Exception warning) {
        if (warning == null) {
            return;
        }
        synchronized (this.globalState) {
            if (globalState.warnings == null) {
                globalState.warnings = new ArrayList<Exception>(1);
            }
            globalState.warnings.add(warning);
            if (globalState.warnings.size() > MAX_WARNINGS) {
                globalState.warnings.remove(0);
            }
        }
        if (!this.getOptions().isSanitizeMessages() || LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
            LogManager.logInfo(LogConstants.CTX_DQP, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31105, warning.getMessage()));
        }
    }

    public TupleSourceCache getTupleSourceCache() {
        return tupleSourceCache;
    }

    public void setTupleSourceCache(TupleSourceCache tupleSourceCache) {
        this.tupleSourceCache = tupleSourceCache;
    }

    public Options getOptions() {
        if (this.globalState.options == null) {
            this.globalState.options = new Options();
        }
        return this.globalState.options;
    }

    public void setOptions(Options options) {
        this.globalState.options = options;
    }

    @Override
    public boolean isReturnAutoGeneratedKeys() {
        return true;
    }

    public void setReturnAutoGeneratedKeys(List<ElementSymbol> variables) {
        this.globalState.returnAutoGeneratedKeys = variables;
    }

    public List<ElementSymbol> getReturnAutoGeneratedKeys() {
        return this.globalState.returnAutoGeneratedKeys;
    }
    @Override
    public GeneratedKeysImpl returnGeneratedKeys(String[] columnNames,
            Class<?>[] columnDataTypes) {
        synchronized (this.globalState) {
            this.globalState.generatedKeys = new GeneratedKeysImpl(columnNames, columnDataTypes, this);
            return this.globalState.generatedKeys;
        }
    }

    public GeneratedKeysImpl getGeneratedKeys() {
        synchronized (this.globalState) {
            return this.globalState.generatedKeys;
        }
    }

    public static CommandContext getThreadLocalContext() {
        return threadLocalContext.get().peek();
    }

    public static void pushThreadLocalContext(CommandContext context) {
        threadLocalContext.get().push(context);
    }

    public static void popThreadLocalContext() {
        threadLocalContext.get().poll();
    }

    public long addAndGetReservedBuffers(int i) {
        synchronized (this.globalState) {
            return globalState.reservedBuffers += i;
        }
    }

    @Override
    public Object setSessionVariable(String key, Object value) {
        if (this.vdbState.session == null) {
            return null;
        }
        return this.vdbState.session.getSessionVariables().put(key, value);
    }

    @Override
    public Object getSessionVariable(String key) {
        return this.vdbState.session.getSessionVariables().get(key);
    }

    public AuthorizationValidator getAuthorizationValidator() {
        return this.globalState.authorizationValidator;
    }

    public TupleSource getCodeLookup(String matTableName, Object keyValue) {
        if (this.globalState.lookups != null) {
            return this.globalState.lookups.remove(new LookupKey(matTableName, keyValue));
        }
        return null;
    }

    public void putCodeLookup(String matTableName, Object keyValue, TupleSource ts) {
        if (this.globalState.lookups == null) {
            this.globalState.lookups = new TreeMap<LookupKey, TupleSource>();
        }
        this.globalState.lookups.put(new LookupKey(matTableName, keyValue), ts);
    }


    @Override
    public TeiidConnection getConnection() throws TeiidSQLException {
        LocalProfile ep = getDQPWorkContext().getConnectionProfile();
        //TODO: this is problematic as the client properties are not conveyed
        Properties info = new Properties();
        info.put(LocalProfile.DQP_WORK_CONTEXT, getDQPWorkContext());
        String url = "jdbc:teiid:" + getVdbName() + "." + getVdbVersion(); //$NON-NLS-1$ //$NON-NLS-2$
        ServerConnection sc;
        try {
            sc = ep.createServerConnection(info);
        } catch (TeiidException e) {
            throw TeiidSQLException.create(e);
        }
        return new ConnectionImpl(sc, info, url) {
            @Override
            public void close() throws SQLException {
                //just ignore
            }

            @Override
            public void rollback() throws SQLException {
                //just ignore
            }

            @Override
            public void setAutoCommit(boolean autoCommit) throws SQLException {
                //TODO: detect if attempted set conflicts with current txn state
                throw new TeiidSQLException();
            }

            @Override
            public void commit() throws SQLException {
                throw new TeiidSQLException();
            }

            @Override
            public void changeUser(String userName, String newPassword)
                    throws SQLException {
                throw new TeiidSQLException();
            }

            @Override
            protected synchronized long nextRequestID() {
                //need to choose ids that won't conflict with the user connection
                return -(long)(Math.random()*Long.MAX_VALUE);
            }
        };
    }

    /**
     * Used by the system table logic
     * @return
     */
    public Clob getSpatialRefSys() {
        return new ClobImpl(new InputStreamFactory() {

            @Override
            public InputStream getInputStream() throws IOException {
                return getClass().getClassLoader().getResourceAsStream("org/teiid/metadata/spatial_ref_sys.csv"); //$NON-NLS-1$
            }
        }, -1);
    }

    public void addCreatedLob(FileStoreInputStreamFactory isf) {
        if (this.globalState.created != null) {
            isf.setTemporary(true);
            this.globalState.created.add(isf);
        }
    }

    public void disableAutoCleanLobs() {
        this.globalState.created = null;
    }

    public void requestCancelled() {
        this.cancelled.set(true);
    }

    /**
     * Check if this context or the parent has been cancelled.
     * If the parent has been, then we'll propagate.
     * @return
     */
    public boolean isCancelled() {
        if (this.cancelled.get()) {
            return true;
        }
        if (this.parentCancelled != null && this.parentCancelled.get()) {
            return true;
        }
        return false;
    }

    public void clearGeneratedKeys() {
        synchronized (this.globalState) {
            this.globalState.generatedKeys = null;
        }
    }

    public Boolean isAccessible(AbstractMetadataRecord record) {
        if (this.globalState.accessible == null) {
            return null;
        }
        return this.globalState.accessible.get(record);
    }

    public void setAccessible(AbstractMetadataRecord record, Boolean result) {
        if (this.globalState.accessible == null) {
            this.globalState.accessible = new LRUCache<>(1000);
        }
        this.globalState.accessible.put(record, result);
    }

    public Throwable getBatchUpdateException() {
        return this.globalState.batchUpdateException;
    }

    public void setBatchUpdateException(Throwable t) {
        this.globalState.batchUpdateException = t;
    }

    public boolean isParallel() {
        return this.globalState.parallel;
    }

    public boolean setParallel(boolean value) {
        boolean result = this.globalState.parallel;
        this.globalState.parallel = value;
        return result;
    }

    public Date currentDate() {
        return TimestampWithTimezone.createDate(new Date(this.globalState.timestamp));
    }

    public Time currentTime() {
        return TimestampWithTimezone.createTime(new Date(this.globalState.timestamp));
    }

    public Timestamp currentTimestamp() {
        if (currentTimestamp != null) {
            return new Timestamp(this.currentTimestamp);
        }
        return new Timestamp(this.globalState.timestamp);
    }

    public void setCurrentTimestamp(long currentTimeMillis) {
        this.currentTimestamp = currentTimeMillis;
    }

    /**
     * Used by transaction detection logic when starting an atomic block
     * transaction
     * @return
     */
    public boolean isAtomicBlock() {
        return atomicBlock;
    }

    public void setAtomicBlock(boolean atomicBlock) {
        this.atomicBlock = atomicBlock;
    }

    /**
     * Used by the planner to track only what is accessed by the plan
     */
    public void addAccessed(TempMetadataID id) {
        if (this.accessed != null) {
            this.accessed.add(id);
        }
    }

    public void setAccessed(Collection<TempMetadataID> accessed) {
        this.accessed = accessed;
    }

    public boolean isReadOnly() {
        return this.globalState.readOnly;
    }

    public void setReadOnly(boolean b) {
        this.globalState.readOnly = b;
    }

}
