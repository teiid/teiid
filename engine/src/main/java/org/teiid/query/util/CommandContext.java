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

package org.teiid.query.util;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import javax.security.auth.Subject;

import org.teiid.CommandListener;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryProcessingException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.util.ArgCheck;
import org.teiid.core.util.ExecutorUtils;
import org.teiid.core.util.LRUCache;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.internal.process.PreparedPlan;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.dqp.internal.process.SessionAwareCache.CacheID;
import org.teiid.dqp.internal.process.TupleSourceCache;
import org.teiid.dqp.message.RequestID;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.dqp.service.TransactionService;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.QueryPlugin;
import org.teiid.query.eval.SecurityFunctionEvaluator;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.parser.ParseInfo;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.sql.lang.SourceHint;
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
	
	private static class GlobalState {
	    /** Uniquely identify the command being processed */
	    private Object processorID;
	    
	    /** Identify a group of related commands, which typically get cleaned up together */
	    private String connectionID;

	    private int processorBatchSize = BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE;
	    
	    private String userName;
	    
	    private Serializable commandPayload;
	    
	    private String vdbName = ""; //$NON-NLS-1$
	    
	    private int vdbVersion;
	    
	    /** Indicate whether statistics should be collected for relational node processing*/
	    private boolean collectNodeStatistics;
	    
	    private Random random = null;
	    
	    private SecurityFunctionEvaluator securityFunctionEvaluator;
	    
	    private TimeZone timezone = TimeZone.getDefault();
	    
	    private QueryProcessor.ProcessorFactory queryProcessorFactory;
	        
	    private Determinism determinismLevel = Determinism.DETERMINISTIC;
	    
	    private Set<String> groups;
	    private Map<String, String> aliasMapping;
	    
	    private long timeSliceEnd = Long.MAX_VALUE;
	    
	    private long timeoutEnd = Long.MAX_VALUE;
	    
	    private QueryMetadataInterface metadata; 
	    
	    private boolean validateXML;
	    
	    private BufferManager bufferManager;
	    
	    private GlobalTableStore globalTables;
	    
	    private SessionAwareCache<PreparedPlan> planCache;
	    
	    private boolean resultSetCacheEnabled = true;
	    
	    private int userRequestSourceConcurrency;
	    private Subject subject;
	    private HashSet<Object> dataObjects;

		private SessionMetadata session;

		private RequestID requestId;
		
		private DQPWorkContext dqpWorkContext;
		private TransactionContext transactionContext;
		private TransactionService transactionService;
		private SourceHint sourceHint;
		private Executor executor = ExecutorUtils.getDirectExecutor();
		Map<Object, List<ReusableExecution<?>>> reusableExecutions;
	    Set<CommandListener> commandListeners = null;
	    private LRUCache<String, DecimalFormat> decimalFormatCache;
		private LRUCache<String, SimpleDateFormat> dateFormatCache;
		private AtomicLong reuseCount = null;
		private ClassLoader classLoader;
		
	    private List<Exception> warnings = null;
	    
	    private Options options = null;
	    private boolean returnAutoGeneratedKeys;
	    private GeneratedKeysImpl generatedKeys;
	}
	
	private GlobalState globalState = new GlobalState();

    private VariableContext variableContext = new VariableContext();
    private TempTableStore tempTableStore;
    private LinkedList<String> recursionStack;
    private boolean nonBlocking;
    private HashSet<Object> planningObjects;
    private HashSet<Object> dataObjects = this.globalState.dataObjects;
    private TupleSourceCache tupleSourceCache;

    /**
     * Construct a new context.
     */
    public CommandContext(Object processorID, String connectionID, String userName, 
        Serializable commandPayload, String vdbName, int vdbVersion, boolean collectNodeStatistics) {
        setProcessorID(processorID);
        setConnectionID(connectionID);
        setUserName(userName);
        setCommandPayload(commandPayload);
        setVdbName(vdbName);
        setVdbVersion(vdbVersion);  
        setCollectNodeStatistics(collectNodeStatistics);
    }

    /**
     * Construct a new context.
     */
    public CommandContext(Object processorID, String connectionID, String userName, 
        String vdbName, int vdbVersion) {

        this(processorID, connectionID, userName, null, vdbName, 
            vdbVersion, false);            
             
    }

    public CommandContext() {
    }
    
    private CommandContext(GlobalState state) {
    	this.globalState = state;
    	this.dataObjects = this.globalState.dataObjects;
    }
    
    public Determinism getDeterminismLevel() {
		return globalState.determinismLevel;
	}
    
    public Determinism resetDeterminismLevel() {
    	Determinism result = globalState.determinismLevel;
    	globalState.determinismLevel = Determinism.DETERMINISTIC;
    	return result;
    }
    
    public void setDeterminismLevel(Determinism level) {
    	if (globalState.determinismLevel == null || level.compareTo(globalState.determinismLevel) < 0) {
    		globalState.determinismLevel = level;
    	}
    }
    
    /**
     * @return
     */
    public Object getProcessorID() {
        return globalState.processorID;
    }

    /**
     * @param object
     */
    public void setProcessorID(Object object) {
        ArgCheck.isNotNull(object);
        globalState.processorID = object;
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
    	return clone;
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
        return globalState.vdbName;
    }

    public int getVdbVersion() {
        return globalState.vdbVersion;
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
        this.globalState.vdbName = vdbName;
    }

    /**
     * Sets the vdbVersion.
     * @param vdbVersion The vdbVersion to set
     */
    public void setVdbVersion(int vdbVersion) {
        this.globalState.vdbVersion = vdbVersion;
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

    /** 
     * @return Returns the securityFunctionEvaluator.
     */
    public SecurityFunctionEvaluator getSecurityFunctionEvaluator() {
        return this.globalState.securityFunctionEvaluator;
    }
    
    /** 
     * @param securityFunctionEvaluator The securityFunctionEvaluator to set.
     */
    public void setSecurityFunctionEvaluator(SecurityFunctionEvaluator securityFunctionEvaluator) {
        this.globalState.securityFunctionEvaluator = securityFunctionEvaluator;
    }

	public TempTableStore getTempTableStore() {
		return tempTableStore;
	}

	public void setTempTableStore(TempTableStore tempTableStore) {
		this.tempTableStore = tempTableStore;
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
		Object value = variableContext.getValue((ElementSymbol)expression);
		if (value == null && !variableContext.containsVariable((ElementSymbol)expression)) {
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
		globalState.metadata = metadata;
	}
	
	public QueryMetadataInterface getMetadata() {
		return globalState.metadata;
	}
    
    public void setValidateXML(boolean validateXML) {
    	globalState.validateXML = validateXML;
	}
    
    public boolean validateXML() {
		return globalState.validateXML;
	}
    
    public BufferManager getBufferManager() {
    	return globalState.bufferManager;
    }
    
    public void setBufferManager(BufferManager bm) {
    	globalState.bufferManager = bm;
    }
    
    public GlobalTableStore getGlobalTableStore() {
    	return globalState.globalTables;
    }
    
    public void setGlobalTableStore(GlobalTableStore tempTableStore) {
    	globalState.globalTables = tempTableStore;
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
		return this.globalState.session;
	}
	
	public void setSession(SessionMetadata session) {
		this.globalState.session = session;
	}
	
	@Override
	public String getRequestId() {
		return this.globalState.requestId != null ? this.globalState.requestId.toString() : null;
	}
	
	public void setRequestId(RequestID requestId) {
		this.globalState.requestId = requestId;
	}
	
	public void setDQPWorkContext(DQPWorkContext workContext) {
		this.globalState.dqpWorkContext = workContext;
	}
	
	@Override
	public Map<String, DataPolicy> getAllowedDataPolicies() {
		if (this.globalState.dqpWorkContext == null) {
			return null;
		}
		return this.globalState.dqpWorkContext.getAllowedDataPolicies();
	}
	
	@Override
	public VDBMetaData getVdb() {
		return this.globalState.dqpWorkContext.getVDB();
	}
	
	public DQPWorkContext getDQPWorkContext() {
		return this.globalState.dqpWorkContext;
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
	
	public SourceHint getSourceHint() {
		return this.globalState.sourceHint;
	}
	
	public void setSourceHint(SourceHint hint) {
		this.globalState.sourceHint = hint;
	}
	
	public Executor getExecutor() {
		return this.globalState.executor;
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
		return this.globalState.classLoader;
	}
	
	public void setVDBClassLoader(ClassLoader classLoader) {
		this.globalState.classLoader = classLoader;
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
		}
        LogManager.logInfo(LogConstants.CTX_DQP, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31105, warning.getMessage()));
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
		return this.globalState.returnAutoGeneratedKeys;
	}

	public void setReturnAutoGeneratedKeys(boolean b) {
		this.globalState.returnAutoGeneratedKeys = b;
	}
	
	@Override
	public GeneratedKeysImpl returnGeneratedKeys(String[] columnNames,
			Class<?>[] columnDataTypes) {
		synchronized (this.globalState) {
			this.globalState.generatedKeys = new GeneratedKeysImpl(columnNames, columnDataTypes);
			return this.globalState.generatedKeys;
		}
	}
	
	public GeneratedKeysImpl getGeneratedKeys() {
		synchronized (this.globalState) {
			return this.globalState.generatedKeys;
		}
	}
	
}
