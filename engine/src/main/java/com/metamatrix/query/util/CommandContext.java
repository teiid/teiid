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

package com.metamatrix.query.util;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryProcessingException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.eval.SecurityFunctionEvaluator;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.relational.PlanToProcessConverter;
import com.metamatrix.query.processor.QueryProcessor;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.util.VariableContext;

/** 
 * Defines the context that a command is processing in.  For example, this defines
 * who is processing the command and why.  Also, this class (or subclasses) provide
 * a means to pass context-specific information between users of the query processor
 * framework.
 */
public class CommandContext implements Cloneable {
	
	private static class GlobalState {
	    /** Uniquely identify the command being processed */
	    private Object processorID;
	    
	    /** Identify a group of related commands, which typically get cleaned up together */
	    private String connectionID;

	    private int processorBatchSize = BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE;
	    
	    private int connectorBatchSize = BufferManager.DEFAULT_CONNECTOR_BATCH_SIZE;

	    private String userName;
	    
	    private Serializable commandPayload;
	    
	    private String vdbName;
	    
	    private int vdbVersion;
	    
	    private Properties environmentProperties;
	    
	    /** Indicate whether data should be dumped for debugging purposes while processing the query */
	    private boolean processDebug;  
	        
	    /** Indicate whether statistics should be collected for relational node processing*/
	    private boolean collectNodeStatistics;
	    
	    private int streamingBatchSize;
	    
	    private Random random = null;
	    
	    private SecurityFunctionEvaluator securityFunctionEvaluator;
	    
	    private TimeZone timezone = TimeZone.getDefault();
	    
	    private PlanToProcessConverter planToProcessConverter;
	    
	    private QueryProcessor.ProcessorFactory queryProcessorFactory;
	        
	    private boolean sessionFunctionEvaluated;
	    
	    private Set<String> groups;
	    
	    private long timeSliceEnd = Long.MAX_VALUE;
	    
	    private long timeoutEnd = Long.MAX_VALUE;
	    
	    private QueryMetadataInterface metadata; 
	    
	    private boolean validateXML;
	    
	    private BufferManager bufferManager;
	}
	
	private GlobalState globalState = new GlobalState();

    private VariableContext variableContext = new VariableContext();
    private Object tempTableStore;
    private LinkedList<String> recursionStack;

    /**
     * Construct a new context.
     */
    public CommandContext(Object processorID, String connectionID, String userName, 
        Serializable commandPayload, String vdbName, int vdbVersion, Properties envProperties, boolean processDebug, boolean collectNodeStatistics) {
        setProcessorID(processorID);
        setConnectionID(connectionID);
        setUserName(userName);
        setCommandPayload(commandPayload);
        setVdbName(vdbName);
        setVdbVersion(vdbVersion);  
        setEnvironmentProperties(envProperties);        
        setProcessDebug(processDebug);
        setCollectNodeStatistics(collectNodeStatistics);
    }

    /**
     * Construct a new context.
     */
    public CommandContext(Object processorID, String connectionID, String userName, 
        String vdbName, int vdbVersion) {

        this(processorID, connectionID, userName, null, vdbName, 
            vdbVersion, null, false, false);            
             
    }

    public CommandContext() {        
    }
    
    public boolean isSessionFunctionEvaluated() {
		return globalState.sessionFunctionEvaluated;
	}
    
    public void setSessionFunctionEvaluated(boolean sessionFunctionEvaluated) {
    	globalState.sessionFunctionEvaluated = sessionFunctionEvaluated;
	}
    
    /**
     * @return
     */
    public Object getProcessorID() {
        return globalState.processorID;
    }

    public boolean getProcessDebug() {
        return globalState.processDebug;
    }
    
    public void setProcessDebug(boolean processDebug) {
    	globalState.processDebug = processDebug;
    }

    /**
     * @param object
     */
    public void setProcessorID(Object object) {
        ArgCheck.isNotNull(object);
        globalState.processorID = object;
    }

    public Object clone() {
    	CommandContext clone = new CommandContext();
    	clone.globalState = this.globalState;
    	clone.variableContext = this.variableContext;
    	if (this.recursionStack != null) {
            clone.recursionStack = new LinkedList<String>(this.recursionStack);
        }
    	return clone;
    }
    
    public String toString() {
        return "CommandContext: " + globalState.processorID; //$NON-NLS-1$
    }

    /**
     * @return String
     */
    public String getConnectionID() {
        return globalState.connectionID;
    }

    /**
     * @return String
     */
    public String getUserName() {
        return globalState.userName;
    }

    /**
     * @return String
     */
    public String getVdbName() {
        return globalState.vdbName;
    }

    /**
     * @return String
     */
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

    public Properties getEnvironmentProperties() {
        return globalState.environmentProperties;
    }

    public void setEnvironmentProperties(Properties properties) {
    	globalState.environmentProperties = properties;
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
    
	public int getStreamingBatchSize() {
		return globalState.streamingBatchSize;
	}

	public void setStreamingBatchSize(int streamingBatchSize) {
		this.globalState.streamingBatchSize = streamingBatchSize;
	}

    
    public int getConnectorBatchSize() {
        return this.globalState.connectorBatchSize;
    }

    
    public void setConnectorBatchSize(int connectorBatchSize) {
        this.globalState.connectorBatchSize = connectorBatchSize;
    }

    
    public int getProcessorBatchSize() {
        return this.globalState.processorBatchSize;
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
			throw new QueryProcessingException(QueryExecPlugin.Util.getString("ExecDynamicSqlInstruction.3", value)); //$NON-NLS-1$
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

	public Object getTempTableStore() {
		return tempTableStore;
	}

	public void setTempTableStore(Object tempTableStore) {
		this.tempTableStore = tempTableStore;
	}
	
	public TimeZone getServerTimeZone() {
		return globalState.timezone;
	}

	public void setPlanToProcessConverter(PlanToProcessConverter planToProcessConverter) {
		this.globalState.planToProcessConverter = planToProcessConverter;
	}

	public PlanToProcessConverter getPlanToProcessConverter() {
		return globalState.planToProcessConverter;
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
	
	public Object getFromContext(Expression expression) throws MetaMatrixComponentException {
		if (variableContext == null || !(expression instanceof ElementSymbol)) {
			throw new MetaMatrixComponentException(ErrorMessageKeys.PROCESSOR_0033, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0033, expression, "No value was available")); //$NON-NLS-1$
		}
		Object value = variableContext.getValue((ElementSymbol)expression);
		if (value == null && !variableContext.containsVariable((ElementSymbol)expression)) {
			throw new MetaMatrixComponentException(ErrorMessageKeys.PROCESSOR_0033, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0033, expression, "No value was available")); //$NON-NLS-1$
		}
		return value;
	}
	
	public Set<String> getGroups() {
		return globalState.groups;
	}
	
	public void setGroups(Set<String> groups) {
		this.globalState.groups = groups;
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
	
}
