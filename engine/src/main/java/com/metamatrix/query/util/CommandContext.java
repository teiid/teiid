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
import java.util.Properties;
import java.util.Random;
import java.util.Stack;
import java.util.TimeZone;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryProcessingException;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.eval.SecurityFunctionEvaluator;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.optimizer.relational.PlanToProcessConverter;
import com.metamatrix.query.processor.QueryProcessor;
import com.metamatrix.query.sql.symbol.ContextReference;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.util.ValueIterator;
import com.metamatrix.query.sql.util.ValueIteratorSource;
import com.metamatrix.query.sql.util.VariableContext;

/** 
 * Defines the context that a command is processing in.  For example, this defines
 * who is processing the command and why.  Also, this class (or subclasses) provide
 * a means to pass context-specific information between users of the query processor
 * framework.
 */
public class CommandContext implements Cloneable {

    /** Uniquely identify the command being processed */
    private Object processorID;
    
    /** Identify a group of related commands, which typically get cleaned up together */
    private String connectionID;

    private int processorBatchSize = 2000;
    
    private int connectorBatchSize = 2000;

    private String userName;
    
    private Serializable commandPayload;
    
    private String vdbName;
    
    private String vdbVersion;
    
    private Properties environmentProperties;
    
    /** Indicate whether data should be dumped for debugging purposes while processing the query */
    private boolean processDebug;  
        
    /** Indicate whether statistics should be collected for relational node processing*/
    private boolean collectNodeStatistics;
    
    private int streamingBatchSize;
    
    private Random random = null;
    
    private Stack<String> recursionStack = null;
    
    private boolean optimisticTransaction = false;
    
    private SecurityFunctionEvaluator securityFunctionEvaluator;
    
    private Object tempTableStore;
    
    private TimeZone timezone = TimeZone.getDefault();
    
    private PlanToProcessConverter planToProcessConverter;
    
    private QueryProcessor.ProcessorFactory queryProcessorFactory;
    
    private VariableContext variableContext = new VariableContext();
    
    private CommandContext parent;
    
    private boolean sessionFunctionEvaluated;
    
    /**
     * Construct a new context.
     * @param collectNodeStatistics TODO
     */
    public CommandContext(Object processorID, String connectionID, String userName, 
        Serializable commandPayload, String vdbName, String vdbVersion, Properties envProperties, boolean processDebug, boolean collectNodeStatistics) {
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
        String vdbName, String vdbVersion) {

        this(processorID, connectionID, userName, null, vdbName, 
            vdbVersion, null, false, false);            
             
    }

    protected CommandContext(CommandContext context) {
        setConnectionID(context.connectionID);
                
        // Reuse existing processor ID - may be overridden
        setProcessorID(context.processorID);
            
        setUserName(context.userName);
        setCommandPayload(context.commandPayload);
        setVdbName(context.vdbName);
        setVdbVersion(context.vdbVersion);   
        setEnvironmentProperties(context.environmentProperties); 
        setProcessDebug(context.processDebug);
        setProcessorBatchSize(context.processorBatchSize);
        setConnectorBatchSize(context.connectorBatchSize);
        setRandom(context.random);
        if (context.recursionStack != null) {
            this.recursionStack = (Stack)context.recursionStack.clone();
        }
        setOptimisticTransaction(context.isOptimisticTransaction());
        this.setSecurityFunctionEvaluator(context.getSecurityFunctionEvaluator());
        this.planToProcessConverter = context.planToProcessConverter;
        this.queryProcessorFactory = context.queryProcessorFactory;
        this.variableContext = context.variableContext;
        this.parent = context;
    }
        
    public CommandContext() {        
    }
    
    public boolean isSessionFunctionEvaluated() {
    	if (parent != null) {
    		return parent.isSessionFunctionEvaluated();
    	}
		return sessionFunctionEvaluated;
	}
    
    public void setSessionFunctionEvaluated(boolean sessionFunctionEvaluated) {
    	if (parent != null) {
    		parent.setCollectNodeStatistics(sessionFunctionEvaluated);
    	} else {
    		this.sessionFunctionEvaluated = sessionFunctionEvaluated;
    	}
	}
    
    /**
     * @return
     */
    public Object getProcessorID() {
        return processorID;
    }

    public boolean getProcessDebug() {
        return this.processDebug;
    }
    
    public void setProcessDebug(boolean processDebug) {
        this.processDebug = processDebug;
    }

    /**
     * @param object
     */
    public void setProcessorID(Object object) {
        ArgCheck.isNotNull(object);
        processorID = object;
    }

    public Object clone() {
    	return new CommandContext(this);
    }
    
    public String toString() {
        return "CommandContext: " + processorID; //$NON-NLS-1$
    }

    /**
     * @return String
     */
    public String getConnectionID() {
        return connectionID;
    }

    /**
     * @return String
     */
    public String getUserName() {
        return userName;
    }

    /**
     * @return String
     */
    public String getVdbName() {
        return vdbName;
    }

    /**
     * @return String
     */
    public String getVdbVersion() {
        return vdbVersion;
    }

    /**
     * Sets the connectionID.
     * @param connectionID The connectionID to set
     */
    public void setConnectionID(String connectionID) {
        this.connectionID = connectionID;
    }

    /**
     * Sets the userName.
     * @param userName The userName to set
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }

    /**
     * Sets the vdbName.
     * @param vdbName The vdbName to set
     */
    public void setVdbName(String vdbName) {
        this.vdbName = vdbName;
    }

    /**
     * Sets the vdbVersion.
     * @param vdbVersion The vdbVersion to set
     */
    public void setVdbVersion(String vdbVersion) {
        this.vdbVersion = vdbVersion;
    }

    public Properties getEnvironmentProperties() {
        return environmentProperties;
    }

    public void setEnvironmentProperties(Properties properties) {
        environmentProperties = properties;
    }
    
    public Serializable getCommandPayload() {
        return this.commandPayload;
    }
    public void setCommandPayload(Serializable commandPayload) {
        this.commandPayload = commandPayload;
    }    
    
    /** 
     * @param collectNodeStatistics The collectNodeStatistics to set.
     * @since 4.2
     */
    public void setCollectNodeStatistics(boolean collectNodeStatistics) {
        this.collectNodeStatistics = collectNodeStatistics;
    }
    
    public boolean getCollectNodeStatistics() {
        return this.collectNodeStatistics;
    }
    
	public int getStreamingBatchSize() {
		return streamingBatchSize;
	}

	public void setStreamingBatchSize(int streamingBatchSize) {
		this.streamingBatchSize = streamingBatchSize;
	}

    
    public int getConnectorBatchSize() {
        return this.connectorBatchSize;
    }

    
    public void setConnectorBatchSize(int connectorBatchSize) {
        this.connectorBatchSize = connectorBatchSize;
    }

    
    public int getProcessorBatchSize() {
        return this.processorBatchSize;
    }

    
    public void setProcessorBatchSize(int processorBatchSize) {
        this.processorBatchSize = processorBatchSize;
    }
    
    public double getNextRand() {
    	if (parent != null) {
    		return parent.getNextRand();
    	}
        if (random == null) {
            random = new Random();
        }
        return random.nextDouble();
    }
    
    public double getNextRand(long seed) {
    	if (parent != null) {
    		return parent.getNextRand(seed);
    	}
        if (random == null) {
            random = new Random();
        }
        random.setSeed(seed);
        return random.nextDouble();
    }
    
    void setRandom(Random random) {
        this.random = random;
    }

    public void pushCall(String value) throws QueryProcessingException {
        if (recursionStack == null) {
            recursionStack = new Stack<String>();
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
     * @param optimisticTransaction The optimisticTransaction to set.
     */
    public void setOptimisticTransaction(boolean optimisticTransaction) {
        this.optimisticTransaction = optimisticTransaction;
    }

    /** 
     * @return Returns the optimisticTransaction.
     */
    public boolean isOptimisticTransaction() {
        return optimisticTransaction;
    }

    /** 
     * @return Returns the securityFunctionEvaluator.
     */
    public SecurityFunctionEvaluator getSecurityFunctionEvaluator() {
        return this.securityFunctionEvaluator;
    }
    
    /** 
     * @param securityFunctionEvaluator The securityFunctionEvaluator to set.
     */
    public void setSecurityFunctionEvaluator(SecurityFunctionEvaluator securityFunctionEvaluator) {
        this.securityFunctionEvaluator = securityFunctionEvaluator;
    }

	public Object getTempTableStore() {
		return tempTableStore;
	}

	public void setTempTableStore(Object tempTableStore) {
		this.tempTableStore = tempTableStore;
	}
	
	public TimeZone getServerTimeZone() {
		return timezone;
	}

	public void setPlanToProcessConverter(PlanToProcessConverter planToProcessConverter) {
		this.planToProcessConverter = planToProcessConverter;
	}

	public PlanToProcessConverter getPlanToProcessConverter() {
		return planToProcessConverter;
	}
	
	public QueryProcessor.ProcessorFactory getQueryProcessorFactory() {
		return this.queryProcessorFactory;
	}

	public void setQueryProcessorFactory(QueryProcessor.ProcessorFactory queryProcessorFactory) {
		this.queryProcessorFactory = queryProcessorFactory;
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
	
}
