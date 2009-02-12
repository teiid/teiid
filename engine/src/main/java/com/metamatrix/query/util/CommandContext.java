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
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Stack;
import java.util.TimeZone;

import com.metamatrix.api.exception.query.QueryProcessingException;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.query.eval.SecurityFunctionEvaluator;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.optimizer.relational.PlanToProcessConverter;
import com.metamatrix.query.processor.QueryProcessor;

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

    /** Identify where the results of processing this command should be placed */
    private TupleSourceID tupleSourceID;
    
    /** Identify how the final result set should be batched.  */
    private int outputBatchSize = 2000;
    
    private int processorBatchSize = 2000;
    
    private int connectorBatchSize = 2000;

    private String userName;
    
    private Serializable trustedPayload;
    
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
    
    private List preparedBatchUpdateValues;
    
    private Object tempTableStore;
    
    private TimeZone timezone = TimeZone.getDefault();
    
    private PlanToProcessConverter planToProcessConverter;
    
    private QueryProcessor.ProcessorFactory queryProcessorFactory;
    
    /**
     * Construct a new context.
     * @param collectNodeStatistics TODO
     */
    public CommandContext(Object processorID, String connectionID, TupleSourceID tupleSourceID, 
        int outputBatchSize, String userName, Serializable trustedPayload, Serializable commandPayload, String vdbName, String vdbVersion, 
        Properties envProperties, boolean processDebug, boolean collectNodeStatistics) {
        setProcessorID(processorID);
        setConnectionID(connectionID);
        setTupleSourceID(tupleSourceID);
        setOutputBatchSize(outputBatchSize);
        setUserName(userName);
        setTrustedPayload(trustedPayload);
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
    public CommandContext(Object processorID, String connectionID, TupleSourceID tupleSourceID, 
        int outputBatchSize, String userName, Serializable trustedPayLoad, String vdbName, String vdbVersion) {

        this(processorID, connectionID, tupleSourceID, outputBatchSize, userName, 
            trustedPayLoad, null, vdbName, vdbVersion, null, false, false);            
             
    }

    protected CommandContext(CommandContext context) {
        setConnectionID(context.connectionID);
        setOutputBatchSize(context.outputBatchSize);
                
        // Reuse existing processor ID - may be overridden
        setProcessorID(context.processorID);
            
        // Can't reuse tuple source for different context
        setTupleSourceID(null);    
        
        setUserName(context.userName);
        setTrustedPayload(context.trustedPayload);
        setCommandPayload(context.commandPayload);
        setVdbName(context.vdbName);
        setVdbVersion(context.vdbVersion);   
        setEnvironmentProperties(PropertiesUtils.clone(context.environmentProperties)); 
        setProcessDebug(context.processDebug);
        setProcessorBatchSize(context.processorBatchSize);
        setConnectorBatchSize(context.connectorBatchSize);
        setRandom(context.random);
        if (context.recursionStack != null) {
            this.recursionStack = (Stack)context.recursionStack.clone();
        }
        setOptimisticTransaction(context.isOptimisticTransaction());
        this.setSecurityFunctionEvaluator(context.getSecurityFunctionEvaluator());
        this.preparedBatchUpdateValues = context.preparedBatchUpdateValues;
        this.planToProcessConverter = context.planToProcessConverter;
        this.queryProcessorFactory = context.queryProcessorFactory;
    }
        
    public CommandContext() {        
    }
    
    /**
     * @return
     */
    public int getOutputBatchSize() {
        return outputBatchSize;
    }

    /**
     * @return
     */
    public Object getProcessorID() {
        return processorID;
    }

    /**
     * @return
     */
    public TupleSourceID getTupleSourceID() {
        return tupleSourceID;
    }

    public boolean getProcessDebug() {
        return this.processDebug;
    }
    
    public void setProcessDebug(boolean processDebug) {
        this.processDebug = processDebug;
    }
    /**
     * @param i
     */
    public void setOutputBatchSize(int i) {
        outputBatchSize = i;
    }

    /**
     * @param object
     */
    public void setProcessorID(Object object) {
        ArgCheck.isNotNull(object);
        processorID = object;
    }

    /**
     * @param sourceID
     */
    public void setTupleSourceID(TupleSourceID sourceID) {
        tupleSourceID = sourceID;
    }
    
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }
        
        if(obj instanceof CommandContext)  {
            return this.processorID.equals(((CommandContext)obj).getProcessorID());
        }
        return false;
    }
    
    public int hashCode() {
        return this.processorID.hashCode();
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
     * @return Serializable
     */
    public Serializable getTrustedPayload() {
        return trustedPayload;
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
     * Sets the trustedPayLoad.
     * @param trustedPayLoad The trustedPayLoad to set
     */
    public void setTrustedPayload(Serializable trustedPayLoad) {
        this.trustedPayload = trustedPayLoad;
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
        if (random == null) {
            random = new Random();
        }
        return random.nextDouble();
    }
    
    public double getNextRand(long seed) {
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

	public List getPreparedBatchUpdateValues() {
		return preparedBatchUpdateValues;
	}

	public void setPreparedBatchUpdateValues(List preparedBatchUpdateValues) {
		this.preparedBatchUpdateValues = preparedBatchUpdateValues;
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
}
