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

package org.teiid.dqp.internal.datamgr;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import javax.naming.InitialContext;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.util.Assertion;
import org.teiid.dqp.message.AtomicRequestID;
import org.teiid.dqp.message.AtomicRequestMessage;
import org.teiid.logging.CommandLogMessage;
import org.teiid.logging.CommandLogMessage.Event;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.FunctionMethod;
import org.teiid.query.QueryPlugin;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.sql.lang.Command;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;


/**
 * The <code>ConnectorManager</code> manages an {@link ExecutionFactory}
 * and its associated workers' state.
 */
public class ConnectorManager  {
	
	private static final String JAVA_CONTEXT = "java:/"; //$NON-NLS-1$

	private String translatorName;
	private String connectionName;
	private List<String> id;
	
    // known requests
    private ConcurrentHashMap<AtomicRequestID, ConnectorWorkItem> requestStates = new ConcurrentHashMap<AtomicRequestID, ConnectorWorkItem>();
	
	private SourceCapabilities cachedCapabilities;
	
	private volatile boolean stopped;
	private ExecutionFactory<Object, Object> executionFactory;
	
    public ConnectorManager(String translatorName, String connectionName) {
    	this.translatorName = translatorName;
    	this.connectionName = connectionName;
    	this.id = Arrays.asList(translatorName, connectionName);
    }
    
    public String getStausMessage() {
    	String msg = ""; //$NON-NLS-1$
    	ExecutionFactory<Object, Object> ef = getExecutionFactory();
		
    	if(ef != null) {
    		if (ef.isSourceRequired()) {
    			
    			Object conn = null;
				try {
					conn = getConnectionFactory();
				} catch (TranslatorException e) {
					// treat this as connection not found. 
				}
				
    			if (conn == null) {
    				msg = QueryPlugin.Util.getString("datasource_not_found", this.connectionName); //$NON-NLS-1$
    			}
    		}
    	}
    	else {
    		msg = QueryPlugin.Util.getString("translator_not_found", this.translatorName); //$NON-NLS-1$
    	}
    	return msg;
    }
    
	public List<FunctionMethod> getPushDownFunctions(){
    	return getExecutionFactory().getPushDownFunctions();
    }
    
    public SourceCapabilities getCapabilities() throws TeiidComponentException {
    	if (cachedCapabilities != null) {
    		return cachedCapabilities;
    	}

		checkStatus();
		ExecutionFactory<Object, Object> translator = getExecutionFactory();
		BasicSourceCapabilities resultCaps = CapabilitiesConverter.convertCapabilities(translator, id);
		cachedCapabilities = resultCaps;
		return resultCaps;
    }
    
    public ConnectorWork registerRequest(AtomicRequestMessage message) throws TeiidComponentException {
    	checkStatus();
    	AtomicRequestID atomicRequestId = message.getAtomicRequestID();
    	LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {atomicRequestId, "Create State"}); //$NON-NLS-1$

    	ConnectorWorkItem item = new ConnectorWorkItem(message, this);
        Assertion.isNull(requestStates.put(atomicRequestId, item), "State already existed"); //$NON-NLS-1$
        return item;
    }
    
    ConnectorWork getState(AtomicRequestID requestId) {
        return requestStates.get(requestId);
    }
    
    /**
     * Remove the state associated with
     * the given <code>RequestID</code>.
     */
    boolean removeState(AtomicRequestID id) {
    	LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {id, "Remove State"}); //$NON-NLS-1$
        return requestStates.remove(id) != null;
    }

    int size() {
        return requestStates.size();
    }
    
    /**
     * initialize this <code>ConnectorManager</code>.
     * @throws TranslatorException 
     */
    public void start() {
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, QueryPlugin.Util.getString("ConnectorManagerImpl.Initializing_connector", translatorName)); //$NON-NLS-1$
    }
    
    /**
     * Stop this connector.
     */
    public void stop() {    
    	stopped = true;
        //ensure that all requests receive a response
        for (ConnectorWork workItem : this.requestStates.values()) {
    		workItem.cancel();
		}
    }

    /**
     * Add begin point to transaction monitoring table.
     * @param qr Request that contains the MetaMatrix command information in the transaction.
     */
    void logSRCCommand(AtomicRequestMessage qr, ExecutionContext context, Event cmdStatus, Integer finalRowCnt) {
    	if (!LogManager.isMessageToBeRecorded(LogConstants.CTX_COMMANDLOGGING, MessageLevel.DETAIL)) {
    		return;
    	}
        String sqlStr = null;
        if(cmdStatus == Event.NEW){
        	Command cmd = qr.getCommand();
            sqlStr = cmd != null ? cmd.toString() : null;
        }
        String userName = qr.getWorkContext().getUserName();
        String transactionID = null;
        if ( qr.isTransactional() ) {
            transactionID = qr.getTransactionContext().getTransactionId();
        }
        
        String modelName = qr.getModelName();
        AtomicRequestID id = qr.getAtomicRequestID();
        
        String principal = userName == null ? "unknown" : userName; //$NON-NLS-1$
        
        CommandLogMessage message = null;
        if (cmdStatus == Event.NEW) {
            message = new CommandLogMessage(System.currentTimeMillis(), qr.getRequestID().toString(), id.getNodeID(), transactionID, modelName, translatorName, qr.getWorkContext().getSessionId(), principal, sqlStr, context);
        } 
        else {
            message = new CommandLogMessage(System.currentTimeMillis(), qr.getRequestID().toString(), id.getNodeID(), transactionID, modelName, translatorName, qr.getWorkContext().getSessionId(), principal, finalRowCnt, cmdStatus, context);
        }         
        LogManager.log(MessageLevel.DETAIL, LogConstants.CTX_COMMANDLOGGING, message);
    }
    
    /**
     * Get the <code>Translator</code> object managed by this  manager.
     * @return the <code>ExecutionFactory</code>.
     */
	public ExecutionFactory<Object, Object> getExecutionFactory() {
		return this.executionFactory;
    }
    
	public void setExecutionFactory(ExecutionFactory<?, ?> ef) {
		this.executionFactory = (ExecutionFactory<Object, Object>) ef;
	}
    
    /**
     * Get the ConnectionFactory object required by this manager
     * @return
     */
    public Object getConnectionFactory() throws TranslatorException {
    	if (this.connectionName != null) {
    		String jndiName = this.connectionName;
    		if (!this.connectionName.startsWith(JAVA_CONTEXT)) {
    			jndiName = JAVA_CONTEXT + jndiName;
    		}

			try {
				InitialContext ic = new InitialContext();    		
				try {
					return ic.lookup(jndiName);
				} catch (Exception e) {
					if (!jndiName.equals(this.connectionName)) {
						return ic.lookup(this.connectionName);
					}
				}
			} catch (Exception e) {
				 throw new TranslatorException(QueryPlugin.Event.TEIID30481, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30481, this.connectionName));
			}   			
    	}
    	return null;
    }
    
    private void checkStatus() throws TeiidComponentException {
    	if (stopped) {
    		 throw new TeiidComponentException(QueryPlugin.Event.TEIID30482, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30482, this.translatorName));
    	}
    }
    
    public String getTranslatorName() {
    	return this.translatorName;
    }
    
    public String getConnectionName() {
    	return this.connectionName;
    }
    
    public List<String> getId() {
		return id;
	}
    
}
