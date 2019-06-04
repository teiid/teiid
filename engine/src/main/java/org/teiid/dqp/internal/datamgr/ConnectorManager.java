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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.naming.InitialContext;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
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
import org.teiid.query.function.metadata.FunctionMetadataValidator;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.util.TeiidTracingUtil;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.resource.api.WrappedConnection;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;

import io.opentracing.Span;
import io.opentracing.log.Fields;
import io.opentracing.tag.Tags;


/**
 * The <code>ConnectorManager</code> manages an {@link ExecutionFactory}
 * and its associated workers' state.
 */
public class ConnectorManager  {

    private static final String JAVA_CONTEXT = "java:/"; //$NON-NLS-1$

    private final String translatorName;
    private String translatorType;
    private final String connectionName;
    private final String jndiName;
    private final List<String> id;

    // known requests
    private final ConcurrentHashMap<AtomicRequestID, ConnectorWork> requestStates = new ConcurrentHashMap<AtomicRequestID, ConnectorWork>();

    private volatile SourceCapabilities cachedCapabilities;

    private volatile boolean stopped;
    private final ExecutionFactory<Object, Object> executionFactory;

    private List<FunctionMethod> functions;

    public ConnectorManager(String translatorName, String connectionName) {
        this(translatorName, connectionName, new ExecutionFactory<Object, Object>());
    }

    public ConnectorManager(String translatorName, String connectionName, ExecutionFactory<Object, Object> ef) {
        this.translatorName = translatorName;
        this.connectionName = connectionName;
        if (this.connectionName != null) {
            if (!this.connectionName.startsWith(JAVA_CONTEXT)) {
                jndiName = JAVA_CONTEXT + this.connectionName;
            } else {
                jndiName = this.connectionName;
            }
        } else {
            jndiName = null;
        }
        this.executionFactory = ef;
        this.id = Arrays.asList(translatorName, connectionName);
        if (ef != null) {
            Translator annotation = ef.getClass().getAnnotation(Translator.class);
            if (annotation != null) {
                this.translatorType = annotation.name();
            }
            ClassLoader originalCL = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(ef.getClass().getClassLoader());
                functions = ef.getPushDownFunctions();
            } finally {
                Thread.currentThread().setContextClassLoader(originalCL);
            }
            if (functions != null) {
                //set the specific name to match against imported versions of
                //the same function
                for (FunctionMethod functionMethod : functions) {
                    functionMethod.setProperty(FunctionMethod.SYSTEM_NAME, functionMethod.getName());
                }
                ValidatorReport report = new ValidatorReport("Function Validation"); //$NON-NLS-1$
                FunctionMetadataValidator.validateFunctionMethods(functions, report);
                if(report.hasItems()) {
                    throw new TeiidRuntimeException(report.getFailureMessage());
                }
            }
        }
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
        return functions;
    }

    public SourceCapabilities getCapabilities() throws TranslatorException, TeiidComponentException {
        if (cachedCapabilities != null) {
            return cachedCapabilities;
        }
        checkStatus();
        ExecutionFactory<Object, Object> translator = getExecutionFactory();
        synchronized (this) {
            if (cachedCapabilities != null) {
                return cachedCapabilities;
            }
            ClassLoader originalCL = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(translator.getClass().getClassLoader());
                cachedCapabilities = buildCapabilities(translator);
            } finally {
                Thread.currentThread().setContextClassLoader(originalCL);
            }
        }
        return cachedCapabilities;
    }

    private BasicSourceCapabilities buildCapabilities(ExecutionFactory<Object, Object> translator) throws TranslatorException {
        if (translator.isSourceRequiredForCapabilities()) {
            Object connection = null;
            Object connectionFactory = null;
            try {
                connectionFactory = getConnectionFactory();

                if (connectionFactory != null) {
                    connection = translator.getConnection(connectionFactory, null);
                }
                if (connection == null) {
                    throw new TranslatorException(QueryPlugin.Event.TEIID31108, QueryPlugin.Util.getString("datasource_not_found", getConnectionName())); //$NON-NLS-1$);
                }
                if (connection instanceof WrappedConnection) {
                    try {
                        connection = ((WrappedConnection)connection).unwrap();
                    } catch (Exception e) {
                        throw new TranslatorException(QueryPlugin.Event.TEIID30477, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30477, getConnectionName()));
                    }
                }
                LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Initializing the capabilities for", translatorName); //$NON-NLS-1$
                synchronized (executionFactory) {
                    executionFactory.initCapabilities(connection);
                }
            } finally {
                if (connection != null) {
                    translator.closeConnection(connection, connectionFactory);
                }
            }
        }
        BasicSourceCapabilities resultCaps = CapabilitiesConverter.convertCapabilities(translator, id);
        return resultCaps;
    }

    public ConnectorWork registerRequest(AtomicRequestMessage message) throws TeiidComponentException, TranslatorException {
        checkStatus();
        AtomicRequestID atomicRequestId = message.getAtomicRequestID();
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {atomicRequestId, "Create State"}); //$NON-NLS-1$

        final ConnectorWorkItem item = new ConnectorWorkItem(message, this);
        ConnectorWork proxy = (ConnectorWork) Proxy.newProxyInstance(ConnectorWork.class.getClassLoader(),
                new Class[] { ConnectorWork.class }, new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        ClassLoader originalCL = Thread.currentThread().getContextClassLoader();
                        try {
                            Thread.currentThread().setContextClassLoader(getExecutionFactory().getClass().getClassLoader());
                            return method.invoke(item, args);
                        } catch (InvocationTargetException e) {
                            throw e.getTargetException();
                        } finally {
                            Thread.currentThread().setContextClassLoader(originalCL);
                        }
                    }
                });

        Assertion.isNull(requestStates.put(atomicRequestId, proxy), "State already existed"); //$NON-NLS-1$
        return proxy;
    }

    ConnectorWork getState(AtomicRequestID requestId) {
        return requestStates.get(requestId);
    }

    /**
     * Remove the state associated with
     * the given <code>RequestID</code>.
     */
    boolean removeState(AtomicRequestID sid) {
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, sid, "Remove State"); //$NON-NLS-1$
        return requestStates.remove(sid) != null;
    }

    int size() {
        return requestStates.size();
    }

    /**
     * initialize this <code>ConnectorManager</code>.
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
            workItem.cancel(true);
        }
    }

    void logSRCCommand(ConnectorWorkItem cwi, AtomicRequestMessage qr, ExecutionContext context, Event cmdStatus, Long finalRowCnt, Long cpuTime) {
        logSRCCommand(cwi, qr, context, cmdStatus, finalRowCnt, cpuTime, null);
    }

    /**
     * Add begin point to transaction monitoring table.
     * @param qr Request that contains the MetaMatrix command information in the transaction.
     */
    void logSRCCommand(ConnectorWorkItem cwi, AtomicRequestMessage qr, ExecutionContext context, Event cmdStatus, Long finalRowCnt, Long cpuTime, Object[] command) {
        if (!LogManager.isMessageToBeRecorded(LogConstants.CTX_COMMANDLOGGING_SOURCE, MessageLevel.DETAIL)
                && !TeiidTracingUtil.getInstance().isTracingEnabled(null, null)) {
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
        AtomicRequestID sid = qr.getAtomicRequestID();

        String principal = userName == null ? "unknown" : userName; //$NON-NLS-1$

        CommandLogMessage message = null;
        if (cmdStatus == Event.NEW) {
            message = new CommandLogMessage(System.currentTimeMillis(), qr.getRequestID().toString(), sid.getNodeID(), transactionID, modelName, translatorName, qr.getWorkContext().getSessionId(), principal, sqlStr, context);
            Span span = TeiidTracingUtil.getInstance().buildSourceSpan(message, translatorType);
            cwi.setTracingSpan(span);
        }
        else {
            message = new CommandLogMessage(System.currentTimeMillis(), qr.getRequestID().toString(), sid.getNodeID(), transactionID, modelName, translatorName, qr.getWorkContext().getSessionId(), principal, finalRowCnt, cmdStatus, context, cpuTime);
            if (cmdStatus == Event.SOURCE) {
                message.setSourceCommand(command);
            }
            Span span = cwi.getTracingSpan();
            if (span != null) {
                switch (cmdStatus) {
                case SOURCE:
                    if (command != null) {
                        Map<String, String> map = new HashMap<String, String>();
                        map.put("source-command", Arrays.toString(command)); //$NON-NLS-1$
                        span.log(map);
                    }
                    break;
                case CANCEL:
                    span.log("cancel"); //$NON-NLS-1$
                    break;
                case END:
                    span.finish();
                    break;
                case ERROR:
                    Tags.ERROR.set(span, true);
                    Map<String, String> map = new HashMap<String, String>();
                    map.put(Fields.EVENT, "error"); //$NON-NLS-1$
                    span.log(map);
                    break;
                default:
                    break;
                }
            }
        }
        LogManager.log(MessageLevel.DETAIL, LogConstants.CTX_COMMANDLOGGING_SOURCE, message);
    }

    /**
     * Get the <code>Translator</code> object managed by this  manager.
     * @return the <code>ExecutionFactory</code>.
     */
    public ExecutionFactory<Object, Object> getExecutionFactory() {
        return this.executionFactory;
    }

    /**
     * Get the ConnectionFactory object required by this manager
     * @return
     */
    public Object getConnectionFactory() throws TranslatorException {
        if (this.connectionName != null) {
            try {
                InitialContext ic = new InitialContext();
                try {
                    return ic.lookup(jndiName);
                } catch (Exception e) {
                    if (!jndiName.equals(this.connectionName)) {
                        return ic.lookup(this.connectionName);
                    }
                    throw e;
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
