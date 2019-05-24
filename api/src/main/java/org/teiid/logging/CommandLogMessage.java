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

package org.teiid.logging;

import java.sql.Timestamp;

import org.teiid.client.plan.PlanNode;
import org.teiid.core.util.StringUtil;
import org.teiid.translator.ExecutionContext;

/**
 * Log Message for source and user command events.
 */
public class CommandLogMessage {

    public enum Event {
        NEW,
        PLAN,
        END,
        CANCEL,
        SOURCE,
        ERROR
    }

    private boolean source;
    private Event event;
    private long timestamp;

    // Transaction info
    private String transactionID;

    // Session info
    private String sessionID;
    private String applicationName;
    private String principal;
    private String vdbName;
    private String vdbVersion;

    // RequestInfo
    private String requestID;
    private Long sourceCommandID;
    private String sql;
    private Long rowCount;
    private String modelName;
    private String translatorName;
    private ExecutionContext executionContext;
    private PlanNode plan;

    private Object[] sourceCommand;
    private Long cpuTime;

    public CommandLogMessage(long timestamp,
                                String requestID,
                                String transactionID,
                                String sessionID,
                                String applicationName,
                                String principal,
                                String vdbName,
                                String vdbVersion,
                                String sql,
                                Long cpuTime) {
        // userCommandStart
        this(timestamp, requestID, transactionID, sessionID, principal, vdbName, vdbVersion, null, Event.NEW, null);
        this.applicationName = applicationName;
        this.sql = sql;
        this.cpuTime = cpuTime;
    }
    public CommandLogMessage(long timestamp,
                                String requestID,
                                String transactionID,
                                String sessionID,
                                String principal,
                                String vdbName,
                                String vdbVersion,
                                Long finalRowCount,
                                Event event, PlanNode plan) {
        // userCommandEnd
        this.event = event;
        this.timestamp = timestamp;
        this.requestID = requestID;
        this.transactionID = transactionID;
        this.sessionID = sessionID;
        this.principal = principal;
        this.vdbName = vdbName;
        this.vdbVersion = vdbVersion;
        this.rowCount = finalRowCount;
        this.plan = plan;
    }
    public CommandLogMessage(long timestamp,
                                String requestID,
                                long sourceCommandID,
                                String transactionID,
                                String modelName,
                                String translatorName,
                                String sessionID,
                                String principal,
                                String sql,
                                ExecutionContext context) {
        // dataSourceCommandStart
        this(timestamp, requestID, sourceCommandID, transactionID, modelName, translatorName, sessionID, principal, null, Event.NEW, context, null);
        this.sql = sql;
    }
    public CommandLogMessage(long timestamp,
                                String requestID,
                                long sourceCommandID,
                                String transactionID,
                                String modelName,
                                String translatorName,
                                String sessionID,
                                String principal,
                                Long finalRowCount,
                                Event event,
                                ExecutionContext context, Long cpuTime) {
        // dataSourceCommandEnd
        this.source = true;
        this.event = event;
        this.timestamp = timestamp;
        this.requestID = requestID;
        this.sourceCommandID = sourceCommandID;
        this.transactionID = transactionID;
        this.modelName = modelName;
        this.translatorName = translatorName;
        this.sessionID = sessionID;
        this.principal = principal;
        this.rowCount = finalRowCount;
        this.executionContext = context;
        this.cpuTime = cpuTime;
    }

    public String toString() {
        if (!source) {
            if (event == Event.NEW) {
                return "\tSTART USER COMMAND:\tstartTime=" + new Timestamp(timestamp) + "\trequestID=" + requestID + "\ttxID=" + transactionID + "\tsessionID=" + sessionID + "\tapplicationName=" + applicationName + "\tprincipal=" + principal + "\tvdbName=" + vdbName + "\tvdbVersion=" + vdbVersion + "\tsql=" + sql;  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$//$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$
            }
            return "\t"+ event +" USER COMMAND:\tendTime=" + new Timestamp(timestamp) + "\trequestID=" + requestID + "\ttxID=" + transactionID + "\tsessionID=" + sessionID + "\tprincipal=" + principal + "\tvdbName=" + vdbName + "\tvdbVersion=" + vdbVersion + "\tfinalRowCount=" + rowCount+ ((plan!=null)?"\tplan=" + plan.toYaml():"") + ((cpuTime!=null)?"\tcpuTime(ns)=" + cpuTime:"");  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$//$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$ //$NON-NLS-10$ //$NON-NLS-11$ //$NON-NLS-12$ //$NON-NLS-13$
        }
        if (event == Event.NEW) {
            return "\tSTART DATA SRC COMMAND:\tstartTime=" + new Timestamp(timestamp) + "\trequestID=" + requestID + "\tsourceCommandID="+ sourceCommandID + "\texecutionID="+ executionContext.getExecutionCountIdentifier() + "\ttxID=" + transactionID + "\tmodelName="+ modelName + "\ttranslatorName=" + translatorName + "\tsessionID=" + sessionID + "\tprincipal=" + principal + "\tsql=" + sql;  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$//$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$ //$NON-NLS-10$
        }
        return "\t"+ event +" SRC COMMAND:\tendTime=" + new Timestamp(timestamp) + "\trequestID=" + requestID + "\tsourceCommandID="+ sourceCommandID + //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                "\texecutionID="+ executionContext.getExecutionCountIdentifier() + "\ttxID=" + transactionID + "\tmodelName="+ modelName + "\ttranslatorName=" + translatorName +  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                "\tsessionID=" + sessionID + "\tprincipal=" + principal + ((sourceCommand != null)?"\tsourceCommand=" + StringUtil.toString(sourceCommand, " "):"") + ((rowCount != null)?"\tfinalRowCount=" + rowCount:"") + ((cpuTime!=null)?"\tcpuTime(ns)=" + cpuTime:"");  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$//$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$
      }

    public long getTimestamp() {
        return timestamp;
    }
    public String getTransactionID() {
        return transactionID;
    }
    public String getSessionID() {
        return sessionID;
    }
    public String getApplicationName() {
        return applicationName;
    }
    public String getPrincipal() {
        return principal;
    }
    public String getVdbName() {
        return vdbName;
    }
    public String getVdbVersion() {
        return vdbVersion;
    }
    public String getRequestID() {
        return requestID;
    }
    public Long getSourceCommandID() {
        return sourceCommandID;
    }
    /**
     * Returns the command.  Only valid for {@link Event#NEW}
     * @return
     */
    public String getSql() {
        return sql;
    }
    /**
     * Returns the command.  Only valid for {@link Event#END}
     * @return
     */
    public Long getRowCount() {
        return rowCount;
    }
    public String getModelName() {
        return modelName;
    }

    /**
     * @deprecated in 7.7 see {@link #getTranslatorName()}
     */
    public String getConnectorBindingName() {
        return translatorName;
    }

    public String getTranslatorName() {
        return translatorName;
    }

    public Event getStatus() {
        return event;
    }
    public boolean isSource() {
        return source;
    }
    /**
     * Only available for source commands
     * @return
     */
    public ExecutionContext getExecutionContext() {
        return executionContext;
    }
    /**
     * Only available for user commands after the NEW event
     * @return
     */
    public PlanNode getPlan() {
        return plan;
    }

    /**
     * the cpu time in nanoseconds.  Will be null for events
     * that don't have a cpu time measurement.  Will be -1 when
     * the system is not able to determine a value.
     *
     * @return
     */
    public Long getCpuTime() {
        return cpuTime;
    }

    public void setSourceCommand(Object[] sourceCommand) {
        this.sourceCommand = sourceCommand;
    }

    /**
     * The source command issued.  It's up to each source as to what the representation is.
     * Only set for the {@link Event#SOURCE}
     */
    public Object[] getSourceCommand() {
        return sourceCommand;
    }
}
