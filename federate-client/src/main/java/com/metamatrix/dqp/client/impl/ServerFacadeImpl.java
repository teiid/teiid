/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.dqp.client.impl;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.comm.api.ServerConnectionFactory;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.comm.platform.socket.client.SocketServerConnectionFactory;
import com.metamatrix.common.util.SqlUtil;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.dqp.client.ConnectionInfo;
import com.metamatrix.dqp.client.MetadataResult;
import com.metamatrix.dqp.client.PortableContext;
import com.metamatrix.dqp.client.RequestInfo;
import com.metamatrix.dqp.client.Results;
import com.metamatrix.dqp.client.ResultsMetadata;
import com.metamatrix.dqp.client.ServerFacade;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.dqp.message.ResultsMessage;
import com.metamatrix.jdbc.api.ExecutionProperties;


/** 
 * @since 4.2
 */
public class ServerFacadeImpl implements ServerFacade {
    
    private AtomicLong requestIDGenerator = new AtomicLong(0);
    private ServerConnectionFactory connectionFactory;
    
    /* ServerSessionContext -> ConnectionHolder */
    private Map connections = Collections.synchronizedMap(new HashMap());
    
    public ServerFacadeImpl(SocketServerConnectionFactory connFactory) {
        this.connectionFactory = connFactory;
    }

    /** 
     * @see com.metamatrix.dqp.client.ServerFacade#createSession(com.metamatrix.dqp.client.ConnectionInfo)
     * @since 4.2
     */
    public PortableContext createSession(ConnectionInfo connectionInfo) throws MetaMatrixComponentException, MetaMatrixProcessingException {
        ServerConnectionInfo connInfo = validateConnectionInfo(connectionInfo);
        try {
            ServerConnection conn = connectionFactory.createConnection(new MMURL(connInfo.getServerUrl()), connInfo.getConnectionProperties());

            ServerSessionContext context = new ServerSessionContext(connInfo, conn.getContext().getPortableString());
            connections.put(context, new ConnectionHolder(conn));
          return context;
        } catch (MetaMatrixCoreException e) {
            throw new MetaMatrixComponentException(e);
        }
    }
    
    /** 
     * @see com.metamatrix.dqp.client.ServerFacade#executeRequest(com.metamatrix.dqp.client.PortableContext, com.metamatrix.dqp.client.RequestInfo)
     * @since 4.2
     */
    public PortableContext executeRequest(PortableContext connectionContext, RequestInfo requestInfo) throws MetaMatrixComponentException, MetaMatrixProcessingException {
        ServerSessionContext context = ServerSessionContext.createSessionContextFromPortableContext(connectionContext);
        ServerRequest request = validateRequestInfo(requestInfo);
        RequestID requestID = new RequestID(nextRequestID());
        Future<ResultsMessage> results = null;
        try {
            ConnectionHolder holder = getConnection(context);
            RequestMessage message = createRequestMessage(context, request, requestID);
            results = holder.getDqp().executeRequest(requestID.getExecutionID(), message);
        } catch (MetaMatrixCoreException e) {
            throw new MetaMatrixComponentException(e);
        }

        try {
			results.get();
		} catch (InterruptedException e) {
			throw new MetaMatrixComponentException(e);
		} catch (ExecutionException e) {
			//TODO: throw proper exception
			throw new MetaMatrixComponentException(e);
		}
        return new RequestContext(requestID, SqlUtil.isUpdateSql(request.getSql()));
    }
    
    /** 
     * @see com.metamatrix.dqp.client.ServerFacade#getBatch(com.metamatrix.dqp.client.PortableContext, com.metamatrix.dqp.client.PortableContext, int, int, int)
     * @since 4.2
     */
    public Results getBatch(PortableContext connectionContext, PortableContext requestContext,
                            int beginRow, int maxEndRow, int waitTime) throws MetaMatrixComponentException, MetaMatrixProcessingException {
        if (beginRow <= 0) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ServerFacadeImpl.invalid_beginRow", beginRow)); //$NON-NLS-1$
        } else if (maxEndRow <beginRow) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ServerFacadeImpl.invalid_endRow", maxEndRow)); //$NON-NLS-1$
        } else if (waitTime < 0) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ServerFacadeImpl.invalid_waitTime", waitTime)); //$NON-NLS-1$
        }
        ServerSessionContext context = ServerSessionContext.createSessionContextFromPortableContext(connectionContext);
        RequestContext reqContext = RequestContext.createRequestContextFromPortableContext(requestContext);
        ConnectionHolder holder = null;
        Future<ResultsMessage> receiver = null;
        try {
            holder = getConnection(context);
            receiver = holder.getDqp().processCursorRequest(reqContext.getRequestID().getExecutionID(), beginRow, maxEndRow);
        } catch (MetaMatrixCoreException e) {
            throw new MetaMatrixComponentException(e);
        }
        ResultsMessage results;
		try {
			results = receiver.get(waitTime, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			throw new MetaMatrixComponentException(e);
		} catch (ExecutionException e) {
			throw new MetaMatrixComponentException(e);
		} catch (TimeoutException e) {
			return null;
		} 
        return new ResultBatch(results, reqContext.isUpdate(), reqContext.getRequestID().getExecutionID(), holder);
    }

    /** 
     * @see com.metamatrix.dqp.client.ServerFacade#getMetadata(com.metamatrix.dqp.client.PortableContext, com.metamatrix.dqp.client.PortableContext)
     * @since 4.2
     */
    public ResultsMetadata getMetadata(PortableContext connectionContext, PortableContext requestContext) throws MetaMatrixComponentException, MetaMatrixProcessingException {
        ServerSessionContext context = ServerSessionContext.createSessionContextFromPortableContext(connectionContext);
        RequestContext reqContext = RequestContext.createRequestContextFromPortableContext(requestContext);
        if (reqContext.isUpdate()) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ServerFacadeImpl.updates_not_supported")); //$NON-NLS-1$
        }
        try {
        	ConnectionHolder holder = getConnection(context);
            MetadataResult result = holder.getDqp().getMetadata(reqContext.getRequestID().getExecutionID());
            return new ResultsMetadataImpl(result.getColumnMetadata());

        } catch (CommunicationException e) {
        	throw new MetaMatrixComponentException(e);
        } catch (ConnectionException e) {
        	throw new MetaMatrixComponentException(e);
		}
    }

    
    public void cancelRequest(PortableContext connectionContext, PortableContext requestContext) throws MetaMatrixComponentException, MetaMatrixProcessingException {
        ServerSessionContext context = ServerSessionContext.createSessionContextFromPortableContext(connectionContext);
        RequestContext reqContext = RequestContext.createRequestContextFromPortableContext(requestContext);
        try {
            ConnectionHolder holder = getConnection(context);
            holder.getDqp().cancelRequest(reqContext.getRequestID().getExecutionID());
        } catch (MetaMatrixCoreException e) {
            throw new MetaMatrixComponentException(e);
        }
    }

    /** 
     * @see com.metamatrix.dqp.client.ServerFacade#closeRequest(com.metamatrix.dqp.client.PortableContext, com.metamatrix.dqp.client.PortableContext)
     * @since 4.2
     */
    public void closeRequest(PortableContext connectionContext, PortableContext requestContext) throws MetaMatrixComponentException, MetaMatrixProcessingException {
        ServerSessionContext context = ServerSessionContext.createSessionContextFromPortableContext(connectionContext);
        RequestContext reqContext = RequestContext.createRequestContextFromPortableContext(requestContext);
        try {
            ConnectionHolder holder = getConnection(context);
            holder.getDqp().closeRequest(reqContext.getRequestID().getExecutionID());
        } catch (MetaMatrixCoreException e) {
            throw new MetaMatrixComponentException(e);
        }
    }

    /** 
     * @see com.metamatrix.dqp.client.ServerFacade#closeSession(com.metamatrix.dqp.client.PortableContext)
     * @since 4.2
     */
    public void closeSession(PortableContext connectionContext) throws MetaMatrixComponentException, MetaMatrixProcessingException {
        ServerSessionContext context = ServerSessionContext.createSessionContextFromPortableContext(connectionContext);
        try {
            ConnectionHolder holder = getConnection(context);
            connections.remove(context);
            holder.getServiceRegistry().shutdown();
        } catch (MetaMatrixCoreException e) {
            throw new MetaMatrixComponentException(e);
        }
    }
    
    private ServerConnectionInfo validateConnectionInfo(ConnectionInfo connectionInfo) throws MetaMatrixProcessingException {
        if (connectionInfo == null) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ServerFacadeImpl.null_connInfo")); //$NON-NLS-1$
        } else if (!(connectionInfo instanceof ServerConnectionInfo)) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ServerFacadeImpl.invalid_connInfo")); //$NON-NLS-1$
        }
        ServerConnectionInfo info = (ServerConnectionInfo)connectionInfo;
        if (info.getServerUrl() == null || info.getServerUrl().length() == 0) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ServerFacadeImpl.null_URL")); //$NON-NLS-1$
        } else if (info.getUser() == null || info.getUser().length() == 0) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ServerFacadeImpl.null_user")); //$NON-NLS-1$
        } else if (info.getPassword() == null || info.getPassword().length() == 0) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ServerFacadeImpl.null_pwd")); //$NON-NLS-1$
        } else if (info.getVDBName() == null || info.getVDBName().length() == 0) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ServerFacadeImpl.null_vdb")); //$NON-NLS-1$
        }
        return info;
    }
    
    private ServerRequest validateRequestInfo(RequestInfo requestInfo) throws MetaMatrixProcessingException {
        if (requestInfo == null) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ServerFacadeImpl.null_requestInfo")); //$NON-NLS-1$
        } else if (!(requestInfo instanceof ServerRequest)) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ServerFacadeImpl.invalid_requestInfo")); //$NON-NLS-1$
        }
        ServerRequest request = (ServerRequest)requestInfo;
        if (request.getCursorType() != ResultSet.TYPE_FORWARD_ONLY &&
            request.getCursorType() != ResultSet.TYPE_SCROLL_INSENSITIVE) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ServerFacadeImpl.invalid_cursorType")); //$NON-NLS-1$
        } else if (request.getFetchSize() <= 0) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ServerFacadeImpl.invalid_fetchSize")); //$NON-NLS-1$
        } else if (request.getRequestType() != RequestInfo.REQUEST_TYPE_STATEMENT &&
                   request.getRequestType() != RequestInfo.REQUEST_TYPE_PREPARED_STATEMENT &&
                   request.getRequestType() != RequestInfo.REQUEST_TYPE_CALLABLE_STATEMENT) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ServerFacadeImpl.invalid_requestType")); //$NON-NLS-1$
        } else if (request.getSql() == null || request.getSql().length() == 0) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ServerFacadeImpl.invalid_sql")); //$NON-NLS-1$
        } else if (request.getTransactionAutoWrapMode() != RequestInfo.AUTOWRAP_OFF &&
                   request.getTransactionAutoWrapMode() != RequestInfo.AUTOWRAP_ON &&
                   request.getTransactionAutoWrapMode() != RequestInfo.AUTOWRAP_OPTIMISTIC &&
                   request.getTransactionAutoWrapMode() != RequestInfo.AUTOWRAP_PESSIMISTIC) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ServerFacadeImpl.invalid_txnAutowrap")); //$NON-NLS-1$
        }
            
        
        return request;
    }

    private RequestMessage createRequestMessage(ServerSessionContext context, ServerRequest info, RequestID requestID) throws MetaMatrixProcessingException {
        // Create a request message
        RequestMessage request = new RequestMessage(info.getSql());
        request.markSubmissionStart();
        request.setQueryPlanAllowed(false);
        // TODO Make changes so support doubleQuotes

        if (info.getRequestType() == ServerRequest.REQUEST_TYPE_PREPARED_STATEMENT) {
            request.setPreparedStatement(true);
            Object [] params = info.getBindParameters();
            if (params != null) {
                request.setParameterValues(Arrays.asList(params));
            }
        } else if (info.getRequestType() == ServerRequest.REQUEST_TYPE_CALLABLE_STATEMENT) {
            request.setCallableStatement(true);
            Object [] params = info.getBindParameters();
            if (params != null) {
                request.setParameterValues(Arrays.asList(params));
            }
        }

//        startTimer(info.timeout*1000, new Long(currentRequestID));
        request.setFetchSize(info.getFetchSize());
        request.setStyleSheet(info.getXMLStyleSheet());

        // Get partial mode
        request.setPartialResults(info.getPartialResults());

        // Get fetch size
        request.setFetchSize(info.getFetchSize());

        // Get cursor type
        request.setCursorType(info.getCursorType());

        // Get xml validation mode
        request.setValidationMode(info.getXMLValidationMode());

        // Get xml format mode
        request.setXMLFormat(info.getXMLFormat());

        // Get transaction auto-wrap mode
        int transactionAutowrap = info.getTransactionAutoWrapMode();
        String autowrap = null;
        switch(transactionAutowrap) {
            case ServerRequest.AUTOWRAP_OFF:         autowrap = ExecutionProperties.AUTO_WRAP_OFF;         break;
            case ServerRequest.AUTOWRAP_ON:          autowrap = ExecutionProperties.AUTO_WRAP_ON;          break;
            case ServerRequest.AUTOWRAP_OPTIMISTIC:  autowrap = ExecutionProperties.AUTO_WRAP_OPTIMISTIC;  break;
            case ServerRequest.AUTOWRAP_PESSIMISTIC: autowrap = ExecutionProperties.AUTO_WRAP_PESSIMISTIC; break;
            default: throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ServerFacadeImpl.invalid_txnautowrap", transactionAutowrap)); //$NON-NLS-1$
        }
        
        request.setTxnAutoWrapMode(autowrap);
        
        // Get result set cache mode
        request.setUseResultSetCache(info.getUseResultSetCache());
        
        // Mark this request synchronous
        request.setSynchronousRequest(true);

        return request;
    }
    
    private ConnectionHolder getConnection(ServerSessionContext context) throws CommunicationException, ConnectionException, LogonException {
        ConnectionHolder holder = null;
        synchronized(connections) {
            holder = (ConnectionHolder)connections.get(context);
            if (holder == null) {
                holder = new ConnectionHolder(null);
                connections.put(context, holder);
            }
        }
        synchronized (holder) {
			if (holder.getServiceRegistry() == null) {
				holder.setServiceRegistry(connectionFactory.createConnection(new MMURL(context.getConnectionContext()), context.getConnectionProperties()));
			}
		}
        return holder;
    }
    
    private long nextRequestID() {
        return requestIDGenerator.getAndIncrement();
    }
}
