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

package com.metamatrix.common.messaging.jgroups;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.EventObject;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.RspList;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.common.messaging.MessagingException;
import com.metamatrix.common.messaging.RemoteMessagingException;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.event.AsynchEventBroker;
import com.metamatrix.core.event.EventBroker;
import com.metamatrix.core.event.EventBrokerException;
import com.metamatrix.core.event.EventObjectListener;
import com.metamatrix.core.event.EventSourceException;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.server.ChannelProvider;
import com.metamatrix.server.Configuration;

@Singleton
public class JGroupsMessageBus implements MessageBus {
	
	public static final String MESSAGE_KEY = "MessageKey"; //$NON-NLS-1$
	public static final int REMOTE_TIMEOUT = 30000; 
	
	private Channel channel;
	private volatile boolean shutdown;
	
    private EventBroker eventBroker = new AsynchEventBroker("VMMessageBus"); //$NON-NLS-1$

	// these are original objects that implement
	private ConcurrentHashMap<UUID, RPCStruct> rpcStructs = new ConcurrentHashMap<UUID, RPCStruct>();
	
	private RpcDispatcher rpcDispatcher;
	
    @Inject
    public JGroupsMessageBus(ChannelProvider channelProvider, @Named(Configuration.CLUSTERNAME) final String clusterName) throws ChannelException {
		Channel c = channelProvider.get(ChannelProvider.ChannelID.RPC);
		
		if (c == null || !c.isOpen()) {
			throw new MetaMatrixRuntimeException("Channel is not open"); //$NON-NLS-1$
		}
		
		this.channel = c;
		
		ReceiverAdapter receiver = new ReceiverAdapter(){
			@Override
			public void receive(Message msg) {
				if (!msg.getSrc().equals(channel.getLocalAddress())) {
					eventBroker.processEvent((EventObject) msg.getObject());
		        }
			}

			@Override
			public void viewAccepted(View view) {
				super.viewAccepted(view);
				LogManager.logInfo(LogCommonConstants.CTX_CONTROLLER, view + "is added to cluster:" + clusterName); //$NON-NLS-1$
			}
		};
		
		this.rpcDispatcher = new RpcDispatcher(this.channel, receiver, receiver, new RemoteProxy(this.rpcStructs));
		this.channel.connect(clusterName);
	}

	public void unExport(Object object) {
		if (object == null) {
			return;
		}
		ArgCheck.isInstanceOf(RPCStruct.class, object);
		RPCStruct struct = (RPCStruct)object;
		this.rpcStructs.remove(struct.objectId);
	}
	
	public Serializable export(Object object, Class[] targetClasses) {
		if (object == null || shutdown) {
			return null;
		}
		RPCStruct struct = new RPCStruct(channel.getLocalAddress(), UUID.randomUUID(), targetClasses, object);
		this.rpcStructs.put(struct.objectId, struct);
		return struct;
	}
	
	public Object getRPCProxy(Object object) {
		if (object == null || shutdown) {
			return null;
		}
		ArgCheck.isInstanceOf(RPCStruct.class, object);
		final RPCStruct struct = (RPCStruct)object;
		final Vector dest = new Vector();
		dest.add(struct.address);

		return Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), struct.getTargetClasses(), new InvocationHandler() {

			public Object invoke(Object arg0, Method arg1, Object[] arg2) throws Throwable {
				
				Object[] invokeArgs = {struct.objectId, arg1.getName(), arg1.getParameterTypes(), arg2};
				RspList rsp_list = rpcDispatcher.callRemoteMethods(dest, new MethodCall(RemoteProxy.getInvokeMethod(), invokeArgs), GroupRequest.GET_FIRST, REMOTE_TIMEOUT);
				
				if (rsp_list.isEmpty()) {
					throw new RemoteMessagingException(PlatformPlugin.Util.getString("JGroupsMessageBus.noResponse")); //$NON-NLS-1$
				}
	
				Object result = rsp_list.getFirst();
				if (result instanceof Throwable) {
					throw (Throwable)result;
				}
				return result;
			}
		});
	}

	/**
	 * @see com.metamatrix.common.messaging.MessageBus#processEvent(java.util.EventObject)
	 */
	public void processEvent(EventObject obj) throws MessagingException {
		if (obj != null) {
			try {
				Message msg = new Message(null, null, obj);
				this.channel.send(msg);
			} catch (Exception e) {
				throw new MessagingException(e, ErrorMessageKeys.MESSAGING_ERR_0004, CommonPlugin.Util.getString(ErrorMessageKeys.MESSAGING_ERR_0004));
			}
		}
		eventBroker.processEvent(obj);
	}

	public synchronized void shutdown() throws MessagingException {
		if (shutdown) {
			return;
		}
		shutdown = true;
		this.channel.close();
		this.rpcDispatcher.stop();
		this.rpcStructs.clear();
		try {
			eventBroker.shutdown();
		} catch (EventBrokerException e) {
			throw new MessagingException(e, ErrorMessageKeys.MESSAGING_ERR_0014, CommonPlugin.Util.getString(ErrorMessageKeys.MESSAGING_ERR_0014));
		}
	}

	public void addListener(Class eventClass, EventObjectListener listener)
			throws MessagingException {
    	if (shutdown) {
    		return;
    	}
        try {
            eventBroker.addListener(eventClass, listener);
        } catch (EventSourceException e) {
            throw new MessagingException(e, ErrorMessageKeys.MESSAGING_ERR_0013, CommonPlugin.Util.getString(ErrorMessageKeys.MESSAGING_ERR_0013));
        }
	}

	public void removeListener(Class eventClass, EventObjectListener listener)
			throws MessagingException {
		if (shutdown) {
			return;
		}
		try {
			eventBroker.removeListener(eventClass, listener);
		} catch (EventSourceException e) {
			throw new MessagingException(e, ErrorMessageKeys.MESSAGING_ERR_0015, CommonPlugin.Util.getString(ErrorMessageKeys.MESSAGING_ERR_0015));
		}
	}

	public void removeListener(EventObjectListener listener)
			throws MessagingException {
		if (shutdown) {
    		return;
    	}
    	try {
    		eventBroker.removeListener(listener);
    	} catch (EventSourceException e) {
    		throw new MessagingException(e, ErrorMessageKeys.MESSAGING_ERR_0015, CommonPlugin.Util.getString(ErrorMessageKeys.MESSAGING_ERR_0015));
    	}
	}
}
