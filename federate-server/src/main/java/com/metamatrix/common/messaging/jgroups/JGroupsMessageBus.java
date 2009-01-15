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

package com.metamatrix.common.messaging.jgroups;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
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
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.RspList;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.common.messaging.MessagingException;
import com.metamatrix.common.messaging.RemoteMessagingException;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.event.EventBroker;
import com.metamatrix.core.event.EventObjectListener;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.server.ChannelProvider;

public class JGroupsMessageBus implements MessageBus {
	
	public static final String MESSAGE_KEY = "MessageKey"; //$NON-NLS-1$
	public static final int REMOTE_TIMEOUT = 30000; 
	static final FederateHeader MSG_HEADER = new FederateHeader(456188434); // with some random number	
	
	private Channel channel;
	private volatile boolean shutdown;

	// these are original objects that implement
	private ConcurrentHashMap<UUID, RPCStruct> rpcStructs = new ConcurrentHashMap<UUID, RPCStruct>();
	
	private RpcDispatcher rpcDispatcher;
	
	public JGroupsMessageBus(ChannelProvider channelProvider, final EventBroker eventBroker) throws ChannelException {
		Channel c = channelProvider.get(ChannelProvider.ChannelID.RPC);
		
		if (c == null || !c.isOpen()) {
			throw new MetaMatrixRuntimeException("Channel is not open"); //$NON-NLS-1$
		}
		
		this.channel = c;
		
		ReceiverAdapter receiver = new ReceiverAdapter(){
			@Override
			public void receive(Message msg) {
				if (!msg.getSrc().equals(channel.getLocalAddress())) {
					if (MSG_HEADER.equals(msg.getHeader(MESSAGE_KEY))) {
						eventBroker.processEvent((EventObject) msg.getObject());
					}
		        }
			}

			@Override
			public void viewAccepted(View view) {
				super.viewAccepted(view);
				LogManager.logInfo(LogCommonConstants.CTX_CONTROLLER, view + "is added"); //$NON-NLS-1$
			}
		};
		
		this.rpcDispatcher = new RpcDispatcher(this.channel, receiver, receiver, new RemoteProxy(this.rpcStructs));
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
	
				return rsp_list.getFirst();
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
				msg.putHeader(MESSAGE_KEY, MSG_HEADER);
				this.channel.send(msg);
			} catch (Exception e) {
				throw new MessagingException(e, ErrorMessageKeys.MESSAGING_ERR_0004, CommonPlugin.Util.getString(ErrorMessageKeys.MESSAGING_ERR_0004));
			}
		}
	}

	public synchronized void shutdown() throws MessagingException {
		shutdown = true;
		this.channel.close();
		this.rpcDispatcher.stop();
		this.rpcStructs.clear();
	}

	public void addListener(Class eventClass, EventObjectListener listener)
			throws MessagingException {
		
	}

	public void removeListener(Class eventClass, EventObjectListener listener)
			throws MessagingException {
		
	}

	public void removeListener(EventObjectListener listener)
			throws MessagingException {
		
	}
	
    public static class FederateHeader extends Header {
    	int type;
        public FederateHeader(int type) {
        	this.type = type;
        }
	
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(type);
        }
	
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readInt();
        }
    }	    
	
}



