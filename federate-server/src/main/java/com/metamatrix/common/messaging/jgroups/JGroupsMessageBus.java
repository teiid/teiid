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

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.EventObject;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.ChannelListener;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.PullPushAdapter;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.RspList;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.common.messaging.MessagingException;
import com.metamatrix.common.messaging.RemoteMessagingException;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.event.EventBroker;
import com.metamatrix.core.event.EventObjectListener;
import com.metamatrix.core.util.ArgCheck;

public class JGroupsMessageBus implements MessageBus, ChannelListener {
	
	public static class RPCStruct implements Serializable {
		private static final long serialVersionUID = -7372264971977481565L;
		
		Class[] targetClasses;
		Address address;
		UUID objectId;
		transient RpcDispatcher disp;
		transient boolean shutdown;
		
		public RPCStruct(Address address, UUID objectId,
				Class[] targetClasses) {
			this.address = address;
			this.objectId = objectId;
			this.targetClasses = targetClasses;
		}		
		
	}

	public static final String MESSAGE_KEY = "MessageKey"; //$NON-NLS-1$
	public static final int REMOTE_TIMEOUT = 30000; //$NON-NLS-1$
		
	private Channel channel;
	private PullPushAdapter adapter;
	private EventBroker eventBroker;
	private volatile boolean shutdown;
	private ConcurrentHashMap<UUID, RPCStruct> rpcStructs = new ConcurrentHashMap<UUID, RPCStruct>();
	
	public JGroupsMessageBus(Channel channel, final EventBroker eventBroker) throws ChannelException {
		this.eventBroker = eventBroker;
		this.channel = channel;
		initChannel();
	}

	private synchronized void initChannel() throws ChannelException {
		if (channel != null && channel.isOpen()) {
			return;
		}
		adapter = new PullPushAdapter(channel);
		channel.addChannelListener(this);
		adapter.registerListener(MESSAGE_KEY, new ReceiverAdapter() {
			@Override
			public void receive(Message msg) {
				if (!msg.getSrc().equals(channel.getLocalAddress())) {
					eventBroker.processEvent((EventObject) msg.getObject());
		        }
			}
		});
	}
	
	public void unExport(Object object) {
		if (object == null) {
			return;
		}
		ArgCheck.isInstanceOf(RPCStruct.class, object);
		RPCStruct struct = (RPCStruct)object;
		rpcStructs.remove(struct.objectId);
		synchronized (struct) {
			struct.shutdown = true;
			if (struct.disp != null) {
				struct.disp.stop();
				adapter.unregisterListener(struct.objectId);
				struct.disp = null;
			}
		}
	}
	
	public Serializable export(Object object, Class[] targetClasses) {
		if (object == null || shutdown) {
			return null;
		}
		RPCStruct struct = new RPCStruct(channel.getLocalAddress(), UUID.randomUUID(), targetClasses);
		struct.disp = new RpcDispatcher(adapter, struct.objectId, null, null, object);
		return struct;
	}
	
	public Object getRPCProxy(Object object) {
		if (object == null || shutdown) {
			return null;
		}
		ArgCheck.isInstanceOf(RPCStruct.class, object);
		RPCStruct struct = (RPCStruct)object;
		final Vector dest = new Vector();
		dest.add(struct.address);
		RPCStruct existing = rpcStructs.putIfAbsent(struct.objectId, struct);
		if (existing == null) {
			existing = struct;
		}
		synchronized (existing) {
			if (existing.shutdown) {
				return null;
			}
			if (existing.disp == null) {
				existing.disp = new RpcDispatcher(adapter, struct.objectId, null, null, null);
			}
		}	
		final RpcDispatcher disp = existing.disp;
		return Proxy.newProxyInstance(Thread.currentThread()
				.getContextClassLoader(), struct.targetClasses,
				new InvocationHandler() {

					public Object invoke(Object arg0, Method arg1, Object[] arg2)
							throws Throwable {

						RspList rsp_list = disp.callRemoteMethods(dest, arg1
								.getName(), arg2, arg1.getParameterTypes(),
								GroupRequest.GET_FIRST, REMOTE_TIMEOUT);

						if (rsp_list.isEmpty()) {
							throw new RemoteMessagingException();
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
				adapter.send(MESSAGE_KEY, new Message(null, null, obj));
			} catch (Exception e) {
				throw new MessagingException(e, ErrorMessageKeys.MESSAGING_ERR_0004, CommonPlugin.Util.getString(ErrorMessageKeys.MESSAGING_ERR_0004));
			}
		}
	}

	public synchronized void shutdown() throws MessagingException {
		shutdown = true;
		getChannel().close();
		this.adapter.stop();
		this.rpcStructs.clear();
	}
    
    /**
     * Get the JGroups Channel used by this bus. 
     * @return JGroups Channel used by this bus.
     * @since 4.2
     */
    Channel getChannel() {
        return channel;
    }

	public void channelClosed(Channel arg0) {
		try {
			if (!shutdown) {
				initChannel();
			}
		} catch (ChannelException e) {
			throw new MetaMatrixRuntimeException(e);
		}
	}

	public void channelConnected(Channel arg0) {
	}

	public void channelDisconnected(Channel arg0) {
	}

	public void channelReconnected(Address arg0) {
	}

	public void channelShunned() {
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
	
}



