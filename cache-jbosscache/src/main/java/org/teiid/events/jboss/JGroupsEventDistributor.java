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
 
 package org.teiid.events.jboss;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Vector;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.util.naming.Util;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.JChannelFactory;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RpcDispatcher;
import org.teiid.events.EventDistributor;

public class JGroupsEventDistributor extends ReceiverAdapter implements Serializable {
	
	private static final long serialVersionUID = -1140683411842561358L;
	
	private transient JChannelFactory channelFactory;
	private String multiplexerStack;
	private String clusterName;
	private String jndiName;
	private String localEventDistributorName;
	
	private transient EventDistributor proxyEventDistributor;
	private transient EventDistributor localEventDistributor;
	
	private transient Channel channel;
	private transient RpcDispatcher rpcDispatcher;
	private transient Vector<Address> members;
	
	public JChannelFactory getChannelFactory() {
		return channelFactory;
	}
	
	public void setJndiName(String jndiName) {
		this.jndiName = jndiName;
	}
	
	public String getJndiName() {
		return jndiName;
	}
	
	public String getLocalEventDistributorName() {
		return localEventDistributorName;
	}
	
	public void setLocalEventDistributorName(String localEventDistributorName) {
		this.localEventDistributorName = localEventDistributorName;
	}
	
	public String getMultiplexerStack() {
		return multiplexerStack;
	}
	
	public String getClusterName() {
		return clusterName;
	}
	
	public void setChannelFactory(JChannelFactory channelFactory) {
		this.channelFactory = channelFactory;
	}
	
	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}
	
	public void setMultiplexerStack(String multiplexerStack) {
		this.multiplexerStack = multiplexerStack;
	}
		
	public void start() throws Exception {
		if (this.channelFactory == null) {
			return; //no need to distribute events
		}
		channel = this.channelFactory.createMultiplexerChannel(this.multiplexerStack, null);
		channel.connect(this.clusterName);
		
		proxyEventDistributor = (EventDistributor) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {EventDistributor.class}, new InvocationHandler() {
			
			@Override
			public Object invoke(Object proxy, Method method, Object[] args)
					throws Throwable {
				rpcDispatcher.callRemoteMethods(members, new MethodCall(method, args), GroupRequest.GET_NONE, 0);
				return null;
			}
		});
		//wrap the local in a proxy to prevent unintended methods from being called
		rpcDispatcher = new RpcDispatcher(channel, this, this, Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {EventDistributor.class}, new InvocationHandler() {
			
			@Override
			public Object invoke(Object proxy, Method method, Object[] args)
					throws Throwable {
				EventDistributor local = getLocalEventDistributor();
				if (local == null) {
					return null;
				}
				return method.invoke(local, args);
			}
		}));
		rpcDispatcher.setDeadlockDetection(false);
    	if (jndiName != null) {
	    	final InitialContext ic = new InitialContext();
    		Util.bind(ic, jndiName, proxyEventDistributor);
    	}
	}
	
	private EventDistributor getLocalEventDistributor() {
		if (localEventDistributor == null && this.localEventDistributorName != null) {
			try {
				Context ctx = new InitialContext();
				return (EventDistributor) ctx.lookup(this.localEventDistributorName);
			} catch (NamingException e) {
				return null;
			}
		}
		return localEventDistributor;
	}
	
	@Override
	public void viewAccepted(View newView) {
		Vector<Address> new_members = new Vector<Address>(newView.getMembers());
		new_members.remove(this.channel.getLocalAddress());
		this.members = new_members; 
	}
	
	public void stop() {
    	if (jndiName != null) {
	    	final InitialContext ic ;
	    	try {
	    		ic = new InitialContext() ;
	    		Util.unbind(ic, jndiName) ;
	    	} catch (final NamingException ne) {
	    	}
    	}
		if (this.channel != null) {
			this.channel.close();
			this.rpcDispatcher.stop();
		}
	}

}
