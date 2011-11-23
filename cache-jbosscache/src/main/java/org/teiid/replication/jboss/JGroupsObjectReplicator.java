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

package org.teiid.replication.jboss;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.jboss.as.clustering.jgroups.ChannelFactory;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.MethodLookup;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Promise;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.teiid.Replicated;
import org.teiid.Replicated.ReplicationMode;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.ObjectReplicator;
import org.teiid.query.ReplicatedObject;

public class JGroupsObjectReplicator implements ObjectReplicator, Serializable {
	
	private final class ReplicatorRpcDispatcher<S> extends RpcDispatcher {
		private final S object;
		private final HashMap<Method, Short> methodMap;
		private final ArrayList<Method> methodList;
		Map<List<?>, JGroupsInputStream> inputStreams = new ConcurrentHashMap<List<?>, JGroupsInputStream>();

		private ReplicatorRpcDispatcher(Channel channel, MessageListener l,
				MembershipListener l2, Object serverObj, S object,
				HashMap<Method, Short> methodMap, ArrayList<Method> methodList) {
			super(channel, l, l2, serverObj);
			this.object = object;
			this.methodMap = methodMap;
			this.methodList = methodList;
		}

		@Override
		public Object handle(Message req) {
			Object      body=null;

		    if(req == null || req.getLength() == 0) {
		        if(log.isErrorEnabled()) log.error("message or message buffer is null"); //$NON-NLS-1$
		        return null;
		    }
		    
		    try {
		        body=req_marshaller != null?
		                req_marshaller.objectFromBuffer(req.getBuffer(), req.getOffset(), req.getLength())
		                : req.getObject();
		    }
		    catch(Throwable e) {
		        if(log.isErrorEnabled()) log.error("exception marshalling object", e); //$NON-NLS-1$
		        return e;
		    }

		    if(!(body instanceof MethodCall)) {
		        if(log.isErrorEnabled()) log.error("message does not contain a MethodCall object"); //$NON-NLS-1$

		        // create an exception to represent this and return it
		        return  new IllegalArgumentException("message does not contain a MethodCall object") ; //$NON-NLS-1$
		    }

		    final MethodCall method_call=(MethodCall)body;

		    try {
		        if(log.isTraceEnabled())
		            log.trace("[sender=" + req.getSrc() + "], method_call: " + method_call); //$NON-NLS-1$ //$NON-NLS-2$

		        if(method_lookup == null)
		            throw new Exception("MethodCall uses ID=" + method_call.getId() + ", but method_lookup has not been set"); //$NON-NLS-1$ //$NON-NLS-2$

		        if (method_call.getId() >= methodList.size() - 3) {
		        	Serializable address = new AddressWrapper(req.getSrc());
		        	Serializable stateId = (Serializable)method_call.getArgs()[0];
		        	List<?> key = Arrays.asList(stateId, address);
		        	JGroupsInputStream is = inputStreams.get(key);
		        	if (method_call.getId() == methodList.size() - 3) {
		        		LogManager.logTrace(LogConstants.CTX_RUNTIME, object, "create state", stateId); //$NON-NLS-1$
		        		if (is != null) {
		        			is.receive(null);
		        		}
		        		is = new JGroupsInputStream(15000);
		        		this.inputStreams.put(key, is);
		        		executor.execute(new StreamingRunner(object, stateId, is, null));
		        	} else if (method_call.getId() == methodList.size() - 2) {
		        		LogManager.logTrace(LogConstants.CTX_RUNTIME, object, "building state", stateId); //$NON-NLS-1$
		        		if (is != null) {
		        			is.receive((byte[])method_call.getArgs()[1]);
		        		}
		        	} else if (method_call.getId() == methodList.size() - 1) {
		        		LogManager.logTrace(LogConstants.CTX_RUNTIME, object, "finished state", stateId); //$NON-NLS-1$
		        		if (is != null) {
		        			is.receive(null);
		        		}
		        		this.inputStreams.remove(key);
		        	}  
		        	return null;
		        } else if (method_call.getId() == methodList.size() - 5) {
		        	//hasState
		        	ReplicatedObject ro = (ReplicatedObject)object;
		        	Serializable stateId = (Serializable)method_call.getArgs()[0];
		        	
		        	if (ro.hasState(stateId)) {
		        		return Boolean.TRUE;
		        	}
		        	return null;
		        } else if (method_call.getId() == methodList.size() - 4) {
		        	//sendState
		        	ReplicatedObject ro = (ReplicatedObject)object;
		        	String stateId = (String)method_call.getArgs()[0];
		        	AddressWrapper dest = (AddressWrapper)method_call.getArgs()[1];
		        	
		        	JGroupsOutputStream oStream = new JGroupsOutputStream(this, Arrays.asList(dest.address), stateId, (short)(methodMap.size() - 3), false);
					try {
						ro.getState(stateId, oStream);
					} finally {
						oStream.close();
					}
					LogManager.logTrace(LogConstants.CTX_RUNTIME, object, "sent state", stateId); //$NON-NLS-1$
			        return null;
		        }
		        
		        Method m=method_lookup.findMethod(method_call.getId());
		        if(m == null)
		            throw new Exception("no method found for " + method_call.getId()); //$NON-NLS-1$
		        method_call.setMethod(m);
		        
		    	return method_call.invoke(server_obj);
		    }
		    catch(Throwable x) {
		        return x;
		    }
		}
	}

	private static final long serialVersionUID = -6851804958313095166L;
	private static final String HAS_STATE = "hasState"; //$NON-NLS-1$
	private static final String SEND_STATE = "sendState"; //$NON-NLS-1$
	private static final String CREATE_STATE = "createState"; //$NON-NLS-1$
	private static final String BUILD_STATE = "buildState"; //$NON-NLS-1$
	private static final String FINISH_STATE = "finishState"; //$NON-NLS-1$

	private final static class StreamingRunner implements Runnable {
		private final Object object;
		private final Serializable stateId;
		private final JGroupsInputStream is;
		private Promise<Boolean> promise;

		private StreamingRunner(Object object, Serializable stateId, JGroupsInputStream is, Promise<Boolean> promise) {
			this.object = object;
			this.stateId = stateId;
			this.is = is;
			this.promise = promise;
		}

		@Override
		public void run() {
			try {
				((ReplicatedObject)object).setState(stateId, is);
				if (promise != null) { 
					promise.setResult(Boolean.TRUE);
				}
				LogManager.logDetail(LogConstants.CTX_RUNTIME, "state set " + stateId); //$NON-NLS-1$
			} catch (Exception e) {
				if (promise != null) {
					promise.setResult(Boolean.FALSE);
				}
				LogManager.logError(LogConstants.CTX_RUNTIME, e, "error setting state " + stateId); //$NON-NLS-1$
			} finally {
				is.close();
			}
		}
	}

	private final class ReplicatedInvocationHandler<S> extends ReceiverAdapter implements
			InvocationHandler, Serializable {
		
		private static final int PULL_RETRIES = 3;
		private static final long serialVersionUID = -2943462899945966103L;
		private final S object;
		private transient ReplicatorRpcDispatcher<S> disp;
		private final HashMap<Method, Short> methodMap;
	    protected List<Address> remoteMembers = new ArrayList<Address>();
	    protected final transient Promise<Boolean> state_promise=new Promise<Boolean>();
		private Map<Serializable, Promise<Boolean>> loadingStates = new HashMap<Serializable, Promise<Boolean>>();
	    
		private ReplicatedInvocationHandler(S object,HashMap<Method, Short> methodMap) {
			this.object = object;
			this.methodMap = methodMap;
		}
		
		public void setDisp(ReplicatorRpcDispatcher<S> disp) {
			this.disp = disp;
		}
		
		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			Short methodNum = methodMap.get(method);
			if (methodNum == null || remoteMembers.isEmpty()) {
				if (methodNum != null) {
			    	Replicated annotation = method.getAnnotation(Replicated.class);
			    	if (annotation != null && annotation.remoteOnly()) {
			    		return null;
			    	}
				}
				try {
					return method.invoke(object, args);
				} catch (InvocationTargetException e) {
					throw e.getCause();
				}
			}
		    try {
		    	Replicated annotation = method.getAnnotation(Replicated.class);
		    	if (annotation.replicateState() != ReplicationMode.NONE) {
		    		return handleReplicateState(method, args, annotation);
				}
		        MethodCall call=new MethodCall(methodNum, args);
		        ArrayList<Address> dests = null;
		        if (annotation.remoteOnly()) {
					synchronized (remoteMembers) {
						dests = new ArrayList<Address>(remoteMembers);
					}
		        }
		        RspList<Object> responses = disp.callRemoteMethods(dests, call, new RequestOptions().setMode(annotation.asynch()?ResponseMode.GET_NONE:ResponseMode.GET_ALL).setTimeout(annotation.timeout()).setAnycasting(dests != null));
		        if (annotation.asynch()) {
			        return null;
		        }
		        List<Object> results = responses.getResults();
		        if (method.getReturnType() == boolean.class) {
		        	for (Object o : results) {
						if (!Boolean.TRUE.equals(o)) {
							return false;
						}
					}
		        	return true;
		        } else if (method.getReturnType() == Collection.class) {
		        	ArrayList<Object> result = new ArrayList<Object>();
		        	for (Object o : results) {
		        		result.addAll((Collection)o);
					}
		        	return results;
		        }
	        	return null;
		    } catch(Exception e) {
		        throw new RuntimeException(method + " " + args + " failed", e); //$NON-NLS-1$ //$NON-NLS-2$
		    }
		}

		private Object handleReplicateState(Method method, Object[] args,
				Replicated annotation) throws IllegalAccessException,
				Throwable, IOException, IllegalStateException, Exception {
			Object result = null;
			try {
				result = method.invoke(object, args);
			} catch (InvocationTargetException e) {
				throw e.getCause();
			}
			List<Address> dests = null;
			synchronized (remoteMembers) {
				dests = new ArrayList<Address>(remoteMembers);
			}
			ReplicatedObject ro = (ReplicatedObject)object;
			Serializable stateId = (Serializable)args[0];
			if (annotation.replicateState() == ReplicationMode.PUSH) {
				LogManager.logDetail(LogConstants.CTX_RUNTIME, object, "replicating state", stateId); //$NON-NLS-1$
				JGroupsOutputStream oStream = new JGroupsOutputStream(disp, dests, stateId, (short)(methodMap.size() - 3), true);
				try {
					ro.getState(stateId, oStream);
				} finally {
					oStream.close();
				}
				LogManager.logTrace(LogConstants.CTX_RUNTIME, object, "sent state", stateId); //$NON-NLS-1$
			    return result;
			}
			if (result != null) {
				return result;
			}
			if (!(object instanceof ReplicatedObject)) {
				throw new IllegalStateException("A non-ReplicatedObject cannot use state pulling."); //$NON-NLS-1$
			}
			for (int i = 0; i < PULL_RETRIES; i++) {
				Promise<Boolean> p = null;
				boolean wait = true;
				synchronized (loadingStates) {
					p = loadingStates.get(stateId);
					if (p == null) {
						wait = false;
						try {
							result = method.invoke(object, args);
						} catch (InvocationTargetException e) {
							throw e.getCause();
						}
						if (result != null) {
							return result;
						}
						p = new Promise<Boolean>();
						loadingStates.put(stateId, p);
					}
				}
				long timeout = annotation.timeout();
				if (wait) {
					p.getResult(timeout);
					continue;
				}
				try {
					LogManager.logDetail(LogConstants.CTX_RUNTIME, object, "pulling state", stateId); //$NON-NLS-1$
					RspList<Boolean> resp = this.disp.callRemoteMethods(null, new MethodCall((short)(methodMap.size() - 5), stateId), new RequestOptions(ResponseMode.GET_ALL, timeout));
					Collection<Rsp<Boolean>> values = resp.values();
					Rsp<Boolean> rsp = null;
					for (Rsp<Boolean> response : values) {
						if (Boolean.TRUE.equals(response.getValue())) {
							rsp = response;
							break;
						}
					}
					if (rsp == null || this.disp.getChannel().getAddress().equals(rsp.getSender())) {
						break;
					}
					JGroupsInputStream is = new JGroupsInputStream(15000);
					StreamingRunner runner = new StreamingRunner(object, stateId, is, p);
					List<?> key = Arrays.asList(stateId, new AddressWrapper(rsp.getSender()));
					disp.inputStreams.put(key, is);
					executor.execute(runner);
					
					this.disp.callRemoteMethod(rsp.getSender(), new MethodCall((short)(methodMap.size() - 4), stateId, new AddressWrapper(this.disp.getChannel().getAddress())), new RequestOptions(ResponseMode.GET_NONE, 0).setAnycasting(true));
					
					Boolean fetched = p.getResult(timeout);

					if (fetched != null) {
						if (fetched) {
							LogManager.logDetail(LogConstants.CTX_RUNTIME, object, "pulled state", stateId); //$NON-NLS-1$
						} else {
							LogManager.logWarning(LogConstants.CTX_RUNTIME, object + " failed to pull " + stateId); //$NON-NLS-1$
						}
					} else {
						LogManager.logWarning(LogConstants.CTX_RUNTIME, object + " timeout pulling " + stateId); //$NON-NLS-1$
					}
					try {
						result = method.invoke(object, args);
					} catch (InvocationTargetException e) {
						throw e.getCause();
					}
				} finally {
					synchronized (loadingStates) {
						loadingStates.remove(stateId);
					}
				}
			}
			return null; //could not fetch the remote state
		}
		
		@Override
		public void viewAccepted(View newView) {
			if (newView.getMembers() != null) {
				synchronized (remoteMembers) {
					remoteMembers.removeAll(newView.getMembers());
					if (object instanceof ReplicatedObject && !remoteMembers.isEmpty()) {
						HashSet<Serializable> dropped = new HashSet<Serializable>();
						for (Address address : remoteMembers) {
							dropped.add(new AddressWrapper(address));
						}
						((ReplicatedObject)object).droppedMembers(dropped);
					}
					remoteMembers.clear();
					remoteMembers.addAll(newView.getMembers());
					remoteMembers.remove(this.disp.getChannel().getAddress());
				}
			}
		}
		
		@Override
		public void setState(InputStream istream) {
			LogManager.logDetail(LogConstants.CTX_RUNTIME, object, "loading initial state"); //$NON-NLS-1$
			try {
				((ReplicatedObject)object).setState(istream);
				state_promise.setResult(Boolean.TRUE);
			} catch (Exception e) {
				state_promise.setResult(Boolean.FALSE);
				LogManager.logError(LogConstants.CTX_RUNTIME, e, "error loading initial state"); //$NON-NLS-1$
			} finally {
				Util.close(istream);
			}
		}
		
		@Override
		public void getState(OutputStream ostream) {
			LogManager.logDetail(LogConstants.CTX_RUNTIME, object, "getting initial state"); //$NON-NLS-1$
			try {
				((ReplicatedObject)object).getState(ostream);
			} catch (Exception e) {
				LogManager.logError(LogConstants.CTX_RUNTIME, e, "error gettting initial state"); //$NON-NLS-1$
			} finally {
				Util.close(ostream);
			}
		}
		
	}
	
	private interface Streaming {
		void sendState(Serializable id, AddressWrapper dest);
		void createState(Serializable id);
		void buildState(Serializable id, byte[] bytes);
		void finishState(Serializable id);
	}

	//TODO: this should be configurable, or use a common executor
	private transient Executor executor = Executors.newCachedThreadPool();
	private transient ChannelFactory channelFactory;

	public JGroupsObjectReplicator(ChannelFactory channelFactory) {
		this.channelFactory = channelFactory;
	}
	
	public void stop(Object object) {
		if (!Proxy.isProxyClass(object.getClass())) {
			return;
		}
		ReplicatedInvocationHandler<?> handler = (ReplicatedInvocationHandler<?>) Proxy.getInvocationHandler(object);
		Channel c = handler.disp.getChannel();
		handler.disp.stop();
		c.close();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T, S> T replicate(String mux_id,
			Class<T> iface, final S object, long startTimeout) throws Exception {
		Channel channel = channelFactory.createChannel(mux_id);
		Method[] methods = iface.getMethods();
		
		final HashMap<Method, Short> methodMap = new HashMap<Method, Short>();
		final ArrayList<Method> methodList = new ArrayList<Method>();
		
		for (Method method : methods) {
			if (method.getAnnotation(Replicated.class) == null) {
				continue;
			}
			methodList.add(method);
			methodMap.put(method, (short)(methodList.size() - 1));
		}
		
		Method hasState = ReplicatedObject.class.getMethod(HAS_STATE, new Class<?>[] {Serializable.class});
		methodList.add(hasState);
		methodMap.put(hasState, (short)(methodList.size() - 1));
		
		Method sendState = JGroupsObjectReplicator.Streaming.class.getMethod(SEND_STATE, new Class<?>[] {Serializable.class, AddressWrapper.class});
		methodList.add(sendState);
		methodMap.put(sendState, (short)(methodList.size() - 1));
		
		//add in streaming methods
		Method createState = JGroupsObjectReplicator.Streaming.class.getMethod(CREATE_STATE, new Class<?>[] {Serializable.class});
		methodList.add(createState);
		methodMap.put(createState, (short)(methodList.size() - 1));
		Method buildState = JGroupsObjectReplicator.Streaming.class.getMethod(BUILD_STATE, new Class<?>[] {Serializable.class, byte[].class});
		methodList.add(buildState);
		methodMap.put(buildState, (short)(methodList.size() - 1));
		Method finishState = JGroupsObjectReplicator.Streaming.class.getMethod(FINISH_STATE, new Class<?>[] {Serializable.class});
		methodList.add(finishState);
		methodMap.put(finishState, (short)(methodList.size() - 1));
		
        ReplicatedInvocationHandler<S> proxy = new ReplicatedInvocationHandler<S>(object, methodMap);
        /*
         * TODO: could have an object implement streaming
         * Override the normal handle method to support streaming
         */
        ReplicatorRpcDispatcher disp = new ReplicatorRpcDispatcher<S>(channel, proxy, proxy, object,
				object, methodMap, methodList);
		
		proxy.setDisp(disp);
        disp.setMethodLookup(new MethodLookup() {
            public Method findMethod(short id) {
                return methodList.get(id);
            }
        });
        
		T replicatedProxy = (T) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {iface}, proxy);
		boolean success = false;
		try {
			channel.connect(mux_id);
			if (object instanceof ReplicatedObject) {
				((ReplicatedObject)object).setAddress(new AddressWrapper(channel.getAddress()));
				channel.getState(null, startTimeout);
				Boolean loaded = proxy.state_promise.getResult(1);
				if (loaded == null) {
					LogManager.logInfo(LogConstants.CTX_RUNTIME, object + " timeout exceeded or first member"); //$NON-NLS-1$
				} else if (loaded) {
					LogManager.logDetail(LogConstants.CTX_RUNTIME, object, "loaded"); //$NON-NLS-1$
				} else {
					LogManager.logWarning(LogConstants.CTX_RUNTIME, object + " load error"); //$NON-NLS-1$
				}
			}
			success = true;
			return replicatedProxy;
		} finally {
			if (!success) {
				channel.close();
			}
		}
	}
	
}
