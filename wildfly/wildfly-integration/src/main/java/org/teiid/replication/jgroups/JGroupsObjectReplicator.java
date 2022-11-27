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

package org.teiid.replication.jgroups;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import org.jgroups.Address;
import org.jgroups.JChannel;
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
import org.teiid.core.TeiidRuntimeException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.ObjectReplicator;
import org.teiid.query.ReplicatedObject;
import org.teiid.runtime.RuntimePlugin;

@SuppressWarnings("unchecked")
public class JGroupsObjectReplicator implements ObjectReplicator, Serializable {

    private static final int IO_TIMEOUT = 15000;
    private static final int STATE_TIMEOUT = 5000;

    private final class ReplicatorRpcDispatcher<S> extends RpcDispatcher {
        private final S object;
        private boolean initialized;
        private final HashMap<Method, Short> methodMap;
        private final ArrayList<Method> methodList;
        Map<List<?>, JGroupsInputStream> inputStreams = new ConcurrentHashMap<List<?>, JGroupsInputStream>();

        private ReplicatorRpcDispatcher(JChannel channel, MessageListener l,
                MembershipListener l2, Object serverObj, S object,
                HashMap<Method, Short> methodMap, ArrayList<Method> methodList) {
            super(channel, serverObj);
            this.setMembershipListener(l2);
            this.object = object;
            this.methodMap = methodMap;
            this.methodList = methodList;
        }

        @Override
        public Object handle(Message req) {
            Object      body=null;
            if(server_obj == null) {
                log.error(Util.getMessage("NoMethodHandlerIsRegisteredDiscardingRequest"));
                return null;
            }

            if(req == null || req.getLength() == 0) {
                log.error(Util.getMessage("MessageOrMessageBufferIsNull"));
                return null;
            }

            try {
                MethodCall method_call=methodCallFromBuffer(req.getRawBuffer(), req.getOffset(), req.getLength(), marshaller);

                if(log.isTraceEnabled())
                    log.trace("[sender=" + req.getSrc() + "], method_call: " + method_call); //$NON-NLS-1$ //$NON-NLS-2$

                if (method_call.getMethodId() >= methodList.size() - 5 && req.getSrc().equals(local_addr)) {
                    return null;
                }

                if (method_call.getMethodId() >= methodList.size() - 3) {
                    Address address = req.getSrc();
                    Serializable stateId = (Serializable)method_call.getArgs()[0];
                    List<?> key = Arrays.asList(stateId, address);
                    JGroupsInputStream is = inputStreams.get(key);
                    if (method_call.getMethodId() == methodList.size() - 3) {
                        LogManager.logTrace(LogConstants.CTX_RUNTIME, object, "create state", stateId); //$NON-NLS-1$
                        if (is != null) {
                            is.receive(null);
                        }
                        is = new JGroupsInputStream(IO_TIMEOUT);
                        this.inputStreams.put(key, is);
                        executor.execute(new StreamingRunner(object, stateId, is, null));
                    } else if (method_call.getMethodId() == methodList.size() - 2) {
                        LogManager.logTrace(LogConstants.CTX_RUNTIME, object, "building state", stateId); //$NON-NLS-1$
                        if (is != null) {
                            is.receive((byte[])method_call.getArgs()[1]);
                        }
                    } else if (method_call.getMethodId() == methodList.size() - 1) {
                        LogManager.logTrace(LogConstants.CTX_RUNTIME, object, "finished state", stateId); //$NON-NLS-1$
                        if (is != null) {
                            is.receive(null);
                        }
                        this.inputStreams.remove(key);
                    }
                    return null;
                } else if (method_call.getMethodId() == methodList.size() - 5) {
                    //hasState
                    ReplicatedObject ro = (ReplicatedObject)object;
                    Serializable stateId = (Serializable)method_call.getArgs()[0];

                    if (stateId == null) {
                        synchronized (this) {
                            if (initialized) {
                                return Boolean.TRUE;
                            }
                            return null;
                        }
                    }

                    if (ro.hasState(stateId)) {
                        return Boolean.TRUE;
                    }
                    return null;
                } else if (method_call.getMethodId() == methodList.size() - 4) {
                    //sendState
                    ReplicatedObject ro = (ReplicatedObject)object;
                    String stateId = (String)method_call.getArgs()[0];
                    Address dest = (Address)method_call.getArgs()[1];

                    JGroupsOutputStream oStream = new JGroupsOutputStream(this, Arrays.asList(dest), stateId, (short)(methodMap.size() - 3), false);
                    try {
                        if (stateId == null) {
                            ro.getState(oStream);
                        } else {
                            ro.getState(stateId, oStream);
                        }
                    } finally {
                        oStream.close();
                    }
                    LogManager.logTrace(LogConstants.CTX_RUNTIME, object, "sent state", stateId); //$NON-NLS-1$
                    return null;
                }

                Method m=method_lookup.findMethod(method_call.getMethodId());
                if(m == null)
                    throw new Exception("no method found for " + method_call.getMethodId()); //$NON-NLS-1$
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
                if (stateId == null) {
                    ((ReplicatedObject<?>)object).setState(is);
                } else {
                    ((ReplicatedObject)object).setState(stateId, is);
                }
                if (promise != null) {
                    promise.setResult(Boolean.TRUE);
                }
                LogManager.logDetail(LogConstants.CTX_RUNTIME, "state set", stateId); //$NON-NLS-1$
            } catch (Exception e) {
                if (promise != null) {
                    promise.setResult(Boolean.FALSE);
                }
                LogManager.logError(LogConstants.CTX_RUNTIME, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40101, stateId));
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
        protected List<Address> remoteMembers = Collections.synchronizedList(new ArrayList<Address>());
        private Map<Serializable, Promise<Boolean>> loadingStates = new HashMap<Serializable, Promise<Boolean>>();

        private ReplicatedInvocationHandler(S object,HashMap<Method, Short> methodMap) {
            this.object = object;
            this.methodMap = methodMap;
        }

        List<Address> getRemoteMembersCopy() {
            synchronized (remoteMembers) {
                return new ArrayList<Address>(remoteMembers);
            }
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
                List<Address> dests = null;
                if (annotation.remoteOnly()) {
                    dests = getRemoteMembersCopy();
                    if (dests.isEmpty()) {
                        return null;
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

        protected Address whereIsState(Serializable stateId, long timeout) throws Exception {
            if (remoteMembers.isEmpty()) {
                return null;
            }
            RspList<Boolean> resp = this.disp.callRemoteMethods(getRemoteMembersCopy(), new MethodCall((short)(methodMap.size() - 5), new Object[]{stateId}), new RequestOptions(ResponseMode.GET_ALL, timeout));
            Address addr = null;
            for (Map.Entry<Address, Rsp<Boolean>> response : resp.entrySet()) {
                if (Boolean.TRUE.equals(response.getValue().getValue())) {
                    addr = response.getKey();
                    break;
                }
            }
            return addr;
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
            ReplicatedObject ro = (ReplicatedObject)object;
            Serializable stateId = (Serializable)args[0];
            if (annotation.replicateState() == ReplicationMode.PUSH) {
                if (!remoteMembers.isEmpty()) {
                    LogManager.logDetail(LogConstants.CTX_RUNTIME, object, "replicating state", stateId); //$NON-NLS-1$
                    JGroupsOutputStream oStream = new JGroupsOutputStream(disp, null, stateId, (short)(methodMap.size() - 3), true);
                    try {
                        ro.getState(stateId, oStream);
                    } finally {
                        oStream.close();
                    }
                    LogManager.logTrace(LogConstants.CTX_RUNTIME, object, "sent state", stateId); //$NON-NLS-1$
                }
                return result;
            }
            if (result != null) {
                return result;
            }
            long timeout = annotation.timeout();
            return pullState(method, args, stateId, timeout, timeout);
        }

        /**
         * Pull the remote state.  The method and args are optional
         * to determine if the state has been made available.
         */
        Object pullState(Method method, Object[] args, Serializable stateId,
                long timeout, long stateDetectTimeout) throws Throwable {
            Object result = null;
            for (int i = 0; i < PULL_RETRIES; i++) {
                Promise<Boolean> p = null;
                boolean wait = true;
                synchronized (loadingStates) {
                    p = loadingStates.get(stateId);
                    if (p == null) {
                        wait = false;
                        if (method != null) {
                            try {
                                result = method.invoke(object, args);
                            } catch (InvocationTargetException e) {
                                throw e.getCause();
                            }
                            if (result != null) {
                                return result;
                            }
                        }
                        p = new Promise<Boolean>();
                        loadingStates.put(stateId, p);
                    }
                }
                if (wait) {
                    p.getResult(timeout);
                    continue;
                }
                try {
                    LogManager.logDetail(LogConstants.CTX_RUNTIME, object, "pulling state", stateId); //$NON-NLS-1$
                    Address addr = whereIsState(stateId, stateDetectTimeout);
                    if (addr == null) {
                        LogManager.logDetail(LogConstants.CTX_RUNTIME, object, "timeout exceeded or first member"); //$NON-NLS-1$
                        break;
                    }
                    JGroupsInputStream is = new JGroupsInputStream(IO_TIMEOUT);
                    StreamingRunner runner = new StreamingRunner(object, stateId, is, p);
                    List<?> key = Arrays.asList(stateId, addr);
                    disp.inputStreams.put(key, is);
                    executor.execute(runner);

                    this.disp.callRemoteMethod(addr, new MethodCall((short)(methodMap.size() - 4), stateId, this.disp.getChannel().getAddress()), new RequestOptions(ResponseMode.GET_NONE, 0).setAnycasting(true));

                    Boolean fetched = p.getResult(timeout);

                    if (fetched != null) {
                        if (fetched) {
                            LogManager.logDetail(LogConstants.CTX_RUNTIME, object, "pulled state", stateId); //$NON-NLS-1$
                            if (method !=null) {
                                try {
                                    result = method.invoke(object, args);
                                } catch (InvocationTargetException e) {
                                    throw e.getCause();
                                }
                                if (result != null) {
                                    return result;
                                }
                            }
                            break;
                        }
                        LogManager.logWarning(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40102, object, stateId));
                    } else {
                        LogManager.logWarning(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40103, object, stateId));
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
                    if (object instanceof ReplicatedObject<?> && !remoteMembers.isEmpty()) {
                        HashSet<Serializable> dropped = new HashSet<Serializable>();
                        for (Address address : remoteMembers) {
                            dropped.add(address.toString());
                        }
                        ((ReplicatedObject<?>)object).droppedMembers(dropped);
                    }
                    remoteMembers.clear();
                    remoteMembers.addAll(newView.getMembers());
                    remoteMembers.remove(this.disp.getChannel().getAddress());
                }
            }
        }
    }

    private interface Streaming {
        void sendState(Serializable id, Address dest);
        void createState(Serializable id);
        void buildState(Serializable id, byte[] bytes);
        void finishState(Serializable id);
    }

    //TODO: this should be configurable, or use a common executor
    private transient Executor executor;
    private transient ChannelFactory channelFactory;

    public JGroupsObjectReplicator(ChannelFactory channelFactory, Executor executor) {
        this.channelFactory = channelFactory;
        this.executor = executor;
    }

    public void stop(Object object) {
        if (object == null || !Proxy.isProxyClass(object.getClass())) {
            return;
        }
        ReplicatedInvocationHandler<?> handler = (ReplicatedInvocationHandler<?>) Proxy.getInvocationHandler(object);
        JChannel c = handler.disp.getChannel();
        handler.disp.stop();
        c.disconnect();
        c.close();
    }

    @Override
    public <T, S> T replicate(String mux_id,
            Class<T> iface, final S object, long startTimeout) throws Exception {
        JChannel channel = channelFactory.createChannel(mux_id);

        // To keep the order of methods same at all the nodes.
        TreeMap<String, Method> methods = new TreeMap<String, Method>();
        for (Method method : iface.getMethods()) {
            if (method.getAnnotation(Replicated.class) == null) {
                continue;
            }
            methods.put(method.toGenericString(), method);
        }

        final HashMap<Method, Short> methodMap = new HashMap<Method, Short>();
        final ArrayList<Method> methodList = new ArrayList<Method>();

        for (String method : methods.keySet()) {
            methodList.add(methods.get(method));
            methodMap.put(methods.get(method), (short)(methodList.size() - 1));
        }

        Method hasState = ReplicatedObject.class.getMethod(HAS_STATE, new Class<?>[] {Serializable.class});
        methodList.add(hasState);
        methodMap.put(hasState, (short)(methodList.size() - 1));

        Method sendState = JGroupsObjectReplicator.Streaming.class.getMethod(SEND_STATE, new Class<?>[] {Serializable.class, Address.class});
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
        channel.setReceiver(proxy);
        /*
         * TODO: could have an object implement streaming
         * Override the normal handle method to support streaming
         */
        ReplicatorRpcDispatcher disp = new ReplicatorRpcDispatcher<S>(channel, proxy, proxy, object, object, methodMap, methodList);

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
                ((ReplicatedObject)object).setAddress(channel.getAddress().toString());
                proxy.pullState(null, null, null, startTimeout, startTimeout != 0?STATE_TIMEOUT:0);
            }
            success = true;
            return replicatedProxy;
        } catch (Throwable e) {
            if (e instanceof Exception) {
                throw (Exception)e;
            }
             throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40104, e);
        } finally {
            if (!success) {
                channel.close();
            } else {
                synchronized (disp) {
                    //mark as initialized so that state can be pulled if needed
                    disp.initialized = true;
                }
            }
        }
    }
}
