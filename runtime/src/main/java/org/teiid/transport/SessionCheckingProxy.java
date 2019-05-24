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

package org.teiid.transport;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.client.util.ExceptionUtil;
import org.teiid.client.util.ResultsFuture;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.logging.LogManager;
import org.teiid.runtime.RuntimePlugin;

/**
 * Common proxy for for checking the DQPWorkContext session - nominally for use with DQPCore
 */
public final class SessionCheckingProxy
        extends LogManager.LoggingProxy {
    public SessionCheckingProxy(Object instance, String loggingContext,
            int level) {
        super(instance, loggingContext, level);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Throwable exception = null;
        try {
            DQPWorkContext workContext = DQPWorkContext.getWorkContext();
            if (workContext.getSession().isClosed() || workContext.getSessionId() == null) {
                if (method.getName().equals("closeRequest")) { //$NON-NLS-1$
                    //the client can issue close request effectively concurrently with close session
                    //there's no need for this to raise an exception
                    return ResultsFuture.NULL_FUTURE;
                }
                String sessionID = workContext.getSession().getSessionId();
                if (sessionID == null) {
                     throw new InvalidSessionException(RuntimePlugin.Event.TEIID40041, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40041));
                }
                workContext.setSession(new SessionMetadata());
                throw new InvalidSessionException(RuntimePlugin.Event.TEIID40042, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40042, sessionID));
            }
            return super.invoke(proxy, method, args);
        } catch (InvocationTargetException e) {
            exception = e.getTargetException();
        } catch(Throwable t){
            exception = t;
        }
        throw ExceptionUtil.convertException(method, exception);
    }
}