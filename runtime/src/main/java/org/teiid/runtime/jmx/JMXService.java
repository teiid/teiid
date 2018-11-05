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

package org.teiid.runtime.jmx;

import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.OperationsException;

import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.service.SessionService;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.services.BufferServiceImpl;

/**
 * Registers Teiid beans with JMX. Assumes only 1 Teiid will be registered
 */
public class JMXService {

    public static String TEIID = "org.teiid:type=Runtime"; //$NON-NLS-1$
    public static String CACHE_PREFIX = "org.teiid:type=Cache,name="; //$NON-NLS-1$

    private DQPCore dqp;
    private BufferServiceImpl bufferService;
    private SessionService sessionService;
    private MBeanServer server;
    private Set<String> registerdBeans = new HashSet<>();

    public JMXService(DQPCore dqpCore, BufferServiceImpl bufferServiceImpl,
            SessionService sessionService) {
        this.dqp = dqpCore;
        this.bufferService = bufferServiceImpl;
        this.sessionService = sessionService;
        server = ManagementFactory.getPlatformMBeanServer();
    }

    void setServer(MBeanServer server) {
        this.server = server;
    }

    public synchronized void registerBeans() {
        try {
            register(new Teiid(dqp, sessionService, bufferService), TEIID);
            register(new Cache(dqp.getPrepPlanCache()),
                    CACHE_PREFIX + "PreparedPlan"); //$NON-NLS-1$
            register(new Cache(dqp.getResltSetCache()),
                    CACHE_PREFIX + "ResultSet"); //$NON-NLS-1$
        } catch (MBeanRegistrationException | OperationsException e) {
            LogManager.logWarning(LogConstants.CTX_RUNTIME, e,
                    "JMX Registration Exception - it's likely another Teiid instance is running in this VM"); //$NON-NLS-1$
        }
    }

    public synchronized void unregisterBeans() {
        registerdBeans.forEach(name -> {
            try {
                this.server.unregisterMBean(new ObjectName(name));
            } catch (MBeanRegistrationException | InstanceNotFoundException
                    | MalformedObjectNameException e) {
                //ignore
            }
        });
        registerdBeans.clear();
    }

    void register(Object mbean, String name)
            throws OperationsException, MBeanRegistrationException {
        this.server.registerMBean(mbean, new ObjectName(name));
        this.registerdBeans.add(name);
    }

    public static void main(String[] args) throws Exception {
        EmbeddedServer es = new EmbeddedServer();
        es.start(new EmbeddedConfiguration());
        Thread.sleep(1000000);
    }
}
