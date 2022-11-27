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

package org.teiid.dqp.internal.datamgr;

import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.translator.ExecutionContext;

/**
 */
public class FakeExecutionContextImpl extends ExecutionContextImpl {

    private final static AtomicInteger COUNT = new AtomicInteger(0);

    public FakeExecutionContextImpl() {
        this(COUNT.getAndIncrement());
    }

    public FakeExecutionContextImpl(int unique) {
        super("VDB" + unique, //$NON-NLS-1$
                unique,
                "ExecutionPayload" + unique, //$NON-NLS-1$
                "ConnectionID" + unique, //$NON-NLS-1$
                "ConnectorID" + unique, //$NON-NLS-1$
                unique,
                "PartID" + unique, //$NON-NLS-1$
                "ExecCount" + unique); //$NON-NLS-1$
    }

    public FakeExecutionContextImpl(ExecutionContext c) {
        super(c.getVdbName(), c.getVdbVersion(), c.getCommandPayload(), c
                .getConnectionId(), c.getConnectorIdentifier(), Long.valueOf(c
                .getRequestId()), c.getPartIdentifier(), c
                .getExecutionCountIdentifier());
    }

}
