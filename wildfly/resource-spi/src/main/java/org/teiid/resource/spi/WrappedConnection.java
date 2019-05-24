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
package org.teiid.resource.spi;

import javax.resource.ResourceException;
import javax.resource.cci.Connection;
import javax.resource.cci.ConnectionMetaData;
import javax.resource.cci.Interaction;
import javax.resource.cci.ResultSetInfo;

public class WrappedConnection implements Connection, org.teiid.resource.api.WrappedConnection {

    private BasicManagedConnection mc;
    boolean closed = false;

    public WrappedConnection(BasicManagedConnection mc) {
        this.mc = mc;
    }

    @Override
    public void close() throws ResourceException {
        if (!this.closed && this.mc != null) {
            this.closed = true;
            this.mc.connectionClosed(this);
            this.mc = null;
        }
    }

    // Called by managed connection for the connection management
    void setManagedConnection(BasicManagedConnection mc) {
        this.mc = mc;
    }

    @Override
    public Interaction createInteraction() throws ResourceException {
        return this.mc.getConnection().createInteraction();
    }

    @Override
    public javax.resource.cci.LocalTransaction getLocalTransaction() throws ResourceException {
        return this.mc.getConnection().getLocalTransaction();
    }

    @Override
    public ConnectionMetaData getMetaData() throws ResourceException {
        return this.mc.getConnection().getMetaData();
    }

    @Override
    public ResultSetInfo getResultSetInfo() throws ResourceException {
        return this.mc.getConnection().getResultSetInfo();
    }

    public Connection unwrap() throws ResourceException {
        return this.mc.getConnection();
    }

}
