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

import java.io.Serializable;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.resource.Referenceable;
import javax.resource.ResourceException;
import javax.resource.cci.Connection;
import javax.resource.cci.ConnectionFactory;
import javax.resource.cci.ConnectionSpec;
import javax.resource.cci.RecordFactory;
import javax.resource.cci.ResourceAdapterMetaData;
import javax.resource.spi.ConnectionManager;

public class WrappedConnectionFactory implements ConnectionFactory, Referenceable, Serializable, org.teiid.resource.api.ConnectionFactory  {

    private static final long serialVersionUID = 5499157394014613035L;
    private BasicConnectionFactory delegate;
    private ConnectionManager cm;
    private BasicManagedConnectionFactory mcf;
    private Reference reference;

    public WrappedConnectionFactory() {
        // need by spec 17.5.1.1, not sure how this will effect as the this
        // connection factory is always built by ManagedConnectionfactory
    }

    public WrappedConnectionFactory(BasicConnectionFactory delegate, ConnectionManager cm, BasicManagedConnectionFactory mcf) {
        this.delegate = delegate;
        this.cm = cm;
        this.mcf = mcf;
    }

    @Override
    public WrappedConnection getConnection() throws ResourceException {
        return (WrappedConnection)cm.allocateConnection(mcf, null);
    }


    @Override
    public void setReference(Reference arg0) {
        this.reference = arg0;
    }

    @Override
    public Reference getReference() throws NamingException {
        return this.reference;
    }

    @Override
    public Connection getConnection(ConnectionSpec arg0) throws ResourceException {
        return (Connection)cm.allocateConnection(mcf, new ConnectionRequestInfoWrapper(arg0));
    }

    @Override
    public ResourceAdapterMetaData getMetaData() throws ResourceException {
        return this.delegate.getMetaData();
    }

    @Override
    public RecordFactory getRecordFactory() throws ResourceException {
        return this.delegate.getRecordFactory();
    }
}
