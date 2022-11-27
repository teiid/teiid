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

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.resource.ResourceException;
import javax.resource.cci.ConnectionFactory;
import javax.resource.cci.ConnectionSpec;
import javax.resource.cci.RecordFactory;
import javax.resource.cci.ResourceAdapterMetaData;

public abstract class BasicConnectionFactory<T extends ResourceConnection> implements ConnectionFactory, org.teiid.resource.api.ConnectionFactory<T> {
    private static final long serialVersionUID = 2900581028589520388L;
    private Reference reference;

    @Override
    public abstract T getConnection() throws ResourceException;

    @Override
    public ResourceConnection getConnection(ConnectionSpec arg0) throws ResourceException {
        throw new ResourceException("This operation not supported"); //$NON-NLS-1$;
    }

    @Override
    public ResourceAdapterMetaData getMetaData() throws ResourceException {
        throw new ResourceException("This operation not supported"); //$NON-NLS-1$;
    }

    @Override
    public RecordFactory getRecordFactory() throws ResourceException {
        throw new ResourceException("This operation not supported"); //$NON-NLS-1$
    }

    @Override
    public void setReference(Reference arg0) {
        this.reference = arg0;
    }

    @Override
    public Reference getReference() throws NamingException {
        return this.reference;
    }
}
