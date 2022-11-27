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

import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterAssociation;
import javax.resource.spi.ValidatingManagedConnectionFactory;
import javax.security.auth.Subject;

import org.teiid.core.TeiidException;
import org.teiid.core.util.Assertion;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.ReflectionHelper;


public abstract class BasicManagedConnectionFactory implements ManagedConnectionFactory, ResourceAdapterAssociation, ValidatingManagedConnectionFactory {

    private static final long serialVersionUID = -7302713800883776790L;
    private PrintWriter log;
    private BasicResourceAdapter ra;
    private BasicConnectionFactory cf;

    @Override
    public abstract BasicConnectionFactory createConnectionFactory() throws ResourceException;

    @Override
    public Object createConnectionFactory(ConnectionManager cm) throws ResourceException {
        this.cf = createConnectionFactory();
        return new WrappedConnectionFactory(this.cf, cm, this);
    }

    @Override
    public ManagedConnection createManagedConnection(Subject arg0, ConnectionRequestInfo arg1) throws ResourceException {
        Assertion.isNotNull(this.cf);
        ConnectionContext.setSubject(arg0);

        ResourceConnection connection = null;
        if (arg1 instanceof ConnectionRequestInfoWrapper) {
            connection = this.cf.getConnection(((ConnectionRequestInfoWrapper)arg1).cs);
        }
        else {
            connection = this.cf.getConnection();
        }
        ConnectionContext.setSubject(null);
        return new BasicManagedConnection(connection);
    }

    @Override
    public PrintWriter getLogWriter() throws ResourceException {
        return this.log;
    }

    @Override
    public ManagedConnection matchManagedConnections(Set arg0, Subject arg1, ConnectionRequestInfo arg2) throws ResourceException {
        return (ManagedConnection)arg0.iterator().next();
    }

    @Override
    public void setLogWriter(PrintWriter arg0) throws ResourceException {
        this.log = arg0;
    }

    @Override
    public ResourceAdapter getResourceAdapter() {
        return this.ra;
    }

    @Override
    public void setResourceAdapter(ResourceAdapter arg0) throws ResourceException {
        this.ra = (BasicResourceAdapter)arg0;
    }

    public static <T> T getInstance(Class<T> expectedType, String className, Collection ctorObjs, Class defaultClass) throws ResourceException {
        try {
            if (className == null) {
                if (defaultClass == null) {
                    throw new ResourceException("Neither class name or default class specified to create an instance"); //$NON-NLS-1$
                }
                return expectedType.cast(defaultClass.newInstance());
            }
            return expectedType.cast(ReflectionHelper.create(className, ctorObjs, Thread.currentThread().getContextClassLoader()));
        } catch (TeiidException e) {
            throw new ResourceException(e);
        } catch (IllegalAccessException e) {
            throw new ResourceException(e);
        } catch(InstantiationException e) {
            throw new ResourceException(e);
        }
    }

    @Override
    public Set<BasicManagedConnection> getInvalidConnections(Set arg0) throws ResourceException {
        HashSet<BasicManagedConnection> result = new HashSet<BasicManagedConnection>();
        for (Object object : arg0) {
            if (object instanceof BasicManagedConnection) {
                BasicManagedConnection bmc = (BasicManagedConnection)object;
                if (!bmc.isValid()) {
                    result.add(bmc);
                }
            }
        }
        return result;
    }

    protected static boolean checkEquals(Object left, Object right) {
        return EquivalenceUtil.areEqual(left, right);
    }

}
