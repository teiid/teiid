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

package org.teiid.resource.adapter.accumulo;

import java.util.List;

import javax.resource.ResourceException;
import javax.security.auth.Subject;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.teiid.core.util.StringUtil;
import org.teiid.resource.spi.ConnectionContext;
import org.teiid.resource.spi.ResourceConnection;
import org.teiid.translator.accumulo.AccumuloConnection;

public class AccumuloConnectionImpl implements AccumuloConnection, ResourceConnection {
    private Connector conn;
    private AccumuloClient client;
    private String[] roles;

    public AccumuloConnectionImpl(AccumuloManagedConnectionFactory mcf) throws ResourceException {
        try {
            if (mcf.getRoles() != null) {
                List<String> auths = StringUtil.getTokens(mcf.getRoles(), ",");  //$NON-NLS-1$
                this.roles = auths.toArray(new String[auths.size()]);
            }

            String userName = mcf.getUsername();
            String password = mcf.getPassword();

            // if security-domain is specified and caller identity is used; then use
            // credentials from subject
            Subject subject = ConnectionContext.getSubject();
            if (subject != null) {
                userName = ConnectionContext.getUserName(subject, mcf, userName);
                password = ConnectionContext.getPassword(subject, mcf, userName, password);
                this.roles = ConnectionContext.getRoles(subject, this.roles);
            }
            this.client = Accumulo.newClient().to(mcf.getInstanceName(), mcf.getZooKeeperServerList()).as(userName, password).build();
            this.conn = Connector.from(client);
        } catch (AccumuloException e) {
            throw new ResourceException(e);
        } catch (AccumuloSecurityException e) {
            throw new ResourceException(e);
        }
    }


    @Override
    public Connector getInstance() {
        return conn;
    }

    @Override
    public void close() throws ResourceException {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public Authorizations getAuthorizations() {
        if (roles != null && roles.length > 0) {
            return new Authorizations(roles);
        }
        return new Authorizations();
    }
}
