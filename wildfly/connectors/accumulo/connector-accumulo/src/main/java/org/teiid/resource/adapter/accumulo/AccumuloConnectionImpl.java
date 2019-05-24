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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.ConnectorImpl;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.ServerClient;
import org.apache.accumulo.core.client.impl.thrift.ClientService;
import org.apache.accumulo.core.client.impl.thrift.ClientService.Client;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.thrift.transport.TTransportException;
import org.teiid.core.util.StringUtil;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.resource.spi.ConnectionContext;
import org.teiid.translator.accumulo.AccumuloConnection;

public class AccumuloConnectionImpl extends BasicConnection implements AccumuloConnection {
    private ConnectorImpl conn;
    private String[] roles;

    public AccumuloConnectionImpl(AccumuloManagedConnectionFactory mcf, ZooKeeperInstance inst) throws ResourceException {
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
            checkTabletServerExists(inst, userName, password);
            this.conn = (ConnectorImpl) inst.getConnector(userName, new PasswordToken(password));
        } catch (AccumuloException e) {
            throw new ResourceException(e);
        } catch (AccumuloSecurityException e) {
            throw new ResourceException(e);
        }
    }

    private void checkTabletServerExists(ZooKeeperInstance inst, String userName, String password)
            throws ResourceException {
        ClientService.Client client = null;
        try {
            Pair<String,Client> pair = ServerClient.getConnection(new ClientContext(inst, new Credentials(userName, new PasswordToken(password)), inst.getConfiguration()), true, 10);
            client = pair.getSecond();
        } catch (TTransportException e) {
            throw new ResourceException(AccumuloManagedConnectionFactory.UTIL.getString("no_tserver"), e);
        } finally {
            if (client != null) {
                ServerClient.close(client);
            }
        }
    }

    @Override
    public Connector getInstance() {
        return conn;
    }

    @Override
    public void close() throws ResourceException {
        // Where is the close call on instance Accumulo? Am I supposed to
        // waste resources??
    }

    @Override
    public Authorizations getAuthorizations() {
        if (roles != null && roles.length > 0) {
            return new Authorizations(roles);
        }
        return new Authorizations();
    }
}
