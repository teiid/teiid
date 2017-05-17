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

package org.teiid.jdbc;

import java.sql.SQLException;
import java.util.Properties;

import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.net.ServerConnection;


public class LocalProfile implements ConnectionProfile {
	
    public static final String USE_CALLING_THREAD = "useCallingThread"; //$NON-NLS-1$
	public static final String WAIT_FOR_LOAD = "waitForLoad"; //$NON-NLS-1$
	public static final String TRANSPORT_NAME = "transportName"; //$NON-NLS-1$
	public static final Object DQP_WORK_CONTEXT = "dqpWorkContext"; //$NON-NLS-1$

	/**
     * This method tries to make a connection to the given URL. This class
     * will return a null if this is not the right driver to connect to the given URL.
     * @param The URL used to establish a connection.
     * @return Connection object created
     * @throws SQLException if it is unable to establish a connection
     */
    public ConnectionImpl connect(String url, Properties info) 
        throws TeiidSQLException {
        try {
        	ServerConnection sc = createServerConnection(info);
			return new ConnectionImpl(sc, info, url);
		} catch (TeiidRuntimeException e) {
			throw TeiidSQLException.create(e);
		} catch (TeiidException e) {
			throw TeiidSQLException.create(e);
		}
    }

	public ServerConnection createServerConnection(Properties info) throws TeiidException {
		return ModuleHelper.createFromModule(info);
	}

}
