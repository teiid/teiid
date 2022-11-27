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

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Optional methods supported by Teiid Connections.
 */
public interface TeiidConnection extends Connection {

    /**
     * Re-authenticate with the given username and password.  If the re-authentication
     * fails the connection will remain under the current user.
     *
     * @param userName
     *            the username to authenticate with
        * @param newPassword
        *            the password to authenticate with
     */
    public void changeUser(String userName, String newPassword) throws SQLException;
}
