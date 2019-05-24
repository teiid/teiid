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

package org.teiid.runtime;

import java.sql.CallableStatement;
import java.sql.SQLException;

import org.teiid.jdbc.TeiidConnection;
import org.teiid.jdbc.TeiidPreparedStatement;
import org.teiid.query.sql.lang.Command;

/**
 * An extension to {@link TeiidConnection} that allows for by-passing the parser.
 * <br>
 * Note this is a non-public API that is subject to change.  And that the parser will still be consulted
 * if prepared statement metadata is asked for prior to execution or if named parameters are used for
 * {@link CallableStatement} parameter assignments.
 */
public interface EmbeddedConnection extends TeiidConnection {

    CallableStatement prepareCall(Command command, EmbeddedRequestOptions options) throws SQLException;

    TeiidPreparedStatement prepareStatement(Command command, EmbeddedRequestOptions options) throws SQLException;

}
