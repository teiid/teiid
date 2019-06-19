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
import java.sql.SQLWarning;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * This warning class is sent when using partial results mode if one or more
 * sources fails.  In this case, results will be returned (with 0 rows sent from
 * failing source).  This warning can be obtained from the ResultSet to determine
 * which sources failed and provided 0 rows and why they failed.
 */
public class PartialResultsWarning extends SQLWarning {

    private static final long serialVersionUID = 5301215068719177369L;
    private Map failures;

    /**
     * Construct partial results warning.
     * @param reason Reason for the exception
     * @param SQLstate SQL state code
     * @param vendorCode Vendor code
     */
    public PartialResultsWarning(String reason, String SQLstate, int vendorCode) {
        super(reason, SQLstate, vendorCode);
    }

    /**
     * Construct partial results warning.
     * @param reason Reason for the exception
     * @param SQLstate SQL state code
     */
    public PartialResultsWarning(String reason, String SQLstate) {
        super(reason, SQLstate);
    }

    /**
     * Construct partial results warning.
     * @param reason Reason for the exception
     */
    public PartialResultsWarning(String reason) {
        super(reason);
    }

    /**
     * Construct partial results warning.
     */
    public PartialResultsWarning() {
        super();
    }

    /**
     * Add a connector failure to the warning
     * @param name Connector name
     * @param exception Connector exception
     */
    public void addConnectorFailure(String name, SQLException exception) {
        if(this.failures == null) {
            this.failures = new HashMap();
        }
        this.failures.put(name, exception);
    }

    /**
     * Obtain list of connectors that failed.
     * @return List of connectors that failed - List contains String names
     */
    public Collection getFailedConnectors() {
        if(this.failures != null) {
            return new HashSet(this.failures.keySet());
        }
        return Collections.EMPTY_SET;
    }

    /**
     * Obtain failure for a particular connector.
     * @param connectorName Connector name
     * @return Exception that occurred for this connector or null if
     * the exception was unknown
     */
    public SQLException getConnectorException(String connectorName) {
        if(this.failures != null) {
            return (SQLException) this.failures.get(connectorName);
        }
        return null;
    }

}
