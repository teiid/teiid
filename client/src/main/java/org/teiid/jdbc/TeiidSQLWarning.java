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

import java.sql.SQLWarning;


/**
 * Teiid specific SQLWarning<br>
 * If the cause was a source SQLWarning, then you may need to consult
 * the warning chain to get all warnings, see the example below.
 *
<pre>
//warning will be an instanceof TeiidSQLWarning to convey model/source information
SQLWarning warning = stmt.getWarnings();

while (warning != null) {
  Exception e = warning.getCause();
  if (cause instanceof SQLWarning) {
    //childWarning should now be the head of the source warning chain
    SQLWarning childWarning = (SQLWarning)cause;
    while (childWarning != null) {
      //do something with childWarning
      childWarning = childWarning.getNextWarning();
    }
  }
  warning = warning.getNextWarning();
}
</pre>
 *
 */
public class TeiidSQLWarning extends SQLWarning {

    private static final long serialVersionUID = -7080782561220818997L;

    private String modelName = "UNKNOWN"; // variable stores the name of the model for the atomic query //$NON-NLS-1$
    private String sourceName = "UNKNOWN"; // variable stores name of the connector binding //$NON-NLS-1$

    public TeiidSQLWarning() {
        super();
    }

    public TeiidSQLWarning(String reason) {
        super(reason);
    }

    public TeiidSQLWarning(String reason, String state) {
        super(reason, state);
    }

    public TeiidSQLWarning(String reason, String sqlState, Throwable ex, String sourceName, String modelName) {
        super(reason, sqlState, ex);
        this.sourceName = sourceName;
        this.modelName = modelName;
    }

    public TeiidSQLWarning(String reason, String sqlState, int errorCode, Throwable ex) {
        super(reason, sqlState, errorCode, ex);
    }

    /**
     *
     * @return the source name or null if the warning is not associated with a source
     */
    public String getSourceName() {
        return sourceName;
    }

    /**
     *
     * @return the model name or null if the warning is not associated with a model
     */
    public String getModelName() {
        return modelName;
    }

}