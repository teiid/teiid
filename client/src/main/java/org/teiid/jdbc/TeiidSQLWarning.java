/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.jdbc;

import java.sql.SQLWarning;


/**
 * Teiid specific SQLWarning<br>
 * If the cause was a source SQLWarning, then you may need to consult
 * the warning chain to get all warnings, see the example below.
 * 
<code><pre>
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
</pre></code>
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