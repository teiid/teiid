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
 * Teiid specific SQLWarning
 */

public class TeiidSQLWarning extends SQLWarning {
	
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