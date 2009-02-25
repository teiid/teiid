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

/*
 */
package com.metamatrix.connector.object.extension.value;

import java.sql.Timestamp;
import java.util.Date;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ValueTranslator;


/**
 */
public class JavaUtilDateValueTranslator implements ValueTranslator {
        
    public Class getSourceType() {
        return Date.class;
    }

    public Class getTargetType() {
        return Timestamp.class;
    }

    public Object translate(Object value, ExecutionContext context) throws ConnectorException {
        Date d = (Date) value;
        long dt = d.getTime();
        
        Timestamp ts = new Timestamp(dt);
        return ts;
    }    

}
