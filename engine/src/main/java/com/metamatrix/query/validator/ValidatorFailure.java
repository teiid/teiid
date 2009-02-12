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

package com.metamatrix.query.validator;

import java.util.*;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.report.ReportItem;

public class ValidatorFailure extends ReportItem {

	public static final String VALIDATOR_FAILURE = "ValidatorFailure"; //$NON-NLS-1$

    // Don't want to pass this around, so make it transient
    private transient Collection invalidObjects;  
        
    public ValidatorFailure(String description) { 
        super(VALIDATOR_FAILURE);
        setMessage(description);
    }
    
    public ValidatorFailure(String description, LanguageObject object) {
        super(VALIDATOR_FAILURE);
        setMessage(description);
        this.invalidObjects = new ArrayList(1);
        this.invalidObjects.add(object);
    }

    public ValidatorFailure(String description, Collection objects) { 
        super(VALIDATOR_FAILURE);
        setMessage(description);
        this.invalidObjects = new ArrayList(objects);
    }
    
    /** 
     * Get count of invalid objects.
     * @return Count of invalid objects
     */
    public int getInvalidObjectCount() { 
        if(this.invalidObjects == null) { 
            return 0;
        }
        return this.invalidObjects.size();
    }   
    
    /**
     * Get the objects that failed validation.  The collection may be null.
     * @return Invalid objects, may be null
     */
    public Collection getInvalidObjects() { 
        return this.invalidObjects;
    } 
    
    /**
     * Return description
     * @return Description of failure
     */    
    public String toString() { 
        return getMessage();
    }

}
