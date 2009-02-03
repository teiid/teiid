/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package com.metamatrix.data.basic;

import com.metamatrix.data.DataPlugin;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.api.TypeFacility;
import com.metamatrix.data.api.ValueTranslator;
import com.metamatrix.data.exception.ConnectorException;

/**
 * BasicValueTranslator can translate between types using the standard {@link TypeFacility}
 * transformations.
 */
public class BasicValueTranslator implements ValueTranslator {
    private Class sourceType;
    private Class targetType;
    private TypeFacility typeFacility;
    
	public BasicValueTranslator(Class sourceType, Class targetType, TypeFacility typeFacility) {
		this.sourceType = sourceType;
		this.targetType = targetType;
		this.typeFacility = typeFacility;
	}
	
    public Class getSourceType() {
        return this.sourceType;
    }

    public Class getTargetType() {
        return this.targetType;
    }

    public Object translate(Object value, ExecutionContext context) throws ConnectorException {
    	if (typeFacility.hasTransformation(sourceType, targetType)) {
    		return typeFacility.transformValue(value, sourceType, targetType);
    	}
    	throw new ConnectorException(DataPlugin.Util.getString("ValueTranslator.no_tranfrom_found", new Object[] {this.sourceType, this.targetType}));
    }
}
