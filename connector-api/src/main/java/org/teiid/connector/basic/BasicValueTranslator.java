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
package org.teiid.connector.basic;

import org.teiid.connector.DataPlugin;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.api.ValueTranslator;

/**
 * BasicValueTranslator can translate between types using the standard {@link TypeFacility}
 * transformations.
 */
public class BasicValueTranslator<S, T> implements ValueTranslator<S, T> {
    private Class<S> sourceType;
    private Class<T> targetType;
    private TypeFacility typeFacility;
    
    public static <S, T> BasicValueTranslator<S, T> createTranslator(Class<S> sourceType, Class<T> targetType, TypeFacility typeFacility) {
    	return new BasicValueTranslator<S, T>(sourceType, targetType, typeFacility);
    }
    
	public BasicValueTranslator(Class<S> sourceType, Class<T> targetType, TypeFacility typeFacility) {
		this.sourceType = sourceType;
		this.targetType = targetType;
		this.typeFacility = typeFacility;
	}
	
    public Class<S> getSourceType() {
        return this.sourceType;
    }

    public Class<T> getTargetType() {
        return this.targetType;
    }

    public T translate(S value, ExecutionContext context) throws ConnectorException {
    	if (typeFacility.hasTransformation(sourceType, targetType)) {
    		return (T)typeFacility.transformValue(value, sourceType, targetType);
    	}
    	throw new ConnectorException(DataPlugin.Util.getString("ValueTranslator.no_tranfrom_found", new Object[] {this.sourceType, this.targetType}));
    }
}
