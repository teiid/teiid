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

package com.metamatrix.common.types.basic;

import java.io.IOException;
import java.sql.SQLException;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.InvalidReferenceException;
import com.metamatrix.common.types.TransformationException;
import com.metamatrix.common.types.XMLType;
import com.metamatrix.core.CorePlugin;

public class SQLXMLToStringTransform extends AnyToStringTransform {

    /**
     * This method transforms a value of the source type into a value
     * of the target type.
     * @param value Incoming value of source type
     * @return Outgoing value of target type
     * @throws TransformationException if value is an incorrect input type or
     * the transformation fails
     */
    public Object transform(Object value) throws TransformationException {
        if(value == null) {
            return value;
        }

        XMLType source = (XMLType)value;
        
        try {       
            char[] result = new char[DataTypeManager.MAX_STRING_LENGTH];
            int read = source.getCharacterStream().read(result);
            return new String(result, 0, read);
        } catch (SQLException e) {
            throw new TransformationException(e, CorePlugin.Util.getString("failed_convert", new Object[] {getSourceType().getName(), getTargetType().getName()})); //$NON-NLS-1$            
        } catch(InvalidReferenceException e) {
            throw new TransformationException(e, CorePlugin.Util.getString("remote_lob_access")); //$NON-NLS-1$
        } catch (IOException e) {
            throw new TransformationException(e, CorePlugin.Util.getString("failed_convert", new Object[] {getSourceType().getName(), getTargetType().getName()})); //$NON-NLS-1$
        }
    }

    /**
     * Type of the incoming value.
     * @return Source type
     */
    public Class getSourceType() {
        return XMLType.class;
    }
    
    /** 
     * @see com.metamatrix.common.types.AbstractTransform#isNarrowing()
     */
    public boolean isNarrowing() {
        return true;
    }
}
