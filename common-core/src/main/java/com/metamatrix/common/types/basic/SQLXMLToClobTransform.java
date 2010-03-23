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
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.SQLException;

import com.metamatrix.common.types.ClobImpl;
import com.metamatrix.common.types.ClobType;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.InputStreamFactory;
import com.metamatrix.common.types.SQLXMLImpl;
import com.metamatrix.common.types.Streamable;
import com.metamatrix.common.types.Transform;
import com.metamatrix.common.types.TransformationException;
import com.metamatrix.common.types.XMLType;
import com.metamatrix.common.util.ReaderInputStream;
import com.metamatrix.core.CorePlugin;

public class SQLXMLToClobTransform extends Transform {

	@Override
	public Class<?> getSourceType() {
		return DataTypeManager.DefaultDataClasses.XML;
	}
	
	@Override
	public Class<?> getTargetType() {
		return DataTypeManager.DefaultDataClasses.CLOB;
	}
	
    /**
     * This method transforms a value of the source type into a value
     * of the target type.
     * @param value Incoming value of source type
     * @return Outgoing value of target type
     * @throws TransformationException if value is an incorrect input type or
     * the transformation fails
     */
    public Object transformDirect(Object value) throws TransformationException {
        final XMLType source = (XMLType)value;
        
        try {
            if (source.getReference() instanceof SQLXMLImpl) {
            	SQLXMLImpl xml = (SQLXMLImpl)source.getReference();
            	return new ClobType(new ClobImpl(xml.getStreamFactory(), -1));
            } 
            return new ClobType(new ClobImpl(new InputStreamFactory(Streamable.ENCODING) {
            	@Override
            	public InputStream getInputStream() throws IOException {
            		try {
						return new ReaderInputStream(source.getCharacterStream(), Charset.forName(Streamable.ENCODING));
					} catch (SQLException e) {
						throw new IOException(e);
					}
            	}
            }, -1));
        } catch (SQLException e) {
            throw new TransformationException(e, CorePlugin.Util.getString("failed_convert", new Object[] {getSourceType().getName(), getTargetType().getName()})); //$NON-NLS-1$            
        } 
    }

    /** 
     * @see com.metamatrix.common.types.Transform#isExplicit()
     */
    public boolean isExplicit() {
        return true;
    }
}
