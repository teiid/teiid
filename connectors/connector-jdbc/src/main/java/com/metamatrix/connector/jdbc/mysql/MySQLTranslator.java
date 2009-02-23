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

package com.metamatrix.connector.jdbc.mysql;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.SourceSystemFunctions;
import com.metamatrix.connector.jdbc.translator.Translator;


/** 
 * @since 4.3
 */
public class MySQLTranslator extends Translator {

	@Override
    public void initialize(ConnectorEnvironment env) throws ConnectorException {
        super.initialize(env);
        registerFunctionModifier(SourceSystemFunctions.CONVERT, new MySQLConvertModifier(getLanguageFactory())); //$NON-NLS-1$
    }  
	
	@Override
    public String translateLiteralDate(Date dateValue) {
        return "DATE('" + formatDateValue(dateValue) + "')";  //$NON-NLS-1$//$NON-NLS-2$
    }

	@Override
    public String translateLiteralTime(Time timeValue) {
        return "TIME('" + formatDateValue(timeValue) + "')";  //$NON-NLS-1$//$NON-NLS-2$
    }

	@Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        return "TIMESTAMP('" + formatDateValue(timestampValue) + "')";  //$NON-NLS-1$//$NON-NLS-2$
    }
	
	@Override
	public boolean useParensForSetQueries() {
		return true;
	}
	
	@Override
	public int getTimestampNanoSecondPrecision() {
		return 6;
	}
	
}
