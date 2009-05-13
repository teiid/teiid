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

package org.teiid.connector.jdbc.mysql;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.SourceSystemFunctions;
import org.teiid.connector.jdbc.translator.Translator;



/** 
 * @since 4.3
 */
public class MySQLTranslator extends Translator {

	@Override
    public void initialize(ConnectorEnvironment env) throws ConnectorException {
        super.initialize(env);
        registerFunctionModifier(SourceSystemFunctions.CONVERT, new MySQLConvertModifier(getLanguageFactory()));
        registerFunctionModifier(SourceSystemFunctions.BITAND, new BitFunctionModifier("&", getLanguageFactory())); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITNOT, new BitFunctionModifier("~", getLanguageFactory())); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITOR, new BitFunctionModifier("|", getLanguageFactory())); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITXOR, new BitFunctionModifier("^", getLanguageFactory())); //$NON-NLS-1$
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
	public int getTimestampNanoPrecision() {
		return 6;
	}
	
	@Override
	public void afterConnectionCreation(Connection connection) {
		super.afterConnectionCreation(connection);
		
		Statement stmt = null;
		try {
			stmt = connection.createStatement();
			stmt.execute("set SESSION sql_mode = 'ANSI'"); //$NON-NLS-1$
		} catch (SQLException e) {
			getEnvironment().getLogger().logError("Error setting ANSI mode", e); //$NON-NLS-1$
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					getEnvironment().getLogger().logDetail("Error closing statement", e); //$NON-NLS-1$
				}
			}
		}
	}
	
	@Override
	public Class<? extends ConnectorCapabilities> getDefaultCapabilities() {
		return MySQLCapabilities.class;
	}
	
    public boolean useParensForJoins() {
    	return true;
    }
}
