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

package org.teiid.translator.jdbc.redshift;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.postgresql.PostgreSQLExecutionFactory;



/** 
 * Translator class for Red Shift. 
 */
@Translator(name="redshift", description="A translator for Redshift")
public class RedshiftExecutionFactory extends PostgreSQLExecutionFactory {

	@Override
	public void start() throws TranslatorException {
		super.start();
		getFunctionModifiers().remove(SourceSystemFunctions.SUBSTRING); //redshift doesn't use substr
		//to_timestamp is not supported
		this.parseModifier.setPrefix("TO_DATE("); //$NON-NLS-1$
		//redshift needs explicit precision/scale
		this.convertModifier.addTypeMapping("decimal(38, 19)", FunctionModifier.BIGDECIMAL); //$NON-NLS-1$
	}
	
	@Override
	public void intializeConnectionAfterCancel(Connection c)
			throws SQLException {
		//cancel can leave the connection in an invalid state, issue another query to clear any flags
		Statement s = c.createStatement();
		try {
			s.execute("select 1"); //$NON-NLS-1$
		} finally {
			s.close();
		}
	}
	
	@Override
	public boolean hasTimeType() {
		return false;
	}
	
	@Override
	public boolean supportsConvert(int fromType, int toType) {
		if (toType == TypeFacility.RUNTIME_CODES.TIME) {
			return false;
		}
		return super.supportsConvert(fromType, toType);
	}
	
	@Override
	public List<String> getSupportedFunctions() {
		List<String> functions = super.getSupportedFunctions();
		functions.remove(SourceSystemFunctions.ASCII);
		return functions;
	}
	
	@Override
	public boolean supportsQuantifiedCompareCriteriaAll() {
		return false;
	}
	
	@Override
	public boolean supportsQuantifiedCompareCriteriaSome() {
		return false;
	}
	
	@Override
	public Object convertToken(String group) {
		//timezone not supported
		if (group.charAt(0) == 'Z') {
			throw new IllegalArgumentException();
		}
		//TODO: time fields are probably not supported for parsing
		return super.convertToken(group);
	}
	
    @Override
    public String getCreateTemporaryTablePostfix(boolean inTransaction) {
        return ""; //$NON-NLS-1$ //redshift does not support the ON COMMIT clause
    }
	
}
