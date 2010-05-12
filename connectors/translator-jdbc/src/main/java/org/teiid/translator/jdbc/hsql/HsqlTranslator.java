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

package org.teiid.translator.jdbc.hsql;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.teiid.resource.adapter.jdbc.JDBCExecutionFactory;
import org.teiid.translator.ConnectorCapabilities;
import org.teiid.translator.ConnectorException;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.Translator;

public class HsqlTranslator extends Translator {
	
	@Override
	public void initialize(JDBCExecutionFactory env) throws ConnectorException {
		super.initialize(env);
		registerFunctionModifier(SourceSystemFunctions.TIMESTAMPADD, new AddDiffModifier(true, getLanguageFactory()));
		registerFunctionModifier(SourceSystemFunctions.TIMESTAMPDIFF, new AddDiffModifier(false, getLanguageFactory())); 
	
		ConvertModifier convert = new ConvertModifier();
		convert.addTypeMapping("boolean", FunctionModifier.BOOLEAN); //$NON-NLS-1$
		convert.addTypeMapping("tinyint", FunctionModifier.BYTE); //$NON-NLS-1$
		convert.addTypeMapping("smallint", FunctionModifier.SHORT); //$NON-NLS-1$
		convert.addTypeMapping("int", FunctionModifier.INTEGER); //$NON-NLS-1$
		convert.addTypeMapping("bigint", FunctionModifier.LONG); //$NON-NLS-1$
		convert.addTypeMapping("real", FunctionModifier.FLOAT); //$NON-NLS-1$
		convert.addTypeMapping("double", FunctionModifier.DOUBLE); //$NON-NLS-1$
		convert.addTypeMapping("decimal", FunctionModifier.BIGDECIMAL); //$NON-NLS-1$
		convert.addTypeMapping("decimal(38,0)", FunctionModifier.BIGINTEGER); //$NON-NLS-1$
		convert.addTypeMapping("date", FunctionModifier.DATE); //$NON-NLS-1$
		convert.addTypeMapping("time", FunctionModifier.TIME); //$NON-NLS-1$
		convert.addTypeMapping("timestamp", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
		convert.addTypeMapping("char(1)", FunctionModifier.CHAR); //$NON-NLS-1$
		convert.addTypeMapping("varchar", FunctionModifier.STRING); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.CONVERT, convert);		
	}
	
    @Override
    public String translateLiteralDate(Date dateValue) {
        return "DATE '" + formatDateValue(dateValue) + "'"; //$NON-NLS-1$//$NON-NLS-2$
    }

    @Override
    public String translateLiteralTime(Time timeValue) {
        return "TIME '" + formatDateValue(timeValue) + "'"; //$NON-NLS-1$//$NON-NLS-2$
    }
    
    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        return "TIMESTAMP '" + formatDateValue(timestampValue) + "'"; //$NON-NLS-1$//$NON-NLS-2$ 
    }
	
	@Override
	public Class<? extends ConnectorCapabilities> getDefaultCapabilities() {
		return HsqlCapabilities.class;
	}

}
