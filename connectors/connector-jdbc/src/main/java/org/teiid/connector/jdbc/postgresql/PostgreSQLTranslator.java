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

package org.teiid.connector.jdbc.postgresql;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.SourceSystemFunctions;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.jdbc.oracle.LeftOrRightFunctionModifier;
import org.teiid.connector.jdbc.oracle.MonthOrDayNameFunctionModifier;
import org.teiid.connector.jdbc.translator.AliasModifier;
import org.teiid.connector.jdbc.translator.EscapeSyntaxModifier;
import org.teiid.connector.jdbc.translator.ExtractFunctionModifier;
import org.teiid.connector.jdbc.translator.Translator;
import org.teiid.connector.language.IAggregate;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.ILimit;
import org.teiid.connector.visitor.framework.HierarchyVisitor;
import org.teiid.connector.visitor.util.SQLReservedWords;



/** 
 * Translator class for PostgreSQL.  Updated to expect a 8.0+ jdbc client
 * @since 4.3
 */
public class PostgreSQLTranslator extends Translator {

    public void initialize(ConnectorEnvironment env) throws ConnectorException {
        //TODO: all of the functions (except for convert) can be handled through just the escape syntax
        super.initialize(env);
        registerFunctionModifier(SourceSystemFunctions.LOG, new AliasModifier("ln")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.LOG10, new AliasModifier("log")); //$NON-NLS-1$ 
        
        registerFunctionModifier(SourceSystemFunctions.BITAND, new AliasModifier("&")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.BITNOT, new AliasModifier("~")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.BITOR, new AliasModifier("|")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.BITXOR, new AliasModifier("#")); //$NON-NLS-1$ 
        
        registerFunctionModifier(SourceSystemFunctions.CHAR, new AliasModifier("chr")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.CONCAT, new AliasModifier("||")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("lower")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.LEFT, new LeftOrRightFunctionModifier(getLanguageFactory()));
        registerFunctionModifier(SourceSystemFunctions.RIGHT, new LeftOrRightFunctionModifier(getLanguageFactory()));
        registerFunctionModifier(SourceSystemFunctions.SUBSTRING, new AliasModifier("substr")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("upper")); //$NON-NLS-1$ 
        
        registerFunctionModifier(SourceSystemFunctions.DAYNAME, new MonthOrDayNameFunctionModifier(getLanguageFactory(), "Day"));//$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.DAYOFWEEK, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.DAYOFYEAR, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.HOUR, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.MINUTE, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.MONTH, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.MONTHNAME, new MonthOrDayNameFunctionModifier(getLanguageFactory(), "Month"));//$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.QUARTER, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.SECOND, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.WEEK, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.YEAR, new ExtractFunctionModifier()); 
        
        //specific to 8.2 client or later
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPADD, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPDIFF, new EscapeSyntaxModifier());
        
        registerFunctionModifier(SourceSystemFunctions.IFNULL, new AliasModifier("coalesce")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.CONVERT, new PostgreSQLConvertModifier(getLanguageFactory()));        
    }    
    
    @Override
    public String translateLiteralBoolean(Boolean booleanValue) {
        if(booleanValue.booleanValue()) {
            return "TRUE"; //$NON-NLS-1$
        }
        return "FALSE"; //$NON-NLS-1$
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
        return "to_timestamp('" + formatDateValue(timestampValue) + "', 'YYYY-MM-DD HH24:MI:SS.US')"; //$NON-NLS-1$//$NON-NLS-2$ 
    }
    
    @Override
    public int getTimestampNanoPrecision() {
    	return 6;
    }
    
    @SuppressWarnings("unchecked")
	@Override
    public List<?> translateLimit(ILimit limit, ExecutionContext context) {
    	if (limit.getRowOffset() > 0) {
    		return Arrays.asList("LIMIT ", limit.getRowLimit(), " OFFSET ", limit.getRowOffset()); //$NON-NLS-1$ //$NON-NLS-2$ 
    	}
        return null;
    }

    /**
     * Postgres doesn't provide min/max(boolean), so this conversion writes a min(BooleanValue) as 
     * bool_and(BooleanValue)
     * @see org.teiid.connector.visitor.framework.LanguageObjectVisitor#visit(org.teiid.connector.language.IAggregate)
     * @since 4.3
     */
    @Override
    public ICommand modifyCommand(ICommand command, ExecutionContext context)
    		throws ConnectorException {
    	HierarchyVisitor visitor = new HierarchyVisitor() {
    		@Override
    		public void visit(IAggregate obj) {
                if (TypeFacility.RUNTIME_TYPES.BOOLEAN.equals(obj.getExpression().getType())) {
                	if (obj.getName().equalsIgnoreCase(SQLReservedWords.MIN)) {
                		obj.setName("bool_and"); //$NON-NLS-1$
                	} else if (obj.getName().equalsIgnoreCase(SQLReservedWords.MAX)) {
                		obj.setName("bool_or"); //$NON-NLS-1$
                	}
                }
    		}
   		};
    	
    	command.acceptVisitor(visitor);
    	return command;
    }
    
    @Override
    public Class<? extends ConnectorCapabilities> getDefaultCapabilities() {
    	return PostgreSQLCapabilities.class;
    }

}
