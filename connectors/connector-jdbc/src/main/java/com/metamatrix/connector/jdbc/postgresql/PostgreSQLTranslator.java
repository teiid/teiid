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

package com.metamatrix.connector.jdbc.postgresql;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.SourceSystemFunctions;
import com.metamatrix.connector.api.TypeFacility;
import com.metamatrix.connector.jdbc.extension.SQLTranslator;
import com.metamatrix.connector.jdbc.extension.impl.AliasModifier;
import com.metamatrix.connector.jdbc.oracle.LeftOrRightFunctionModifier;
import com.metamatrix.connector.jdbc.oracle.MonthOrDayNameFunctionModifier;
import com.metamatrix.connector.language.IAggregate;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.language.ILimit;
import com.metamatrix.connector.visitor.framework.HierarchyVisitor;
import com.metamatrix.connector.visitor.util.SQLReservedWords;


/** 
 * @since 4.3
 */
public class PostgreSQLTranslator extends SQLTranslator {

    public void initialize(ConnectorEnvironment env) throws ConnectorException {
        
        super.initialize(env);
        registerFunctionModifier(SourceSystemFunctions.LOG, new AliasModifier("ln")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.LOG10, new AliasModifier("log")); //$NON-NLS-1$ //$NON-NLS-2$
        
        registerFunctionModifier(SourceSystemFunctions.CHAR, new AliasModifier("chr")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.CONCAT, new AliasModifier("||")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("lower")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.LEFT, new LeftOrRightFunctionModifier(getLanguageFactory()));//$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.RIGHT, new LeftOrRightFunctionModifier(getLanguageFactory()));//$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.SUBSTRING, new AliasModifier("substr")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("upper")); //$NON-NLS-1$ //$NON-NLS-2$
        
        registerFunctionModifier(SourceSystemFunctions.DAYNAME, new MonthOrDayNameFunctionModifier(getLanguageFactory(), "Day"));//$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.DAYOFWEEK, new ModifiedDatePartFunctionModifier(getLanguageFactory(), "dow", "+", new Integer(1)));//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new DatePartFunctionModifier(getLanguageFactory(), "day"));//$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.DAYOFYEAR, new DatePartFunctionModifier(getLanguageFactory(), "doy"));//$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.HOUR, new DatePartFunctionModifier(getLanguageFactory(), "hour"));//$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.MINUTE, new DatePartFunctionModifier(getLanguageFactory(), "minute"));//$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.MONTH, new DatePartFunctionModifier(getLanguageFactory(), "month"));//$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.MONTHNAME, new MonthOrDayNameFunctionModifier(getLanguageFactory(), "Month"));//$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.QUARTER, new DatePartFunctionModifier(getLanguageFactory(), "quarter"));//$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.SECOND, new DatePartFunctionModifier(getLanguageFactory(), "second"));//$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.WEEK, new DatePartFunctionModifier(getLanguageFactory(), "week"));//$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.YEAR, new DatePartFunctionModifier(getLanguageFactory(), "year"));//$NON-NLS-1$ //$NON-NLS-2$
        
        registerFunctionModifier(SourceSystemFunctions.IFNULL, new AliasModifier("coalesce")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.CONVERT, new PostgreSQLConvertModifier(getLanguageFactory())); //$NON-NLS-1$
    }    
    
    @Override
    public String translateLiteralBoolean(Boolean booleanValue) {
        if(booleanValue.booleanValue()) {
            return "TRUE"; //$NON-NLS-1$
        }
        return "FALSE"; //$NON-NLS-1$
    }

    @Override
    public String translateLiteralDate(Date dateValue, Calendar cal) {
        return "DATE '" + formatDateValue(dateValue, cal) + "'"; //$NON-NLS-1$//$NON-NLS-2$
    }

    @Override
    public String translateLiteralTime(Time timeValue, Calendar cal) {
        return "TIME '" + formatDateValue(timeValue, cal) + "'"; //$NON-NLS-1$//$NON-NLS-2$
    }
    
    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue, Calendar cal) {
        return "to_timestamp('" + formatDateValue(timestampValue, cal) + "', 'YYYY-MM-DD HH24:MI:SS.US')"; //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
    }
    
    @Override
    public int getTimestampNanoSecondPrecision() {
    	return 6;
    }
    
    @Override
    public String addLimitString(String queryCommand, ILimit limit) {
        StringBuffer sb = new StringBuffer(queryCommand);
        sb.append(" LIMIT ").append(limit.getRowLimit());
        if (limit.getRowOffset() > 0) {
            sb.append(" OFFSET ").append(limit.getRowOffset());
        }
        return sb.toString();
    }

    /**
     * Postgres doesn't provide min/max(boolean), so this conversion writes a min(BooleanValue) as 
     * bool_and(BooleanValue)
     * @see com.metamatrix.connector.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.connector.language.IAggregate)
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

}
