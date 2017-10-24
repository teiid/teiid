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
package org.teiid.translator.jdbc.pi;

import static org.teiid.language.SQLConstants.Reserved.*;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.language.*;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.Join.JoinType;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.Procedure;
import org.teiid.translator.jdbc.JDBCPlugin;
import org.teiid.translator.jdbc.SQLConversionVisitor;

public class PISQLConversionVisitor extends SQLConversionVisitor {

	private static final String APPLY = "APPLY";
    PIExecutionFactory executionFactory;
	
	public PISQLConversionVisitor(PIExecutionFactory hef) {
		super(hef);
		this.executionFactory = hef;
	}
	
    @Override
	public void visit(Join obj) {
        TableReference leftItem = obj.getLeftItem();
        TableReference rightItem = obj.getRightItem();
        JoinType joinType = obj.getJoinType();

        boolean ignoreOnClause = false;
        Condition condition = obj.getCondition();
        
        if(useParensForJoins() && leftItem instanceof Join) {
            buffer.append(Tokens.LPAREN);
            append(leftItem);
            buffer.append(Tokens.RPAREN);
        } else {
            append(leftItem);
        }
        buffer.append(Tokens.SPACE);
        
        switch(joinType) {
            case CROSS_JOIN:
                if (hasLateralJoin(rightItem)) {
                    if (!isTVF(rightItem)) {
                        throw new TeiidRuntimeException(JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11025, rightItem));
                    }
                    buffer.append(CROSS)
                        .append(Tokens.SPACE)
                        .append(APPLY)
                        .append(Tokens.SPACE);
                    ignoreOnClause = true;
                } else {
                    // "a cross join b" is the same as "a inner join b on (1 = 1)"
                    buffer.append(INNER)
                        .append(Tokens.SPACE)
                        .append(JOIN)
                        .append(Tokens.SPACE);
                    Literal e1 = LanguageFactory.INSTANCE.createLiteral(new Integer(1), Integer.class);
                    Comparison criteria = new Comparison(e1, e1, Operator.EQ);
                    condition = criteria;
                }
                break;
            case FULL_OUTER_JOIN:
                // should not get here as capabilities are turned off.
                throw new TeiidRuntimeException(JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11024, "FULL OUTER")); 
            case INNER_JOIN:
                buffer.append(INNER)
                    .append(Tokens.SPACE)
                    .append(JOIN)
                    .append(Tokens.SPACE);
                break;
            case LEFT_OUTER_JOIN:
                if (!hasLateralJoin(leftItem) && !hasLateralJoin(rightItem)) {
                    buffer.append(LEFT)
                      .append(Tokens.SPACE)
                      .append(OUTER)                 
                      .append(Tokens.SPACE)
                      .append(JOIN)
                      .append(Tokens.SPACE);
                } else if (hasLateralJoin(leftItem)) {
                    throw new TeiidRuntimeException(JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11024, "RIGHT OUTER APPLY"));
                } else if (hasLateralJoin(rightItem)) {     
                    if (!isTVF(rightItem)) {
                        throw new TeiidRuntimeException(JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11025, rightItem));
                    }  
                    buffer.append(OUTER)
                    .append(Tokens.SPACE)
                    .append(APPLY)                 
                    .append(Tokens.SPACE);
                    ignoreOnClause = true;
                }
                break;
            case RIGHT_OUTER_JOIN:
                // right outer is never pushed, so we should probably never get here.
                throw new TeiidRuntimeException(JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11024, "RIGHT OUTER"));
            default: 
                buffer.append(UNDEFINED);
        }
        
        if(rightItem instanceof Join && (useParensForJoins() || obj.getJoinType() == Join.JoinType.CROSS_JOIN)) {
            buffer.append(Tokens.LPAREN);
            append(hasLateralJoin(rightItem)?unwrap(rightItem):rightItem);
            buffer.append(Tokens.RPAREN);
        } else {
            append(hasLateralJoin(rightItem)?unwrap(rightItem):rightItem);
        }
        
        if (condition != null && !ignoreOnClause) {
            buffer.append(Tokens.SPACE)
              .append(ON)
              .append(Tokens.SPACE);
            append(condition);                    
        }        
    }	
    
    private TableReference unwrap(TableReference table) {
        if (table instanceof DerivedTable) {
            return table;
        } else if (table instanceof NamedProcedureCall) {
            NamedProcedureCall npc = (NamedProcedureCall) table;
            return npc.getCall();
        }        
        return null;
    }

    protected void appendCallStart(Call call) {
        if (!isTVF(call.getMetadataObject())) {
            buffer.append(EXEC)
                  .append(Tokens.SPACE);
        }
    }    
    
    public void visit(ColumnReference obj) {
    	if (obj.getMetadataObject() != null) {
	        ColumnSet<?> cs = obj.getMetadataObject().getParent();
	        if (cs.getParent() instanceof Procedure && isTVF((Procedure)cs.getParent())) {
	            this.shortNameOnly = true;
	            super.visit(obj);
	            this.shortNameOnly = false;
	        } else {
	            super.visit(obj);
	        }
    	} else {
    		super.visit(obj);
    	}
    }    

    public void visit(DerivedTable obj) {
        buffer.append(Tokens.LPAREN);
        append(obj.getQuery());
        buffer.append(Tokens.RPAREN);
        buffer.append(Tokens.SPACE);
        if(useAsInGroupAlias()) {
            buffer.append(AS);
            buffer.append(Tokens.SPACE);
        }
        buffer.append(obj.getCorrelationName());
    }

    protected void appendLateralKeyword() {
        buffer.append(LATERAL);
    }
    
    public void visit(NamedProcedureCall obj) {
        buffer.append(Tokens.LPAREN);
        append(obj.getCall());
        buffer.append(Tokens.RPAREN);
        buffer.append(Tokens.SPACE);
        if(useAsInGroupAlias()) {
            buffer.append(AS);
            buffer.append(Tokens.SPACE);
        }
        buffer.append(obj.getCorrelationName());
    }    
    
    private boolean hasLateralJoin(TableReference table) {
        if (table instanceof DerivedTable) {
            return ((DerivedTable)table).isLateral();
        } else if (table instanceof NamedProcedureCall) {
            return ((NamedProcedureCall)table).isLateral();
        }
        return false;
    }
    
    private boolean isTVF(TableReference table) {
        if (table instanceof NamedTable) {
            String value = ((NamedTable)table).getMetadataObject().getProperty(PIMetadataProcessor.TVF, false);
            return Boolean.parseBoolean(value);
        } else if (table instanceof NamedProcedureCall) {
            String value = ((NamedProcedureCall)table).getCall().getMetadataObject().getProperty(PIMetadataProcessor.TVF, false);
            return Boolean.parseBoolean(value); 
        }
        return false;
    }
    
    private boolean isTVF(Procedure proc) {
        String value = proc.getProperty(PIMetadataProcessor.TVF, false);
        return Boolean.parseBoolean(value); 
    }    
}
