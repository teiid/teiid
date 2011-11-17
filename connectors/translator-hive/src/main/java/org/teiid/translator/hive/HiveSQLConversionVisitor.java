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
package org.teiid.translator.hive;

import static org.teiid.language.SQLConstants.Reserved.ALL;
import static org.teiid.language.SQLConstants.Reserved.DISTINCT;
import static org.teiid.language.SQLConstants.Reserved.FROM;
import static org.teiid.language.SQLConstants.Reserved.FULL;
import static org.teiid.language.SQLConstants.Reserved.JOIN;
import static org.teiid.language.SQLConstants.Reserved.LEFT;
import static org.teiid.language.SQLConstants.Reserved.ON;
import static org.teiid.language.SQLConstants.Reserved.OUTER;
import static org.teiid.language.SQLConstants.Reserved.RIGHT;
import static org.teiid.language.SQLConstants.Reserved.SELECT;

import java.util.List;

import org.teiid.language.ColumnReference;
import org.teiid.language.Condition;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.Join;
import org.teiid.language.Limit;
import org.teiid.language.OrderBy;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.Select;
import org.teiid.language.SetQuery;
import org.teiid.language.TableReference;
import org.teiid.translator.jdbc.SQLConversionVisitor;

public class HiveSQLConversionVisitor extends SQLConversionVisitor {

	public HiveSQLConversionVisitor(HiveExecutionFactory hef) {
		super(hef);
	}
	
    public void visit(Join obj) {
        TableReference leftItem = obj.getLeftItem();
        if(useParensForJoins() && leftItem instanceof Join) {
            buffer.append(Tokens.LPAREN);
            append(leftItem);
            buffer.append(Tokens.RPAREN);
        } else {
            append(leftItem);
        }
        buffer.append(Tokens.SPACE);
        
        switch(obj.getJoinType()) {
            case CROSS_JOIN:
            	// Hive just works with "JOIN" keyword no inner or cross
                //buffer.append(CROSS); 
                break;
            case FULL_OUTER_JOIN:
                buffer.append(FULL)
                      .append(Tokens.SPACE)
                      .append(OUTER);
                break;
            case INNER_JOIN:
            	// Hive just works with "JOIN" keyword no inner or cross
                //buffer.append(INNER);
                break;
            case LEFT_OUTER_JOIN:
                buffer.append(LEFT)
                      .append(Tokens.SPACE)
                      .append(OUTER);
                break;
            case RIGHT_OUTER_JOIN:
                buffer.append(RIGHT)
                      .append(Tokens.SPACE)
                      .append(OUTER);
                break;
            default: buffer.append(UNDEFINED);
        }
        buffer.append(Tokens.SPACE)
              .append(JOIN)
              .append(Tokens.SPACE);
        
        TableReference rightItem = obj.getRightItem();
        if(rightItem instanceof Join && (useParensForJoins() || obj.getJoinType() == Join.JoinType.CROSS_JOIN)) {
            buffer.append(Tokens.LPAREN);
            append(rightItem);
            buffer.append(Tokens.RPAREN);
        } else {
            append(rightItem);
        }
        
        final Condition condition = obj.getCondition();
        if (condition != null) {
            buffer.append(Tokens.SPACE)
                  .append(ON)
                  .append(Tokens.SPACE);
            append(condition);                    
        }        
    }	
    
    public void addColumns(List<DerivedColumn> items) {
        if (items != null && items.size() != 0) {
        	addColumn(items.get(0));
            for (int i = 1; i < items.size(); i++) {
                buffer.append(Tokens.COMMA)
                      .append(Tokens.SPACE);
                addColumn(items.get(i));
            }
        }    	
    }

	private void addColumn(DerivedColumn dc) {
		if (dc.getAlias() != null) {
		    buffer.append(dc.getAlias());
		}
		else {
			Expression expr = dc.getExpression();
			if (expr instanceof ColumnReference) {
				buffer.append(((ColumnReference)expr).getName());
			}
			else {
				append(expr);
			}
		}
	}
    
    @Override
    public void visit(SetQuery obj) {
    	if (obj.getWith() != null) {
    		append(obj.getWith());
    	}
    	
    	Select select =  (Select)obj.getLeftQuery();
    	buffer.append(SELECT).append(Tokens.SPACE);
    	if(!obj.isAll()) {
    		buffer.append(DISTINCT).append(Tokens.SPACE);
    	}
    	addColumns(select.getDerivedColumns());
    	buffer.append(Tokens.SPACE);
    	buffer.append(FROM).append(Tokens.SPACE);
    	buffer.append(Tokens.LPAREN);
    	 
        appendSetQuery(obj, obj.getLeftQuery(), false);
        
        buffer.append(Tokens.SPACE);
        
        appendSetOperation(obj.getOperation());

        // UNION "ALL" always
        buffer.append(Tokens.SPACE);
        buffer.append(ALL);                
        buffer.append(Tokens.SPACE);

        appendSetQuery(obj, obj.getRightQuery(), true);
        
        OrderBy orderBy = obj.getOrderBy();
        if(orderBy != null) {
            buffer.append(Tokens.SPACE);
            append(orderBy);
        }

        Limit limit = obj.getLimit();
        if(limit != null) {
            buffer.append(Tokens.SPACE);
            append(limit);
        }
        buffer.append(Tokens.RPAREN);
        buffer.append(Tokens.SPACE);
        buffer.append("X__"); //$NON-NLS-1$
    }    
}
