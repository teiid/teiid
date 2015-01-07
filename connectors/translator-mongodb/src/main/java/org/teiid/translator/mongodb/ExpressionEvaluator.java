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
package org.teiid.translator.mongodb;

import java.util.ArrayList;
import java.util.EmptyStackException;
import java.util.List;
import java.util.Stack;

import org.teiid.language.*;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.mongodb.MongoDBUpdateExecution.RowInfo;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.DBRef;

public class ExpressionEvaluator extends HierarchyVisitor {
	private BasicDBObject row;
	private RowInfo rowInfo;
	private MongoDBExecutionFactory executionFactory; 
	private DB mongoDB;
	protected Stack<Boolean> match = new Stack<Boolean>();
	protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();

	public static boolean matches(MongoDBExecutionFactory executionFactory, DB mongoDB, Condition condition, BasicDBObject row, RowInfo rowInfo) throws TranslatorException {
		ExpressionEvaluator evaluator = new ExpressionEvaluator(executionFactory, mongoDB, row, rowInfo);
		evaluator.append(condition);
		if (!evaluator.exceptions.isEmpty()) {
			throw evaluator.exceptions.get(0);
		}
		try {
			return evaluator.match.pop();
		} catch (EmptyStackException e) {
			return true;
		}
	}

	private ExpressionEvaluator(MongoDBExecutionFactory executionFactory, DB mongoDB, BasicDBObject row, RowInfo rowInfo) {
	    this.executionFactory = executionFactory;
	    this.mongoDB  = mongoDB;
		this.row = row;
		this.rowInfo = rowInfo;
	}

	@Override
	public void visit(Comparison obj) {
		try {
			Object o1 = getRowValue(obj.getLeftExpression());
			Object o2 = getLiteralValue(obj.getRightExpression());
			int compare = ((Comparable<Object>)o1).compareTo(o2);
			switch(obj.getOperator()) {
			case EQ:
				this.match.push(Boolean.valueOf(compare == 0));
				break;
			case NE:
				this.match.push(Boolean.valueOf(compare != 0));
				break;
			case LT:
				this.match.push(Boolean.valueOf(compare < 0));
				break;
			case LE:
				this.match.push(Boolean.valueOf(compare <= 0));
				break;
			case GT:
				this.match.push(Boolean.valueOf(compare > 0));
				break;
			case GE:
				this.match.push(Boolean.valueOf(compare >= 0));
				break;
			}
		} catch (TranslatorException e) {
			this.exceptions.add(e);
		}
	}

	private Object getRowValue(Expression obj) throws TranslatorException {
		if (!(obj instanceof ColumnReference)) {
			throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18017));
		}
		ColumnReference column = (ColumnReference)obj;
		
		Object value = null;
		if (MongoDBSelectVisitor.isPartOfPrimaryKey(column.getTable().getMetadataObject(), column.getName())) {
			// this is true one to many case
		    value = this.row.get("_id"); //$NON-NLS-1$
			if (value == null) {
			    value = getValueFromRowInfo(column, value);
			}
		}
        if (value == null && MongoDBSelectVisitor.isPartOfForeignKey(column.getTable().getMetadataObject(), column.getName())) {
            value = getValueFromRowInfo(column, value);		    
		}
		if (value == null) {
		    value = this.row.get(column.getName());
		}
		if (value instanceof DBRef) {
			value = ((DBRef)value).getId();
		}
		if (value instanceof DBObject) {
			value = ((DBObject) value).get(column.getName());
		}		
		return this.executionFactory.retrieveValue(value, column.getType(), this.mongoDB, column.getName(), column.getName());
	}

    private Object getValueFromRowInfo(ColumnReference column, Object value) {
        String tableName = column.getTable().getMetadataObject().getName();
        if (this.rowInfo.tableName.equals(tableName) || this.rowInfo.mergedTableName.equals(tableName)) {
            //  one to one case
            value = this.rowInfo.PK;
        }
        else {
            // one to many case                 
            RowInfo info = this.rowInfo;
            while(info.parent != null) {
                info = info.parent;
                if (info.tableName.equals(tableName) || info.mergedTableName.equals(tableName)) {
                    value = info.PK;
                    break;
                }                       
            }
        }
        return value;
    }

	private Object getLiteralValue(Expression obj) throws TranslatorException {
		if (!(obj instanceof Literal)) {
			throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18018));
		}
		Literal right = (Literal)obj;
		return right.getValue();
	}

	@Override
    public void visit(AndOr obj) {
		append(obj.getLeftCondition());
		append(obj.getRightCondition());

		if (!this.exceptions.isEmpty()) {
			return;
		}

        boolean right = this.match.pop();
        boolean left = this.match.pop();

        switch(obj.getOperator()) {
        case AND:
        	this.match.push(right && left);
        	break;
        case OR:
        	this.match.push(right || left);
        	break;
        }
    }


	@Override
	public void visit(In obj) {
		try {
			Object o1 = getRowValue(obj.getLeftExpression());

			ArrayList<Object> values = new ArrayList<Object>();
			for (int i = 0; i < obj.getRightExpressions().size(); i++) {
				values.add(getLiteralValue(obj.getRightExpressions().get(i)));
			}

			if (obj.isNegated()) {
				this.match.push(!values.contains(o1));
			} else {
				this.match.push(values.contains(o1));
			}
		} catch (TranslatorException e) {
			this.exceptions.add(e);
		}
	}


	@Override
	public void visit(IsNull obj) {
		try {
			Object o1 = getRowValue(obj.getExpression());

			if (obj.isNegated()) {
				this.match.push(o1 != null);
			}
			else {
				this.match.push(o1 == null);
			}
		} catch (TranslatorException e) {
			this.exceptions.add(e);
		}
	}

	@Override
	public void visit(Like obj) {
		try {
			Object o1 = getRowValue(obj.getLeftExpression());
			Object o2 = getLiteralValue(obj.getRightExpression());

			if (o1 instanceof String && o2 instanceof String) {
				String value = (String)o1;
				if (obj.isNegated()) {
					this.match.push(!value.matches((String)o2));
				}
				else {
					this.match.push(value.matches((String)o2));
				}
			}
			else {
				this.exceptions.add(new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18019)));
			}
		} catch (TranslatorException e) {
			this.exceptions.add(e);
		}
	}


    /**
     * Appends the string form of the LanguageObject to the current buffer.
     * @param obj the language object instance
     */
    public void append(LanguageObject obj) {
        if (obj != null) {
            visitNode(obj);
        }
    }

    /**
     * Simple utility to append a list of language objects to the current buffer
     * by creating a comma-separated list.
     * @param items a list of LanguageObjects
     */
    protected void append(List<? extends LanguageObject> items) {
        if (items != null && items.size() != 0) {
            append(items.get(0));
            for (int i = 1; i < items.size(); i++) {
                append(items.get(i));
            }
        }
    }

    /**
     * Simple utility to append an array of language objects to the current buffer
     * by creating a comma-separated list.
     * @param items an array of LanguageObjects
     */
    protected void append(LanguageObject[] items) {
        if (items != null && items.length != 0) {
            append(items[0]);
            for (int i = 1; i < items.length; i++) {
                append(items[i]);
            }
        }
    }
}
