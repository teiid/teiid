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
package org.teiid.query.sql.symbol;

import java.util.ArrayList;
import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class TextLine implements Expression {
	public static String nl = System.getProperty("line.separator"); //$NON-NLS-1$

	private Character delimiter = null;
	private Character quote = null;
	private boolean includeHeader;
	private List<DerivedColumn> expressions;
	
	public Character getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(Character delimiter) {
		this.delimiter = delimiter;
	}

	public Character getQuote() {
		return quote;
	}

	public void setQuote(Character quote) {
		this.quote = quote;
	}

	public boolean isIncludeHeader() {
		return includeHeader;
	}

	public void setIncludeHeader(boolean includeHeader) {
		this.includeHeader = includeHeader;
	}

	public List<DerivedColumn> getExpressions() {
		return expressions;
	}

	public void setExpressions(List<DerivedColumn> expressions) {
		this.expressions = expressions;
	}	
	
	@Override
	public Class<?> getType() {
		return DataTypeManager.DefaultDataClasses.CLOB;
	}

	@Override
	public boolean isResolved() {
		for (DerivedColumn arg : this.expressions) {
			if (!arg.getExpression().isResolved()) {
				return false;
			}
		}
		return true;
	}

	@Override
	public void acceptVisitor(LanguageVisitor visitor) {
		visitor.visit(this);
	}
	
	@Override
	public TextLine clone() {
		TextLine clone = new TextLine();

		if (this.expressions != null && !this.expressions.isEmpty()) {
			List<DerivedColumn> list = new ArrayList<DerivedColumn>();
			for (DerivedColumn expr:this.expressions) {
				list.add(expr.clone());
			}
			clone.expressions = list;
		}
		
		if (this.delimiter != null) {
			clone.delimiter = new Character(this.delimiter);
		}
		
		if (this.quote != null) {
			clone.quote = new Character(this.quote);
		}
		
		clone.includeHeader = this.includeHeader;
		return clone;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof TextLine)) {
			return false;
		}
		TextLine other = (TextLine)obj;
		return EquivalenceUtil.areEqual(this.expressions, other.expressions)
			  && EquivalenceUtil.areEqual(this.delimiter, other.delimiter)
			  && EquivalenceUtil.areEqual(this.quote, other.quote)
			  && this.includeHeader == other.includeHeader;
	}

	@Override
	public int hashCode() {
		return HashCodeUtil.expHashCode(0, this.expressions);
	}	
	
	@Override
	public String toString() {
		return SQLStringVisitor.getSQLString(this);
	}	
	
	public static String evaluate(final Evaluator.NameValuePair[] values, Character delimeter, Character quote) {
				
		if (delimeter == null) {
			delimeter = new Character(',');
		}
				
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < values.length; i++) {
			if (values[i].value != null) {
				addQuote(quote, sb);
				sb.append(values[i].value);
				addQuote(quote, sb);
			}
			if (i < values.length-1) {
				sb.append(delimeter);
			}			
		}
		sb.append(nl);
		
		return sb.toString();
	}

	public static String getHeader(List<DerivedColumn> args, Character delimeter, Character quote) {
		
		if (delimeter == null) {
			delimeter = new Character(',');
		}		
		
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < args.size(); i++) {
			DerivedColumn symbol = args.get(i);
			String name = symbol.getAlias();
			Expression ex = symbol.getExpression();
			if (name == null && ex instanceof ElementSymbol) {
				name = ((ElementSymbol)ex).getShortName();
			}
			addQuote(quote, sb);
			sb.append(name);
			addQuote(quote, sb);
			
			if (i < args.size()-1) {
				sb.append(delimeter);
			}
		}
		sb.append(nl);
		return sb.toString();
	}
	
	private static void addQuote(Character quote, StringBuilder sb) {
		if (quote != null) {
			sb.append(quote);
		}
	}
}
