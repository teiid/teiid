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
import java.util.Iterator;
import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;

/**
 * Represents the only allowable expression for the textagg aggregate.
 * This is a Teiid specific construct.
 */
public class TextLine implements Expression {
	public static final String nl = System.getProperty("line.separator"); //$NON-NLS-1$

	private Character delimiter;
	private Character quote;
	private boolean includeHeader;
	private List<DerivedColumn> expressions;
	private String encoding;
	private String lineEnding = nl;
	
	public Character getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(Character delimiter) {
		this.delimiter = delimiter;
	}
	
	public String getEncoding() {
		return encoding;
	}
	
	public void setEncoding(String encoding) {
		this.encoding = encoding;
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
	
	public String getLineEnding() {
		return lineEnding;
	}
	
	public void setLineEnding(String lineEnding) {
		this.lineEnding = lineEnding;
	}
	
	@Override
	public Class<?> getType() {
		return String[].class;
	}

	@Override
	public void acceptVisitor(LanguageVisitor visitor) {
		visitor.visit(this);
	}
	
	@Override
	public TextLine clone() {
		TextLine clone = new TextLine();
		clone.expressions = LanguageObject.Util.deepClone(this.expressions, DerivedColumn.class);
		clone.delimiter = this.delimiter;
		clone.quote = this.quote;
		clone.includeHeader = this.includeHeader;
		clone.encoding = this.encoding;
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
			  && this.includeHeader == other.includeHeader
			  && EquivalenceUtil.areEqual(this.encoding, other.encoding);
	}

	@Override
	public int hashCode() {
		return HashCodeUtil.expHashCode(0, this.expressions);
	}	
	
	@Override
	public String toString() {
		return SQLStringVisitor.getSQLString(this);
	}	
	
	public static interface ValueExtractor<T> {
		Object getValue(T t);
	}
	
	public static <T> String[] evaluate(final List<T> values, ValueExtractor<T> valueExtractor, TextLine textLine) throws TransformationException {
		Character delimeter = textLine.getDelimiter();
		if (delimeter == null) {
			delimeter = Character.valueOf(',');
		}
		String delim = String.valueOf(delimeter.charValue());
		Character quote = textLine.getQuote();
		String quoteStr = null;		
		if (quote == null) {
			quoteStr = "\""; //$NON-NLS-1$
		} else {
			quoteStr = String.valueOf(quote);
		}
		String doubleQuote = quoteStr + quoteStr;
		ArrayList<String> result = new ArrayList<String>();
		for (Iterator<T> iterator = values.iterator(); iterator.hasNext();) {
			T t = iterator.next();
			String text = (String)DataTypeManager.transformValue(valueExtractor.getValue(t), DataTypeManager.DefaultDataClasses.STRING);
			if (text == null) {
				continue;
			}
			result.add(quoteStr);
			result.add(StringUtil.replaceAll(text, quoteStr, doubleQuote));
			result.add(quoteStr);
			if (iterator.hasNext()) {
				result.add(delim);
			}			
		}
		result.add(textLine.getLineEnding());
		return result.toArray(new String[result.size()]);
	}
	
}
