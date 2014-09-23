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
package org.teiid.olingo;

import org.apache.olingo.server.api.uri.queryoption.expression.Alias;
import org.apache.olingo.server.api.uri.queryoption.expression.Binary;
import org.apache.olingo.server.api.uri.queryoption.expression.Enumeration;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.LambdaRef;
import org.apache.olingo.server.api.uri.queryoption.expression.Literal;
import org.apache.olingo.server.api.uri.queryoption.expression.Member;
import org.apache.olingo.server.api.uri.queryoption.expression.Method;
import org.apache.olingo.server.api.uri.queryoption.expression.TypeLiteral;
import org.apache.olingo.server.api.uri.queryoption.expression.Unary;

public class ODataExpressionVisitor {
	void visit(@SuppressWarnings("unused") Alias expr) {}
	void visit(@SuppressWarnings("unused") Binary expr) {}
	void visit(@SuppressWarnings("unused") Enumeration expr) {}
	void visit(@SuppressWarnings("unused") LambdaRef expr) {}
	void visit(@SuppressWarnings("unused") Literal expr) {}
	void visit(@SuppressWarnings("unused") Member expr) {}
	void visit(@SuppressWarnings("unused") Method expr) {}
	void visit(@SuppressWarnings("unused") TypeLiteral expr) {}
	void visit(@SuppressWarnings("unused") Unary expr) {}
	
	public void accept(Expression expr) {
	
		if (expr instanceof Alias) {
			visit((Alias)expr);
		}
		else if (expr instanceof Binary) {
			visit((Binary)expr);
		}
		else if (expr instanceof Enumeration) {
			visit((Enumeration)expr);
		}
		else if (expr instanceof LambdaRef) {
			visit((LambdaRef)expr);
		}
		else if (expr instanceof Literal) {
			visit((Literal)expr);
		}
		else if (expr instanceof Member) {
			visit((Member)expr);
		}
		else if (expr instanceof Method) {
			visit((Method)expr);
		}
		else if (expr instanceof TypeLiteral) {
			visit((TypeLiteral)expr);
		}
		else if (expr instanceof Unary) {
			visit((Unary)expr);
		}
	}
}
