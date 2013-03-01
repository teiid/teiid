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

package org.teiid.query.parser;

import java.io.IOException;

/**
 * A customized TokenManager to keep a token history and to treat lexical errors as invalid single character tokens
 */
class TeiidSQLParserTokenManager extends SQLParserTokenManager {

	static final int INVALID_TOKEN = -1;
	int tokenCount = 0;
	Token head;
	
	public TeiidSQLParserTokenManager(JavaCharStream stream) {
		super(stream);
	}
	
	void reinit() {
		tokenCount = 0;
		head = null;
	}
	
	@Override
	public Token getNextToken() {
		try {
			Token t = super.getNextToken();
			//if we're in the default lex state, keep track of a history of tokens
			//this logic is not perfect as deep lookaheads can ruin the positioning
			if (tokenCount == 0) {
				head = t;
			}
			tokenCount++;
			if (tokenCount > 10) {
				head = head.next;
			}
			return t;
		} catch (TokenMgrError err) {
			Token t = new Token();
			t.kind = INVALID_TOKEN;
			t.beginColumn = this.input_stream.getBeginColumn();
			t.beginLine = this.input_stream.getBeginLine();
			t.endColumn = t.beginColumn;
			t.endLine = t.beginLine;
			t.image = this.input_stream.GetImage().substring(0, 1);
			try {
				//mark the char a consumed
				this.input_stream.readChar();
			} catch (IOException e) {
			}
			return t;
		}
	}

}
