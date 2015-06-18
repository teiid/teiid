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

package org.teiid.json.simple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

public class SimpleContentHandler implements ContentHandler {
	
	private Stack<Object> stack = new Stack<Object>();
	private Stack<String> nameStack = new Stack<String>();
	private Object result;

	@Override
	public void startJSON() throws ParseException, IOException {
		
	}

	@Override
	public void endJSON() throws ParseException, IOException {
		
	}

	@Override
	public boolean startObject() throws ParseException, IOException {
		Map<String, Object> current = new LinkedHashMap<String, Object>();
		stack.add(current);
		return true;
	}

	@Override
	public boolean endObject() throws ParseException, IOException {
		end(stack.pop());
		return true;
	}

	private void end(Object current) {
		if (!stack.isEmpty() && stack.lastElement() instanceof List) {
			((List)stack.lastElement()).add(current);
		} else {
			result = current;
		}
	}

	@Override
	public boolean startObjectEntry(String key) throws ParseException,
			IOException {
		nameStack.push(key);
		return true;
	}

	@Override
	public boolean endObjectEntry() throws ParseException, IOException {
		Object parent = stack.lastElement();
		((Map<String, Object>)parent).put(nameStack.pop(), result);
		return true;
	}

	@Override
	public boolean startArray() throws ParseException, IOException {
		List<Object> current = new ArrayList<Object>();
		stack.add(current);
		return true;
	}

	@Override
	public boolean endArray() throws ParseException, IOException {
		end(stack.pop());
		return true;
	}

	@Override
	public boolean primitive(Object value) throws ParseException, IOException {
		end(value);
		return true;
	}
	
	public Object getResult() {
		return result;
	}
	
}
