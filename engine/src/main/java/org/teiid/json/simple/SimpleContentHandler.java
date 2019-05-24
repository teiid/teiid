/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
