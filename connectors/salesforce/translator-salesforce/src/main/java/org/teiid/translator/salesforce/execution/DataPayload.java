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
package org.teiid.translator.salesforce.execution;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * A bucket to pass data to the Salesforce connection.
 *
 */
public class DataPayload {

    public static class Field {
        public String name;
        public Object value;

        public Field(String name, Object value) {
            this.name = name;
            this.value = value;
        }
    }

    private String type;
    private List<Field> messageElements = new ArrayList<DataPayload.Field>();
    private String id;

    public void setType(String typeName) {
        type = typeName;
    }

    public String getType() {
        return type;
    }

    public void setID(String id) {
        this.id = id;
    }

    public String getID() {
        return id;
    }

    public List<Field> getMessageElements() {
        return messageElements;
    }

    public void addField(String name, Object value) {
        this.messageElements.add(new Field(name, value));
    }

}