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
package org.teiid.translator.mongodb;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import com.mongodb.BasicDBObject;

public class IDRef implements Cloneable {
    LinkedHashMap<String, Object> pk = new LinkedHashMap<String, Object>();

    public void addColumn(String key, Object value) {
        // only add if not added before
        if (this.pk.get(key) == null) {
            this.pk.put(key, value);
        }
    }

    public List<String> getKeys(){
        return new ArrayList<String>(this.pk.keySet());
    }

    public Object getValue() {
        if (this.pk.size() == 1) {
            for (String key:this.pk.keySet()) {
                return this.pk.get(key);
            }
        }
        BasicDBObject value = new BasicDBObject();
        for (String key:this.pk.keySet()) {
            value.append(key, this.pk.get(key));
        }
        return value;
    }

    @Override
    public String toString() {
        Object obj =  getValue();
        if (obj != null) {
            return obj.toString();
        }
        return null;
    }

    @Override
    public IDRef clone() {
        IDRef clone = new IDRef();
        clone.pk.putAll(this.pk);
        return clone;
    }
}
