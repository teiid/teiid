
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

package org.teiid.translator.swagger;

import java.util.List;

import org.teiid.metadata.ProcedureParameter;
import org.teiid.translator.document.Document;

public class SwaggerBodyInputDocument extends Document {

    public void addArgument(ProcedureParameter param, Object payload) {
        if (payload == null) {
            return;
        }
        String nis = param.getNameInSource();
        if (nis == null) {
            nis = param.getName();
        }
        addArgument(this, nis, payload);
    }

    private void addArgument(Document document, String nis,
            Object payload) {
        int index = nis.indexOf('/');
        if (index == -1) {
            if (payload instanceof Object[]) {
                Object[] array = (Object[])payload;
                for (Object anObj:array) {
                    document.addArrayProperty(nis, anObj);
                }
            } else {
                document.addProperty(nis, payload);
            }
        } else {
            String childPath = nis.substring(0, index);
            List<? extends Document> child = document.getChildDocuments(childPath);
            String propertyName = nis.substring(index+1);
            if (child == null) {
                boolean array = false;
                if (childPath.endsWith("[]")) {
                    childPath = childPath.substring(0, childPath.length()-2);
                    propertyName = nis.substring(nis.indexOf("/",index+1)+1);
                    array = true;
                }
                Document d  = new Document(childPath, array, this);
                child = document.addChildDocument(childPath, d);
            }
            addArgument(child.get(0), propertyName, payload);
        }
    }


}
