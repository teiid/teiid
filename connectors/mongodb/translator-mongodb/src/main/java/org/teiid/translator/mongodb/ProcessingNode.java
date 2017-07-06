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

import org.teiid.translator.TranslatorException;

import com.mongodb.BasicDBObject;

public abstract class ProcessingNode {

    private MongoDocument document;

    public ProcessingNode(MongoDocument document) {
        this.document = document;
    }

    public abstract BasicDBObject getInstruction() throws TranslatorException;

    public String getDocumentName() throws TranslatorException {
        String alias = document.getAlias();
        return alias != null ? alias:document.getQualifiedName(false);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((document == null) ? 0 : document.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ProcessingNode other = (ProcessingNode) obj;
        if (document == null) {
            if (other.document != null) {
                return false;
            }
        } else if (!document.equals(other.document)) {
            return false;
        }
        return true;
    }
}
