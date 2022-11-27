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

package org.teiid.query.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

/**
 * This is the primary interface for all language objects.  It extends a few
 * key interfaces and adds some additional methods to allow the {@link LanguageVisitor}
 * to work.
 */
public interface LanguageObject extends Cloneable {

    /**
     * Method for accepting a visitor.  It is the responsibility of the
     * language object to call back on the visitor.
     * @param visitor Visitor being used
     */
    void acceptVisitor(LanguageVisitor visitor);

    /**
     * Implement clone to make objects cloneable.
     * @return Deep clone of this object
     */
    Object clone();

    public static class Util {

        public static <S extends LanguageObject, T extends S> ArrayList<S> deepClone(Collection<T> collection, Class<S> type) {
            if (collection == null) {
                return null;
            }
            ArrayList<S> result = new ArrayList<S>(collection.size());
            for (LanguageObject obj : collection) {
                result.add(type.cast(obj.clone()));
            }
            return result;
        }

        @SuppressWarnings("unchecked")
        public static <T extends LanguageObject> T[] deepClone(T[] collection) {
            if (collection == null) {
                return null;
            }
            T[] copy = Arrays.copyOf(collection, collection.length);
            for (int i = 0; i < copy.length; i++) {
                LanguageObject t = copy[i];
                copy[i] = (T) t.clone();
            }
            return copy;
        }

    }

}
