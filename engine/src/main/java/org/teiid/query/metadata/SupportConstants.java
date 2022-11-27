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

package org.teiid.query.metadata;

public class SupportConstants {

    private SupportConstants() {}

    public static class Model {
        private Model() {}
    }

    public static class Group {
        private Group() {}

        public static final int UPDATE = 0;
    }

    public static class Element {
        private Element() {}

        public static final int SELECT = 0;
        public static final int SEARCHABLE_LIKE = 1;
        public static final int SEARCHABLE_COMPARE = 2;
        public static final int SEARCHABLE_EQUALITY = 3;
        public static final int NULL = 4;
        public static final int UPDATE = 5;
        public static final int DEFAULT_VALUE = 7;
        public static final int AUTO_INCREMENT = 8;
        public static final int CASE_SENSITIVE = 9;
        public static final int NULL_UNKNOWN = 10;
        public static final int SIGNED = 11;
    }

}
