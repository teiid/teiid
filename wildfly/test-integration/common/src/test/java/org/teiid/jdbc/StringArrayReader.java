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

package org.teiid.jdbc;

import java.io.IOException;

import org.teiid.script.io.StringLineReader;


/**
 * Converts a String Array object into a Reader object.
 */
public class StringArrayReader extends StringLineReader {
    String[] source = null;
    int index = 0;

    public StringArrayReader(String[] src) {
        this.source = src;
    }

    protected String nextLine() throws IOException {
        if (index < this.source.length) {
            return this.source[index++]+"\n"; //$NON-NLS-1$
        }
        return null;
    }
}
