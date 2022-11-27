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

package org.teiid.translator.google.api.metadata;


public class Column {
    private String alphaName;
    private String label;
    private SpreadsheetColumnType dataType = SpreadsheetColumnType.STRING;

    public String getAlphaName() {
        return alphaName;
    }

    public void setAlphaName(String alphaName) {
        this.alphaName = alphaName;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public SpreadsheetColumnType getDataType() {
        return dataType;
    }

    public void setDataType(SpreadsheetColumnType dataType) {
        this.dataType = dataType;
    }
}
