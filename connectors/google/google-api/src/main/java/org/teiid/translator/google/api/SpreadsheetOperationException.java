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

package org.teiid.translator.google.api;

/**
 * Any error during quering or inserting data to Google Spreadsheet
 * @author fnguyen
 *
 */
public class SpreadsheetOperationException  extends RuntimeException{

    private static final long serialVersionUID = 4939489286115962870L;

    public SpreadsheetOperationException() {
        super();
    }

    public SpreadsheetOperationException(String message, Throwable cause) {
        super(message, cause);
    }

    public SpreadsheetOperationException(String message) {
        super(message);
    }

    public SpreadsheetOperationException(Throwable cause) {
        super(cause);
    }

}
