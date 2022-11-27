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
package org.teiid.metadata;


public class RestMetadataExtension {
    public enum ParameterType {
        PATH,QUERY,FORM,FORMDATA,BODY,HEADER;
    }

    public final static String URI = "teiid_rest:URI";
    public final static String METHOD = "teiid_rest:METHOD";
    public final static String SCHEME = "teiid_rest:SCHEME";
    public final static String PRODUCES = "teiid_rest:PRODUCES";
    public final static String CONSUMES = "teiid_rest:CONSUMES";
    public final static String CHARSET = "teiid_rest:CHARSET";
    public final static String PARAMETER_TYPE = "teiid_rest:PARAMETER_TYPE";
    public final static String COLLECION_FORMAT = "teiid_rest:COLLECION_FORMAT";
}
