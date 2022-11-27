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

package org.teiid.query.function.metadata;

/**
 * This interface defines the default set of function category constants.
 */
public interface FunctionCategoryConstants {

    /**
     * "String" functions typically operate on or otherwise manipulate
     *  strings, such as concat, substring, etc.
     */
    public static final String STRING = "String"; //$NON-NLS-1$

    /**
     * "Numeric" functions typically operate on or otherwise manipulate
     *  numbers, such as +, sqrt, etc.
     */
    public static final String NUMERIC = "Numeric"; //$NON-NLS-1$

    /**
     * "Datetime" functions typically operate on or otherwise manipulate
     *  dates, times, or timestamps.
     */
    public static final String DATETIME = "Datetime"; //$NON-NLS-1$

    /**
     * "Conversion" functions convert an object of one type to another type.
     */
    public static final String CONVERSION = "Conversion"; //$NON-NLS-1$

    /**
     * "System" functions expose system information.
     */
    public static final String SYSTEM = "System"; //$NON-NLS-1$

    /**
     * "Miscellaneous" functions are for functions that don't fit in any obvious category.
     */
    public static final String MISCELLANEOUS = "Miscellaneous"; //$NON-NLS-1$

    /**
     * "XML" functions are for manipulating XML documents.
     */
    public static final String XML = "XML"; //$NON-NLS-1$

    /**
     * "JSON" functions are for manipulating JSON documents.
     */
    public static final String JSON = "JSON"; //$NON-NLS-1$

    /**
     * "Security" functions check authentication or authorization information
     */
    public static final String SECURITY = "Security"; //$NON-NLS-1$

    public static final String AGGREGATE = "Aggregate"; //$NON-NLS-1$

    public static final String GEOMETRY = "Geometry"; //$NON-NLS-1$

    public static final String GEOGRAPHY = "Geography"; //$NON-NLS-1$
}
