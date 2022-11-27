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

import java.lang.annotation.*;

/**
 * Annotates a property that defines a extension metadata property
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface ExtensionMetadataProperty {
    public static final String EMPTY_STRING = ""; //$NON-NLS-1$

    /**
     * Kind of metadata record this property is applicable for
     */
    Class[] applicable() default {Table.class};

    /**
     * Display Name
     */
    String display() default EMPTY_STRING;

    /**
     * Description of the Property
     */
    String description() default EMPTY_STRING;

    /**
     * Is this a required property
     */
    boolean required() default false;

    /**
     * Data type of the property
     * @return
     */
    Class datatype() default java.lang.String.class;

    /**
     * If only takes predefined values, this describes comma separated values
     */
    String allowed() default EMPTY_STRING;
}
