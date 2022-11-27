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
package org.teiid.translator;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a property that can be externally configured.
 * The property name will be inferred from the method.
 * Keep in mind that TranslatorProprties name are treated as case-insensitive
 * - do not annotate two methods in the same ExecutionFactory with the same case-insensitive name.
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface TranslatorProperty {

    public static final String EMPTY_STRING = ""; //$NON-NLS-1$

    public enum PropertyType {
        /*
         * Import properties used during the source metadata import
         */
        IMPORT,
        /*
         * Override translator properties are are set on a new translator instance
         */
        OVERRIDE,
        /*
         * Extension metadata properties that are defined for this translator
         */
        EXTENSION_METADATA
    }

    /**
     * Description to be shown in tools
     * @return
     */
    String description() default EMPTY_STRING;

    /**
     * Display name to be shown in tools
     * @return
     */
    String display() default EMPTY_STRING;

    /**
     * True if a non-null value must be supplied
     * @return
     */
    boolean required() default false;

    /**
     * True if the property has no setter
     * @return
     */
    boolean readOnly() default false;

    /**
     * True if this property should be shown in an advanced panel of properties.
     * @return
     */
    boolean advanced() default false;

    /**
     * True if this is property should be masked when displayed - this has no effect on how the value is persisted.
     * @return
     */
    boolean masked() default false;

    /**
     * Defines the type of the translator property.
     * @return
     */
    PropertyType category() default PropertyType.OVERRIDE;
}
