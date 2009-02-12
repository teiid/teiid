/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.api.core.xmi;

//import java.text.DateFormat;
//import java.text.SimpleDateFormat;

/**
 * This comment is specified in template 'typecomment'. (Window>Preferences>Java>Templates)
 * <p>
 * The XMI processor is designed to hide as much as possible the semantics of XMI by simply constructing
 * {@link FeatureInfo} and {@link EntityInfo} instances as an XMI stream is processed.  These info classes are
 * thus similar to SAX events of a SAX XML document parser.
 * </p>
 */
public class XMIConstants {

//    public static final String NAMESPACE = "";
    public static final String VERSION = "3.0"; //$NON-NLS-1$

//    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";
//	  public static final DateFormat DATE_FORMATTER = new SimpleDateFormat(TIMESTAMP_FORMAT);


    public static final String DELIMITER = "."; //$NON-NLS-1$
    public static final char DELIMITER_CHAR = DELIMITER.charAt(0);

    /**
     * Static class that defines constants for various element names used in XMI.
     */
    public static class ElementName {
        /**
         * Static constant that represents the name of the "XMI" (root) element.
         */
        public static final String XMI                  = "XMI"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.header" element.
         */
        public static final String HEADER               = "XMI.header"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.content" element.
         */
        public static final String CONTENT              = "XMI.content"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.extensions" element.
         */
        public static final String EXTENSIONS           = "XMI.extensions"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.extension" element.
         */
        public static final String EXTENSION            = "XMI.extension"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.documentation" element.
         */
        public static final String DOCUMENTATION        = "XMI.documentation"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.owner" element.
         */
        public static final String OWNER                = "XMI.owner"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.contact" element.
         */
        public static final String CONTACT              = "XMI.contact"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.longDescription" element.
         */
        public static final String LONG_DESCRIPTION     = "XMI.longDescription"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.shortDescription" element.
         */
        public static final String SHORT_DESCRIPTION    = "XMI.shortDescription"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.exporter" element.
         */
        public static final String EXPORTER             = "XMI.exporter"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.exporterVersion" element.
         */
        public static final String EXPORTER_VERSION     = "XMI.exporterVersion"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.exporterID" element.
         */
        public static final String EXPORTER_ID          = "XMI.exporterID"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.notice" element.
         */
        public static final String NOTICE               = "XMI.notice"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.model" element.
         */
        public static final String MODEL                = "XMI.model"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.metamodel" element.
         */
        public static final String METAMODEL            = "XMI.metamodel"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.metametamodel" element.
         */
        public static final String METAMETAMODEL        = "XMI.metametamodel"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.import" element.
         */
        public static final String IMPORT               = "XMI.import"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.difference" element.
         */
        public static final String DIFFERENCE           = "XMI.difference"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.delete" element.
         */
        public static final String DELETE               = "XMI.delete"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.add" element.
         */
        public static final String ADD                  = "XMI.add"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.replace" element.
         */
        public static final String REPLACE              = "XMI.replace"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.reference" element.
         */
        public static final String REFERENCE            = "XMI.reference"; //$NON-NLS-1$

        /**
         * Static constant that represents the name of the "XMI.field" element.
         * This is required by only some metamodels.
         */
        public static final String FIELD                = "XMI.field"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.struct" element.
         * This is required by only some metamodels.
         */
        public static final String STRUCT               = "XMI.struct"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.seqItem" element.
         * This is required by only some metamodels.
         */
        public static final String SEQ_ITEM             = "XMI.seqItem"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.sequence" element.
         * This is required by only some metamodels.
         */
        public static final String SEQUENCE             = "XMI.sequence"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.arrayLength" element.
         * This is required by only some metamodels.
         */
        public static final String ARRAY_LENGTH         = "XMI.arrayLength"; //$NON-NLS-1$
         /**
         * Static constant that represents the name of the "XMI.array" element.
         * This is required by only some metamodels.
         */
       public static final String ARRAY                = "XMI.array"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.enum" element.
         * This is required by only some metamodels.
         */
        public static final String ENUM                 = "XMI.enum"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.discrim" element.
         * This is required by only some metamodels.
         */
        public static final String DISCRIM              = "XMI.discrim"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.union" element.
         * This is required by only some metamodels.
         */
        public static final String UNION                = "XMI.union"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.any" element.
         * This is required by only some metamodels.
         */
        public static final String ANY                  = "XMI.any"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "XMI.value" element.
         * This is required by only some metamodels.
         */
        public static final String VALUE                = "XMI.value"; //$NON-NLS-1$

    }

    /**
     * Static class that defines constants for various attributes names used in XMI.
     */
    public static class AttributeName {
        /**
         * Static constant that represents the name of the "xmi.version" attribute.
         */
        public static final String XMI_VERSION          = "xmi.version"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "timestamp attribute.
         */
        public static final String TIMESTAMP            = "timestamp"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "verified" attribute.
         */
        public static final String VERIFIED             = "verified"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "xmi.id" attribute.
         */
        public static final String XMI_ID               = "xmi.id"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "xmi.label" attribute.
         */
        public static final String XMI_LABEL            = "xmi.label"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "xmi.uuid" attribute.
         */
        public static final String XMI_UUID             = "xmi.uuid"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "href" attribute.
         */
        public static final String HREF                 = "href"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "xmi.idref" attribute.
         */
        public static final String XMI_IDREF            = "xmi.idref"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "xmi.name" attribute.
         */
        public static final String XMI_NAME             = "xmi.name"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "xmi.extender" attribute.
         */
        public static final String XMI_EXTENDER         = "xmi.extender"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "xmi.extenderID" attribute.
         */
        public static final String XMI_EXTENDER_ID      = "xmi.extenderID"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "xmi.position" attribute.
         */
        public static final String XMI_POSITION         = "xmi.position"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "xmi.value" attribute.
         */
        public static final String XMI_VALUE            = "xmi.value"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "xmi.type" attribute.
         */
        public static final String XMI_TYPE             = "xmi.type"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "xmi.tcName" attribute.
         */
        public static final String XMI_TC_NAME          = "xmi.tcName"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "xmi.tcScale" attribute.
         */
        public static final String XMI_TC_ID            = "xmi.tcId"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "xmi.tcScale" attribute.
         */
        public static final String XMI_TC_LENGTH        = "xmi.tcLength"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "xmi.tcScale" attribute.
         */
        public static final String XMI_TC_DIGITS        = "xmi.tcDigits"; //$NON-NLS-1$
        /**
         * Static constant that represents the name of the "xmi.tcScale" attribute.
         */
        public static final String XMI_TC_SCALE         = "xmi.tcScale"; //$NON-NLS-1$
    }

    /**
     * Static class that defines constants for various values used in XMI.
     */
    public static class Values {
        /**
         * The constant value for the "true" boolean condition.
         */
        public static final String TRUE                 = Boolean.TRUE.toString();
        /**
         * The constant value for the "false" boolean condition.
         */
        public static final String FALSE                = Boolean.FALSE.toString();
//        private static final String YES                 = "yes";
//        private static final String NO                  = "no";
//        private static final String DONT_CARE           = "dont_care";

        /**
         * Static class that defines constants for various values of visibility.
         */
        public static class Visibility {
            /**
             * The constant value for "public" visibility.
             */
            public static final String PUBLIC           = "public_vis"; //$NON-NLS-1$
            /**
             * The constant value for "protected" visibility.
             */
            public static final String PROTECTED        = "protected_vis"; //$NON-NLS-1$
            /**
             * The constant value for "private" visibility.
             */
            public static final String PRIVATE          = "private_vis"; //$NON-NLS-1$
        }
        /**
         * Static class that defines constants for various values of scope.
         */
        public static class Scope {
            /**
             * The constant value for classifier-level scope.
             */
            public static final String INSTANCE_LEVEL   = "instance_level"; //$NON-NLS-1$
            /**
             * The constant value for classifier-level scope.
             */
            public static final String CLASSIFIER_LEVEL = "classifier_level"; //$NON-NLS-1$
            //public static final String PROCESSED_REQUEST= "asProcessed";
        }
        /**
         * Static class that defines constants for various values specifying parameter direction.
         */
        public static class Direction {
            /**
             * The constant value denoting an input parmeter.
             */
            public static final String IN               = "in_dir"; //$NON-NLS-1$
            /**
             * The constant value denoting an output parmeter.
             */
            public static final String OUT              = "out_dir"; //$NON-NLS-1$
            /**
             * The constant value denoting a parmeter that is both an input and an output.
             */
            public static final String INOUT            = "inout_dir"; //$NON-NLS-1$
            /**
             * The constant value denoting a return parmeter.
             */
            public static final String RETURN           = "return_dir"; //$NON-NLS-1$
        }
        /**
         * Static class that defines constants for various modes of evaluation.
         */
        public static class EvaluationPolicy {
            /**
             * The constant value denoting immediate evaluation.
             */
            public static final String IMMEDIATE        = "immediate"; //$NON-NLS-1$
            /**
             * The constant value denoting deferred evaluation.
             */
            public static final String DEFERRED         = "deferred"; //$NON-NLS-1$
        }
    }

}
