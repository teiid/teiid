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

package org.teiid.internal.core.xml;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.jdom.Attribute;
import org.jdom.Comment;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.ProcessingInstruction;
import org.jdom.Text;
import org.jdom.input.SAXBuilder;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;
import org.teiid.core.CorePlugin;
import org.teiid.core.util.ArgCheck;


public class JdomHelper {

    /** XML Schema namespace and prefix */
//    private static String SCHEMA_NAMESPACE = "http://www.w3.org/1999/XMLSchema-instance";
//    private static String SCHEMA_NAMESPACE_PREFIX = "xsi";

    /** Default indentation upon write */
    public static final String DEFAULT_INDENT = "  "; //$NON-NLS-1$

    /** Default validation upon reading */
    public static final boolean DEFAULT_VALIDATION = false;

    /** JDOM tree navigation specifiers ***/
    public static final int PRE_ORDER_TRAVERSAL   = 0;
    public static final int POST_ORDER_TRAVERSAL  = 1;
    public static final int LEVEL_ORDER_TRAVERSAL = 2;

    /**
     * <p>
     * Creates an instance of the JDOM Document for the specified file.
     * </p>
     * @param filename <code>String</code> name of file.
     * @param the document instance
     * @throws IOException if the file does not exist.
     * @throws JDOMException when errors occur in parsing.
     */
    public static Document buildDocument( String filename ) throws IOException, JDOMException {
        return buildDocument( SAXBuilderHelper.getParserClassName(), filename, DEFAULT_VALIDATION );
    }


    /**
     * <p>
     * Creates an instance of the JDOM Document for the specified file.
     * </p>
     * @param file <code>File</code> .
     * @param the document instance
     * @throws IOException if the file does not exist.
     * @throws JDOMException when errors occur in parsing.
     */
    public static Document buildDocument(File file ) throws IOException, JDOMException {
        return buildDocument( SAXBuilderHelper.getParserClassName(), file, DEFAULT_VALIDATION );
    }
    
    /**
     * <p>
     * Creates an instance of the JDOM Document for the specified file.
     * </p>
     * @param filename <code>String</code> name of file.
     * @param validateXML indicates whether to validate the XML document
     *   using the DTD referenced in the XML file.
     * @param the document instance
     * @throws IOException if the file does not exist.
     * @throws JDOMException when errors occur in parsing.
     */
    public static Document buildDocument( String filename, boolean validateXML ) throws IOException, JDOMException {
        return buildDocument( SAXBuilderHelper.getParserClassName(), filename, validateXML );
    }

    /**
     * <p>
     * Creates an instance of the JDOM Document for the specified file.
     * </p>
     * @param saxDriverClass <code>String</code> name of driver class to use.
     * @param filename <code>String</code> name of file.
     * @param validateXML indicates whether to validate the XML document
     *   using the DTD referenced in the XML file.
     * @param the document instance
     * @throws IOException if the file does not exist.
     * @throws JDOMException when errors occur in parsing.
     */
    public static Document buildDocument( String saxDriverClass, String filename, boolean validateXML) throws IOException, JDOMException {
        if(filename == null){
            ArgCheck.isNotNull(filename,CorePlugin.Util.getString("JdomHelper.The_file_name_may_not_be_null_3")); //$NON-NLS-1$
        }
        
        if(filename.length() == 0){
            ArgCheck.isNotZeroLength(filename,CorePlugin.Util.getString("JdomHelper.The_file_name_may_not_be_zero-length_4")); //$NON-NLS-1$
        }
        
        SAXBuilder builder = SAXBuilderHelper.createSAXBuilder( saxDriverClass, validateXML );
        return builder.build( new File( filename ) );
    }
    
    

    /**
     * <p>
     * Creates an instance of the JDOM Document for the specified file.
     * </p>
     * @param saxDriverClass <code>String</code> name of driver class to use.
     * @param file <code>File</code>.
     * @param validateXML indicates whether to validate the XML document
     *   using the DTD referenced in the XML file.
     * @param the document instance
     * @throws IOException if the file does not exist.
     * @throws JDOMException when errors occur in parsing.
     */
    public static Document buildDocument( String saxDriverClass, File file, boolean validateXML) throws IOException, JDOMException {
        SAXBuilder builder = SAXBuilderHelper.createSAXBuilder( saxDriverClass, validateXML );
        return builder.build(file);
    }

    /**
     * <p>
     * Creates an instance of the JDOM Document for the specified stream.
     * </p>
     * @param stream the input stream from which the document is to be read
     * @param the document instance
     * @throws IOException if the file does not exist.
     * @throws JDOMException when errors occur in parsing.
     */
    public static Document buildDocument( InputStream stream) throws IOException, JDOMException {
        return buildDocument( SAXBuilderHelper.getParserClassName(), stream, DEFAULT_VALIDATION );
    }

    /**
     * <p>
     * Creates an instance of the JDOM Document for the specified stream.
     * </p>
     * @param stream the input stream from which the document is to be read
     * @param validateXML indicates whether to validate the XML document
     *   using the DTD referenced in the XML file.
     * @param the document instance
     * @throws IOException if the file does not exist.
     * @throws JDOMException when errors occur in parsing.
     */
    public static Document buildDocument( InputStream stream, boolean validateXML) throws IOException, JDOMException {
        return buildDocument( SAXBuilderHelper.getParserClassName(), stream, validateXML );
    }

    /**
     * <p>
     * Creates an instance of the JDOM Document for the specified stream.
     * </p>
     * @param saxDriverClass <code>String</code> name of driver class to use.
     * @param stream the input stream from which the document is to be read
     * @param validateXML indicates whether to validate the XML document
     *   using the DTD referenced in the XML file.
     * @param the document instance
     * @throws IOException if the file does not exist.
     * @throws JDOMException when errors occur in parsing.
     */
    public static Document buildDocument( String saxDriverClass, InputStream stream, boolean validateXML) throws IOException, JDOMException {
        if(stream == null){
            ArgCheck.isNotNull(stream,CorePlugin.Util.getString("JdomHelper.The_InputStream_may_not_be_null_7")); //$NON-NLS-1$
        }
        
        SAXBuilder builder = SAXBuilderHelper.createSAXBuilder( saxDriverClass, validateXML );
        return builder.build( stream );
    }

    /**
     * <p>
     * Creates an instance of the JDOM Document for the specified stream.
     * </p>
     * @param stream the Reader from which the document is to be read
     * @param the document instance
     * @throws IOException if the file does not exist.
     * @throws JDOMException when errors occur in parsing.
     */
    public static Document buildDocument( java.io.Reader stream) throws IOException, JDOMException {
        return buildDocument( SAXBuilderHelper.getParserClassName(), stream, DEFAULT_VALIDATION );
    }

    /**
     * <p>
     * Creates an instance of the JDOM Document for the specified stream.
     * </p>
     * @param stream the Reader from which the document is to be read
     * @param validateXML indicates whether to validate the XML document
     *   using the DTD referenced in the XML file.
     * @param the document instance
     * @throws IOException if the file does not exist.
     * @throws JDOMException when errors occur in parsing.
     */
    public static Document buildDocument( java.io.Reader stream, boolean validateXML) throws IOException, JDOMException {
        return buildDocument( SAXBuilderHelper.getParserClassName(), stream, validateXML );
    }

    /**
     * <p>
     * Creates an instance of the JDOM Document for the specified stream.
     * </p>
     * @param saxDriverClass <code>String</code> name of driver class to use.
     * @param stream the Reader from which the document is to be read
     * @param validateXML indicates whether to validate the XML document
     *   using the DTD referenced in the XML file.
     * @param the document instance
     * @throws IOException if the file does not exist.
     * @throws JDOMException when errors occur in parsing.
     */
    public static Document buildDocument( String saxDriverClass, java.io.Reader stream, boolean validateXML) throws IOException, JDOMException {
        if(stream == null){
            ArgCheck.isNotNull(stream,CorePlugin.Util.getString("JdomHelper.The_InputStream_may_not_be_null_10")); //$NON-NLS-1$
        }
        
        SAXBuilder builder = SAXBuilderHelper.createSAXBuilder( saxDriverClass, validateXML );
        return builder.build( stream );
    }

    /**
     * <p>
     * Create a new JDOM Docment with a root Element of the specified name.
     * @param rootTag the name of the root tag for the new document.
     * @param the new document instance
     * </p>
     */
    public static Document createNewDocument( String rootTag ) {
        if(rootTag == null){
            ArgCheck.isNotNull(rootTag,CorePlugin.Util.getString("JdomHelper.The_root_tag_name_may_not_be_null_11")); //$NON-NLS-1$
        }
        
        if(rootTag.length() == 0){
            ArgCheck.isNotZeroLength(rootTag,CorePlugin.Util.getString("JdomHelper.The_root_tag_name_may_not_be_zero-length_12")); //$NON-NLS-1$
        }
        
        Element root = new Element( rootTag );
        return new Document( root );
    }

    /**
     * <p>
     * This returns a List of Element objects that are descendents of this element
     * and within the specified Namespace. If this target element has no nested
     * elements within this namespace, an empty List is returned.
     * The returned list is "live" and changes to it affect the element's actual
     * contents.
     * </p>
     * @param traversalMethod the method used to traverse the JDOM tree.
     * @param parent the JDOM <code>Element</code> to get descendents for.
     * @param ns the Namespace to match
     * @return list of descendent JDOM <code>Element</code>s or an empty list
     *    if none exist.
     */
    public static List getDescendents( int traversalMethod, Element parent, final Namespace ns ) {
        if(parent == null){
            ArgCheck.isNotNull(parent,CorePlugin.Util.getString("JdomHelper.The_JDOM_Element_reference_may_not_be_null_13")); //$NON-NLS-1$
        }
        if(ns == null){
            ArgCheck.isNotNull(ns,CorePlugin.Util.getString("JdomHelper.The_Namespace_reference_may_not_be_null_14")); //$NON-NLS-1$
        }

        final List results = new LinkedList();
        switch( traversalMethod ) {
            case PRE_ORDER_TRAVERSAL :
                preOrderTraversal( parent,
                    new XMLVisitor() {
                        public void visit( Object obj ) {
                            if( ((Element)obj).getNamespace().equals(ns) ) {
                                results.add( obj );
                            }
                        }
                    }
                );
                break;
            case POST_ORDER_TRAVERSAL :
                postOrderTraversal( parent,
                    new XMLVisitor() {
                        public void visit( Object obj ) {
                            if( ((Element)obj).getNamespace().equals(ns) ) {
                                results.add( obj );
                            }
                        }
                    }
                );
                break;
            case LEVEL_ORDER_TRAVERSAL :
                levelOrderTraversal( parent,
                    new XMLVisitor() {
                        public void visit( Object obj ) {
                            if( ((Element)obj).getNamespace().equals(ns) ) {
                                results.add( obj );
                            }
                        }
                    }
                );
                break;
        }

        return results;
    }

    /**
     * <p>
     * This returns a List of Element objects that are descendents of this element.
     * If this target element has no nested elements, an empty List is returned.
     * The returned list is "live" and changes to it affect the element's actual
     * contents.
     * </p>
     * @param parent the JDOM <code>Element</code> to get descendents for.
     * @return list of descendent JDOM <code>Element</code>s or an empty list
     *    if none exist.
     */
    public static List getDescendents( Element parent ) {
        if(parent == null){
            ArgCheck.isNotNull(parent,CorePlugin.Util.getString("JdomHelper.The_JDOM_Element_reference_may_not_be_null_15")); //$NON-NLS-1$
        }

        final List results = new LinkedList();
        levelOrderTraversal( parent,
            new XMLVisitor() {
                public void visit( Object obj ) {
                    results.add( obj );
                }
            }
        );
        return results;
    }

    /**
     * <p>
     * This returns a List of all elements that are descendents of this element
     * with the given local name, returned as Element objects. If this target
     * element has no nested elements with the given name, an empty List is
     * returned. The returned list is "live" and changes to it affect the
     * element's actual contents.
     * </p>
     * @param parent the JDOM <code>Element</code> to get descendents for.
     * @param name tag name for the descendents to match
     * @return list of descendent JDOM <code>Element</code>s or an empty list
     *    if none exist.
     */
    public static List getDescendents( Element parent, final String name ) {
        if(parent == null){
            ArgCheck.isNotNull(parent,CorePlugin.Util.getString("JdomHelper.The_JDOM_Element_reference_may_not_be_null_16")); //$NON-NLS-1$
        }
        
        if(name == null){
            ArgCheck.isNotNull(name,CorePlugin.Util.getString("JdomHelper.The_name_may_not_be_null_17")); //$NON-NLS-1$
        }
        
        if(name.length() == 0){
            ArgCheck.isNotZeroLength(name,CorePlugin.Util.getString("JdomHelper.The_name_may_not_be_zero-length_18")); //$NON-NLS-1$
        }

        final List results = new LinkedList();
        levelOrderTraversal( parent,
            new XMLVisitor() {
                public void visit( Object obj ) {
                    if( ((Element)obj).getName().equals(name) ) {
                        results.add( obj );
                    }
                }
            }
        );
        return results;
    }
    /**
     * <p>
     * This returns a List of all elements that are descendents of this element
     * belonging to the specified Namespace, returned as Element objects. If this
     * target element has no nested elements within this Namespace, an empty List
     * is returned. The returned list is "live" and changes to it affect the
     * element's actual contents.
     * </p>
     * @param parent the JDOM <code>Element</code> to get descendents for.
     * @param ns the Namespace to match
     * @return list of descendent JDOM <code>Element</code>s or an empty list
     *    if none exist.
     */
    public static List getDescendents( Element parent, final Namespace ns ) {
        if(parent == null){
            ArgCheck.isNotNull(parent,CorePlugin.Util.getString("JdomHelper.The_JDOM_Element_reference_may_not_be_null_19")); //$NON-NLS-1$
        }
        if(ns == null){
            ArgCheck.isNotNull(ns,CorePlugin.Util.getString("JdomHelper.The_Namespace_may_not_be_null_20")); //$NON-NLS-1$
        }

        final List results = new LinkedList();
        levelOrderTraversal( parent,
            new XMLVisitor() {
                public void visit( Object obj ) {
                    if( ((Element)obj).getNamespace().equals(ns) ) {
                        results.add( obj );
                    }
                }
            }
        );
        return results;
    }

    /**
     * <p>
     * This returns a List of all elements that are descendents of this element
     * with the given local name and belonging to the specified Namespace, returned
     * as Element objects. If this target element has no nested elements with the
     * given name in the specified Namespace, an empty List is returned. The returned list
     * is "live" and changes to it affect the element's actual contents.
     * </p>
     * @param parent the JDOM <code>Element</code> to get descendents for.
     * @param name tag name for the descendents to match
     * @param ns the Namespace to match
     * @return list of descendent JDOM <code>Element</code>s or an empty list
     *    if none exist.
     */
    public static List getDescendents( Element parent, final String name, final Namespace ns ) {
        if(parent == null){
            ArgCheck.isNotNull(parent,CorePlugin.Util.getString("JdomHelper.The_JDOM_Element_reference_may_not_be_null_21")); //$NON-NLS-1$
        }
        
        if(name == null){
            ArgCheck.isNotNull(name,CorePlugin.Util.getString("JdomHelper.The_name_may_not_be_null_22")); //$NON-NLS-1$
        }
        
        if(name.length() == 0){
            ArgCheck.isNotZeroLength(name,CorePlugin.Util.getString("JdomHelper.The_name_may_not_be_zero-length_23")); //$NON-NLS-1$
        }

        final List results = new LinkedList();
        levelOrderTraversal( parent,
            new XMLVisitor() {
                public void visit( Object obj ) {
                    if( ((Element)obj).getName().equals(name) && ((Element)obj).getNamespace().equals(ns) ) {
                        results.add( obj );
                    }
                }
            }
        );
        return results;
    }

    /**
     * <p>
     * This returns the number of elements that are descendents of this element.
     * The resulting count excludes the specified parent Element.
     * </p>
     * @param parent the JDOM <code>Element</code> to use as the root for a count.
     * @return the number of JDOM <code>Element</code>s nested within the parent
     */
    public static int getDescendentCount( Element parent ) {
        if(parent == null){
            ArgCheck.isNotNull(parent,CorePlugin.Util.getString("JdomHelper.The_JDOM_Element_reference_may_not_be_null_24")); //$NON-NLS-1$
        }

        XMLVisitor visitor = new XMLVisitor() {
            public void visit( Object obj ) {
                count++;
            }
        };
        levelOrderTraversal( parent, visitor);
        return visitor.count;
    }

    /**
     * <p>
     * This returns the number of elements that are descendents of this element
     * and belonging to the specified Namespace. The resulting count excludes the
     * specified parent Element.
     * </p>
     * @param parent the JDOM <code>Element</code> to use as the root for a count.
     * @param ns the Namespace to match
     * @return the number of JDOM <code>Element</code>s nested within the parent
     */
    public static int getDescendentCount( Element parent, final Namespace ns ) {
        if(parent == null){
            ArgCheck.isNotNull(parent,CorePlugin.Util.getString("JdomHelper.The_JDOM_Element_reference_may_not_be_null_25")); //$NON-NLS-1$
        }

        XMLVisitor visitor = new XMLVisitor() {
            public void visit( Object obj ) {
                if( ((Element)obj).getNamespace().equals(ns) ) {
                    count++;
                }
            }
        }; 
        levelOrderTraversal( parent, visitor);
        return visitor.count;
    }

    /**
     * <p>
     * This returns the number of elements that are descendents of this element
     * with the given local name. The resulting count excludes the specified parent Element.
     * </p>
     * @param parent the JDOM <code>Element</code> to use as the root for a count.
     * @param name tag name for the descendents to match
     * @return the number of JDOM <code>Element</code>s nested within the parent
     */
    public static int getDescendentCount( Element parent, final String name ) {
        if(parent == null){
            ArgCheck.isNotNull(parent,CorePlugin.Util.getString("JdomHelper.The_JDOM_Element_reference_may_not_be_null_26")); //$NON-NLS-1$
        }
        
        if(name == null){
            ArgCheck.isNotNull(name,CorePlugin.Util.getString("JdomHelper.The_name_may_not_be_null_27")); //$NON-NLS-1$
        }
        
        if(name.length() == 0){
            ArgCheck.isNotZeroLength(name,CorePlugin.Util.getString("JdomHelper.The_name_may_not_be_zero-length_28")); //$NON-NLS-1$
        }

        XMLVisitor visitor = new XMLVisitor() {
            public void visit( Object obj ) {
                if( ((Element)obj).getName().equals(name)) {
                    count++;
                }
            }
        };

        levelOrderTraversal( parent, visitor);
        return visitor.count;
    }

    /**
     * <p>
     * This returns the first element that is a descendent of this element
     * and matches given local name.
     * </p>
     * @param parent the JDOM <code>Element</code> to check descendents for.
     * @param name tag name for the descendents to match
     * @return first occurrence of a descendent JDOM <code>Element</code>s that
     *    matches the specified name or null if no such element exists.
     */
    public static Element findElement( Element parent, final String name ) {
        if(parent == null){
            ArgCheck.isNotNull(parent,CorePlugin.Util.getString("JdomHelper.The_JDOM_Element_reference_may_not_be_null_29")); //$NON-NLS-1$
        }
        
        if(name == null){
            ArgCheck.isNotNull(name,CorePlugin.Util.getString("JdomHelper.The_name_may_not_be_null_30")); //$NON-NLS-1$
        }
        
        if(name.length() == 0){
            ArgCheck.isNotZeroLength(name,CorePlugin.Util.getString("JdomHelper.The_name_may_not_be_zero-length_31")); //$NON-NLS-1$
        }

        XMLVisitor visitor = new XMLVisitor() {
            public void visit( Object obj ) {
                if( result != null )
                    return;
                if( ((Element)obj).getName().equals(name) ) {
                    result = (Element)obj;
                }
            }
        };
        levelOrderTraversal( parent, visitor);
        return visitor.result;
    }

    /**
     * <p>
     * This returns the first element that is a descendent of this element
     * and matches the specified local name and Namespace.
     * </p>
     * @param parent the JDOM <code>Element</code> to check descendents for.
     * @param name tag name for the descendents to match
     * @param ns the Namespace to match
     * @return first occurrence of a descendent JDOM <code>Element</code>s that
     *    matches the specified name or null if no such element exists.
     */
    public static Element findElement( Element parent, final String name, final Namespace ns ) {
        if(parent == null){
            ArgCheck.isNotNull(parent,CorePlugin.Util.getString("JdomHelper.The_JDOM_Element_reference_may_not_be_null_32")); //$NON-NLS-1$
        }
        
        if(name == null){
            ArgCheck.isNotNull(name,CorePlugin.Util.getString("JdomHelper.The_name_may_not_be_null_33")); //$NON-NLS-1$
        }
        
        if(name.length() == 0){
            ArgCheck.isNotZeroLength(name,CorePlugin.Util.getString("JdomHelper.The_name_may_not_be_zero-length_34")); //$NON-NLS-1$
        }
        
        if(ns == null){
            ArgCheck.isNotNull(ns,CorePlugin.Util.getString("JdomHelper.The_Namespace_reference_may_not_be_null_35")); //$NON-NLS-1$
        }

        XMLVisitor visitor = new XMLVisitor() {
            public void visit( Object obj ) {
                if( result != null )
                    return;
                if( ((Element)obj).getName().equals(name) && ((Element)obj).getNamespace().equals(ns) ) {
                    result = (Element)obj;
                }
            }
        }; 
        levelOrderTraversal( parent, visitor);
        return visitor.result;
    }

    /**
     * <p>
     * Implements a level order traversal of the JDOM tree in which all nodes of
     * the tree are recursively visited by depth:  first children of the parent,
     * then grandchildren of the parent (children of children) and so on. The
     * visit method of the specified <code>XMLVisitor</code> is invoked on each
     * node during the traversal. This is equivalent to a <i>breadth first search</i>.
     * </p>
     * @param parent the JDOM <code>Element</code> to use as the root for the
     *    traversal.
     * @param v the <code>XMLVisitor</code> to apply at each node as the
     *    tree is traversed.
     */
    public static void levelOrderTraversal( Element parent, XMLVisitor v ) {
        if(parent == null){
            ArgCheck.isNotNull(parent,CorePlugin.Util.getString("JdomHelper.The_JDOM_Element_reference_may_not_be_null_36")); //$NON-NLS-1$
        }
        if(v == null){
            ArgCheck.isNotNull(v,CorePlugin.Util.getString("JdomHelper.The_XMLVisitor_reference_may_not_be_null_37")); //$NON-NLS-1$
        }

        LinkedList queue = new LinkedList( parent.getChildren() );
        while( queue.size() != 0 ) {
            Element child = (Element)queue.getFirst();
            v.visit( child );
            queue.addAll( child.getChildren() );
            queue.removeFirst();
        }
    }

    /**
     * <p>
     * Implements a pre-order traversal of the JDOM tree in which a parent node
     * is processed first then an subtrees.  The visit method of the specified
     * <code>XMLVisitor</code> is invoked on each node during the traversal.
     * </p>
     * @param parent the JDOM <code>Element</code> to use as the root for the
     *    traversal.
     * @param v the <code>XMLVisitor</code> to apply at each node as the
     *    tree is traversed.
     */
    public static void preOrderTraversal( Element parent, XMLVisitor v ) {
        if(parent == null){
            ArgCheck.isNotNull(parent,CorePlugin.Util.getString("JdomHelper.The_JDOM_Element_reference_may_not_be_null_38")); //$NON-NLS-1$
        }
        if(v == null){
            ArgCheck.isNotNull(v,CorePlugin.Util.getString("JdomHelper.The_XMLVisitor_reference_may_not_be_null_39")); //$NON-NLS-1$
        }

        List children = parent.getChildren();
        for (int i=0, len=children.size(); i<len; i++) {
            Element child = (Element)children.get( i );
            v.visit( child );
            preOrderTraversal( child, v );
        }
    }

    /**
     * <p>
     * Implements a post-order traversal of the JDOM tree in which any subtrees
     * are processed first then parent  The visit method of the specified
     * <code>XMLVisitor</code> is invoked on each node during the traversal.
     * </p>
     * @param parent the JDOM <code>Element</code> to use as the root for the
     *    traversal.
     * @param v the <code>XMLVisitor</code> to apply at each node as the
     *    tree is traversed.
     */
    public static void postOrderTraversal( Element parent, XMLVisitor v ) {
        if(parent == null){
            ArgCheck.isNotNull(parent,CorePlugin.Util.getString("JdomHelper.The_JDOM_Element_reference_may_not_be_null_40")); //$NON-NLS-1$
        }
        if(v == null){
            ArgCheck.isNotNull(v,CorePlugin.Util.getString("JdomHelper.The_XMLVisitor_reference_may_not_be_null_41")); //$NON-NLS-1$
        }

        List children = parent.getChildren();
        for (int i=0, len=children.size(); i<len; i++) {
            Element child = (Element)children.get( i );
            postOrderTraversal( child, v );
            v.visit( child );
        }
    }

    /**
     * <p>
     * Ouput this JDOM <code>Document</code> to a file with the specified name.
     * </p>
     * @param doc the document to be output
     * @param filename the fully qualified name of the file to output the XML to.
     * @throws IOException if there are problems writing to the file.
     */
    public static void write( Document doc , String filename ) throws IOException {
        JdomHelper.write(doc, filename, DEFAULT_INDENT, true);
    }

    /**
     * <p>
     * Ouput the current JDOM <code>Document</code> to a file with the specified name.
     * </p>
     * @param doc the document to be output
     * @param filename the fully qualified name of the file to output the XML to.
     * @param indent the indent String, usually some number of spaces
     * @param newlines true indicates new lines should be printed, else new
     * lines are ignored (compacted).
     * @throws IOException if there are problems writing to the file.
     */
    public static void write( Document doc , String filename, String indent, boolean newlines ) throws IOException {
        ArgCheck.isNotNull(doc,CorePlugin.Util.getString("JdomHelper.The_Document_reference_may_not_be_null_42")); //$NON-NLS-1$
        ArgCheck.isNotNull(filename,CorePlugin.Util.getString("JdomHelper.The_filename_may_not_be_null_43")); //$NON-NLS-1$
        
        if(filename.length() == 0){
            ArgCheck.isNotZeroLength(filename,CorePlugin.Util.getString("JdomHelper.The_filename_may_not_be_zero-length_44")); //$NON-NLS-1$
        }
        
        FileOutputStream out = new FileOutputStream( filename );
        try {
	        write(doc,out,indent,newlines);
	        out.flush();
        } finally {
        	out.close();
        }
    }

    /**
     * <p>
     * Ouput the current JDOM <code>Document</code> to the output stream.
     * </p>
     * @param doc the document to be output
     * @param stream the output stream
     * @throws IOException if there are problems writing to the file.
     */
    public static void write( Document doc , OutputStream stream ) throws IOException {
        write(doc,stream,DEFAULT_INDENT, true);
    }

//    /**
//     * <p>
//     * Ouput the current JDOM <code>Document</code> to the output stream.
//     * The root Element for the specified Document instance will be modified
//     * as a result of this operation.
//     * </p>
//     * @param doc the document to be output
//     * @param stream the output stream
//     * @param indent the indent String, usually some number of spaces
//     * @param newlines true indicates new lines should be printed, else new
//     * lines are ignored (compacted).
//     * @param schemaURI the uniform resource identifier of the XML Schema 
//     * to use for validation
//     * @throws IOException if there are problems writing to the file.
//     */
//    public static void write( Document doc, OutputStream stream, String indent, 
//                              boolean newlines, String schemaURI) throws IOException {
//        ArgCheck.isNotNull(doc,"The Document reference may not be null");
//        ArgCheck.isNotNull(stream,"The OutputStream reference may not be null");
//        XMLOutputter outputter = new XMLOutputter( indent, newlines);
//        Element root = doc.getRootElement();
//        root.addAttribute(new Attribute("noNamespaceSchemaLocation",schemaURI,
//                          Namespace.getNamespace(SCHEMA_NAMESPACE_PREFIX,SCHEMA_NAMESPACE)));
//        outputter.output( doc, stream );
//    }

    /**
     * <p>
     * Ouput the current JDOM <code>Document</code> to the output stream.
     * </p>
     * @param doc the document to be output
     * @param stream the output stream
     * @param indent the indent String, usually some number of spaces
     * @param newlines true indicates new lines should be printed, else new
     * lines are ignored (compacted).
     * @throws IOException if there are problems writing to the file.
     */
    public static void write( Document doc , OutputStream stream, String indent, boolean newlines ) throws IOException {
        if(doc == null){
            ArgCheck.isNotNull(doc,CorePlugin.Util.getString("JdomHelper.The_Document_reference_may_not_be_null_45")); //$NON-NLS-1$
        }
        if(stream == null){
            ArgCheck.isNotNull(stream,CorePlugin.Util.getString("JdomHelper.The_OutputStream_reference_may_not_be_null_46")); //$NON-NLS-1$
        }
        XMLOutputter outputter = new XMLOutputter(getFormat(indent, newlines));
        outputter.output( doc, stream );
    }
    
    /**
     * <p>
     * Ouput the current JDOM <code>Document</code> to the output stream.
     * </p>
     * @param doc the document to be output
     * @param writer the output writer
     * @throws IOException if there are problems writing to the file.
     */
    public static void write( Document doc , Writer writer ) throws IOException {
        write(doc,writer,DEFAULT_INDENT, true);
    }
    
    
    /**
     * <p>
     * Ouput the current JDOM <code>Document</code> to the writter.
     * </p>
     * @param doc the document to be output
     * @param writer the output writer
     * @param indent the indent String, usually some number of spaces
     * @param newlines true indicates new lines should be printed, else new
     * lines are ignored (compacted).
     * @throws IOException if there are problems writing to the file.
     */
    public static void write( Document doc , Writer writer, String indent, boolean newlines ) throws IOException {
        if(doc == null){
            ArgCheck.isNotNull(doc,CorePlugin.Util.getString("JdomHelper.The_Document_reference_may_not_be_null_47")); //$NON-NLS-1$
        }
        if(writer == null){
            ArgCheck.isNotNull(writer,CorePlugin.Util.getString("JdomHelper.The_Writer_reference_may_not_be_null_48")); //$NON-NLS-1$
        }
        XMLOutputter outputter = new XMLOutputter(getFormat(indent, newlines));
        outputter.output( doc, writer );
    }
    
    /**
     * <p>
     * Return the current JDOM <code>Document</code> as a String.
     * </p>
     * @param doc the document to be output
     * @return the document in string form
     * @throws IOException if there are problems writing to the file.
     */
    public static String write( Document doc) throws IOException {
        return write(doc, DEFAULT_INDENT, true);
    }
    
    /**
     * <p>
     * Ouput the current JDOM <code>Document</code> as a String.
     * </p>
     * @param doc the document to be output
     * @param indent the indent String, usually some number of spaces
     * @param newlines true indicates new lines should be printed, else new
     * lines are ignored (compacted).
     * @return the document in string form
     * @throws IOException if there are problems writing to the file.
     */
    public static String write( Document doc , String indent, boolean newlines ) throws IOException {
        if(doc == null){
            ArgCheck.isNotNull(doc,CorePlugin.Util.getString("JdomHelper.The_Document_reference_may_not_be_null_49")); //$NON-NLS-1$
        }
        XMLOutputter outputter = new XMLOutputter(getFormat(indent, newlines));
        StringWriter writer = new StringWriter();
        outputter.output( doc, writer );
        
        return writer.getBuffer().toString();
    }

    public static void print( PrintStream stream, Document doc ) {
        if(stream == null){
            ArgCheck.isNotNull(stream,CorePlugin.Util.getString("JdomHelper.The_stream_reference_may_not_be_null_50")); //$NON-NLS-1$
        }
        stream.println(CorePlugin.Util.getString("JdomHelper.JDOM_Document_tree_51")); //$NON-NLS-1$
        Element root = doc.getRootElement();
        print(root,stream,CorePlugin.Util.getString("JdomHelper.___52")); //$NON-NLS-1$
    }

    private static void print( Element elm, PrintStream stream, String leadingString ) {
        stream.println(leadingString + elm.getName() );
        Iterator itr = elm.getChildren().iterator();
        while ( itr.hasNext() ) {
            print((Element)itr.next(),stream,leadingString + CorePlugin.Util.getString("JdomHelper.___53")); //$NON-NLS-1$
        }
    }
    
    /**
     * Get the best content value for a JDOM object.  For elements, the content text is returned.  
     * For attributes, the attribute value is returned.  For namespaces, the URI is returned.  Etc...    
     * @param jdomObject JDOM object such as Element, Attribute, Text, Namespace, Comment, ProcessingInstruction, String
     * @return Content value for the specified JDOM object
     * @since 4.2
     */
    public static String getContentValue( Object jdomObject ) {
        if(jdomObject == null) {
            return null; 
        } else if(jdomObject instanceof String) {
            return (String)jdomObject;
        } else if(jdomObject instanceof Element) {
            return ((Element)jdomObject).getText();
        } else if(jdomObject instanceof Attribute) {
            return ((Attribute)jdomObject).getValue();
        } else if(jdomObject instanceof Text) {
            return ((Text)jdomObject).getValue();
        } else if(jdomObject instanceof Namespace) {
            return ((Namespace)jdomObject).getURI();
        } else if(jdomObject instanceof Comment) {
            return ((Comment)jdomObject).getText();
        } else if(jdomObject instanceof ProcessingInstruction) {
            return ((ProcessingInstruction)jdomObject).getData();
        }
        
        // Default
        return jdomObject.toString();        
    }
    
    public static Format getFormat(String indent, boolean newlines) {
        Format format = Format.getPrettyFormat();
        format.setIndent(indent);
        if (newlines) {
            format.setLineSeparator("\n"); //$NON-NLS-1$
        } else {
            format.setLineSeparator(""); //$NON-NLS-1$
        }
        return format;
    }

} //end class

abstract class XMLVisitor {
    /** Single <code>Element</code> resulting from a search of the JDOM tree */
    Element result = null;

    /** Number of <code>Element</code> resulting from a search of the JDOM tree */
    int count = 0;

    abstract void visit( Object obj);
    
}


