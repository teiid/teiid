/*
 * Copyright (c) 2000-2005 MetaMatrix, Inc.  All rights reserved.
 */
package org.teiid.test.framework.exception;

import java.util.List;

import org.teiid.core.TeiidRuntimeException;


/**
 */

	/**
	 * The QueryTestFailedException is thrown during a test.
	 * 
	 * This exception which contains a reference to another exception.  This
	 * class can be used to maintain a linked list of exceptions. <p>
	 *
	 * Subclasses of this exception typically only need to implement whatever
	 * constructors they need. <p>
	 */
public class QueryTestFailedException extends Exception {
	    //############################################################################################################################
		//# Static Methods                                                                                                           #
		//############################################################################################################################
	    
	    /**
		 * @since
		 */
		private static final long serialVersionUID = -4824998416118801290L;

		/**
	     * Utility method to get the name of a class, without package information.
	     *
	     * @param cls The class to get the name of
	     * @return The name of the class, without package info
	     */
	    public static String getClassShortName( Class<?> cls ) {
	        if ( cls == null ) return ""; //$NON-NLS-1$
	        String className = cls.getName();
	        return className.substring( className.lastIndexOf('.')+1 );
	    }
	    
	    //############################################################################################################################
		//# Variables                                                                                                                #
		//############################################################################################################################
	    
	    /** An error code. */
	    private String code;

	    /** Exception chained to this one. */
	    private Throwable child;
	    
	    private String msg;

	    //############################################################################################################################
		//# Constructors                                                                                                             #
		//############################################################################################################################

	    /**
	     * Construct a default instance of this class.
	     */
	    public QueryTestFailedException() {
	    }

	    /**
	     * Construct an instance with the specified error message.  If the message is actually a key, the actual message will be
	     * retrieved from a resource bundle using the key, the specified parameters will be substituted for placeholders within the
	     * message, and the code will be set to the key.
	     * @param message The error message or a resource bundle key
	     */
	    public QueryTestFailedException(final String message) {
	        setMessage(message, null);
	    }

	    /**
	     * Construct an instance with the specified error code and message.  If the message is actually a key, the actual message will
	     * be retrieved from a resource bundle using the key, and the specified parameters will be substituted for placeholders within
	     * the message.
	     * @param code    The error code 
	     * @param message The error message or a resource bundle key
	     */
	    public QueryTestFailedException(final String code, final String message) {
	        setMessage(message, null);
	        // The following setCode call should be executed after setting the message 
	        setCode(code);
	    }

	    /**
	     * Construct an instance with a linked exception specified.  If the exception is a MetaMatrixException or a
	     * {@link TeiidRuntimeException}, then the code will be set to the exception's code.
	     * @param e An exception to chain to this exception
	     */
	    public QueryTestFailedException(final Throwable e) {
	        // This is nasty, but there's no setMessage() routine!
	        this.msg = ( e instanceof java.lang.reflect.InvocationTargetException )
	                   ? ((java.lang.reflect.InvocationTargetException)e).getTargetException().getMessage()
	                   : (e == null ? null : e.getMessage());
	        setChild( e );

	    }

	    /**
	     * Construct an instance with the linked exception and error message specified.  If the message is actually a key, the error
	     * message will be retrieved from a resource bundle the key, and code will be set to that key.  Otherwise, if the specified
	     * exception is a MetaMatrixException or a {@link TeiidRuntimeException}, the code will be set to the exception's code.
	     * @param e       The exception to chain to this exception
	     * @param message The error message or a resource bundle key
	     */
	    public QueryTestFailedException(final Throwable e, final String message) {
	      setChild( e );
	      // The following setMessage call should be executed after attempting to set the code from the passed-in exception 
	      setMessage(message, null);
	        
	    }


	    /**
	     * Construct an instance with the linked exception, error code, and error message specified.  If the message is actually a
	     * key, the error message will be retrieved from a resource bundle using the key.
	     * @param e       The exception to chain to this exception
	     * @param code    The error code 
	     * @param message The error message or a resource bundle key
	     */
	    public QueryTestFailedException(final Throwable e, final String code, final String message) {
	        setChild(e);
	        setMessage(message, null);
	        // The following setCode call should be executed after setting the message 
	        setCode(code);
	    }

	    //############################################################################################################################
		//# Methods                                                                                                                  #
		//############################################################################################################################

	    /**
	     * Get the exception which is linked to this exception.
	     *
	     * @return The linked exception
	     */
	    public Throwable getChild() {
	        return this.child;
	    }

	    /**
	     * Get the error code.
	     *
	     * @return The error code 
	     */
	    public String getCode() {
	        return this.code;
	    }

	    /**
	     * Returns the error message, formatted for output. <P>
	     *
	     * The default formatting provided by this method is to prepend the
	     * error message with the level and the name of the class, and to
	     * append the error code on the end if a non-zero code is defined. <P>
	     *
	     * This method provides a hook for subclasses to override the default
	     * formatting of any one exception.
	     *
	     * @param except The exception to print 
	     * @param level The depth of the exception in the chain of exceptions
	     * @return A formatted string for the exception
	     */
	    public String getFormattedMessage(final Throwable throwable, final int level) {

	        return ((level != 0) ? ("\n" + level + " ") : "" ) //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	               + "[" + getClassShortName(throwable.getClass()) + "]" //$NON-NLS-1$ //$NON-NLS-2$
	               + ((code != null) ? (' ' + code + ": ") : "")  //$NON-NLS-1$//$NON-NLS-2$
	               + (throwable.getMessage() == null ? "" : throwable.getMessage()); //$NON-NLS-1$
	    }

	    /* (non-Javadoc)
		 * @see java.lang.Throwable#getMessage()
		 */
		public String getMessage() {
			return this.msg;
		}

	    /**
	     * Set the exception which is linked to this exception. <P>
	     *
	     * For the special case of an InvocationTargetException, which is
	     * a wrapped exception provided in the <CODE>java.lang.reflect</CODE>
	     * package, the child is set to the wrapped exception, and the message,
	     * if not yet defined, is set to the message of the wrapped exception.
	     *
	     * @param child The linked exception
	     */
	    public void setChild( Throwable child ) {
	        // Special processing in case this is already a wrapped exception
	        if ( child instanceof java.lang.reflect.InvocationTargetException ) {
	            this.child = ((java.lang.reflect.InvocationTargetException)child).getTargetException();
	        } else {
	            this.child = child;
	        }
	    }

	    /**
	     * Set the error code.
	     *
	     * @param code The error code 
	     */
	    public void setCode( String code ) {
	        this.code = code;
	    }

	    /**
	     * Sets the exception message to the specified text.  If the message is actually a resource bundle key, the actual message
	     * will be retrieved using that key, the specified parameters will be substituted for placeholders within the message, and the
	     * code will be set to the key.
	     * @param message    The message text or a resource bundle message key.
	     * @param parameters The list of parameters to substitute for placeholders in the retrieved message.
	     * @since 3.1
	     */
	    private void setMessage(final String message, final List parameters) {
	        if (message == null) {
	            this.msg = message;
	        } else {
	            final String msg = message;
//	            final String msg = TextManager.INSTANCE.getText(message, parameters);
	            final int len = msg.length();
	            if (msg.startsWith("<")  &&  len >= 2) { //$NON-NLS-1$
	                this.msg = msg.substring(1, len - 1);
	            } else {
	                this.msg = msg;
	                setCode(message);
	            }
	        }
	    }
	    
	    /**
	     * Returns a string representation of this class.
	     *
	     * @return String representation of instance
	     */
	    public String toString() {
	        return this.getMessage();
	    }

	    // =========================================================================
	    //                         T E S T     M E T H O D S
	    // =========================================================================

	    /**
	     * Test program for this class.
	     *
	     * @args The command line arguments
	     */

	/*     
	    public static void main( String[] args ) {       
	        class TestAppException extends MetaMatrixException {
	            public TestAppException( Throwable e ) {
	                super( e );
	            }
	            public TestAppException( Throwable e, String message ) {
	                super( e, message );
	            }
	        } // END INNER CLASS
	        
	        System.out.println( "\n2 MetaMatrixExceptions and 1 Exception..." );
	        try {
	            throw new MetaMatrixException(
	                new MetaMatrixException(
	                    new Exception( "Non-chained exception" ),
	                    "Nested Error" ),
	                "My App Error" );
	        } catch ( MetaMatrixException e ) {
	            System.out.println( "Message is \n" + e.getMessage() );
	            System.out.println( "Full message is \n" + e.getFullMessage() );
	        }

	        System.out.println( "\n2 MetaMatrixExceptions..." );
	        try {
	            throw new MetaMatrixException(
	                new MetaMatrixException( 1001, "Nested Error" ),
	                "My App Error" );
	        } catch ( MetaMatrixException e ) {
	            System.out.println( "Message is \n" + e.getMessage() );
	            System.out.println( "Full message is \n" + e.getFullMessage() );
	        }

	        System.out.println( "\n2 MetaMatrixExceptions (1 a subclass)..." );
	        try {
	            throw new TestAppException(
	                new MetaMatrixException( 1001, "Nested Error" ),
	                "My App Error" );
	        } catch ( MetaMatrixException e ) {
	            System.out.println( "Message is \n" + e.getMessage() );
	            System.out.println( "Full message is \n" + e.getFullMessage() );
	        }

	        System.out.println( "\n2 exceptions (1 MetaMatrixException, 1 null)..." );
	        try {
	            throw new TestAppException( 
	                new MetaMatrixException( null, "Nested Error" ),
	                "My App Error" );
	        } catch ( MetaMatrixException e ) {
	            System.out.println( "Message is \n" + e.getMessage() );
	            System.out.println( "Full message is \n" + e.getFullMessage() );
	        }

	        System.out.println( "\n1 MetaMatrixException..." );
	        try {
	            throw new MetaMatrixException( 1002, "My App Error" );
	        } catch ( MetaMatrixException e ) {
	            System.out.println( "Message is \n" + e.getMessage() );
	            System.out.println( "Full message is \n" + e.getFullMessage() );
	        }

	        System.out.println( "\n5 MetaMatrixExceptions..." );
	        try {
	            MetaMatrixException root = new MetaMatrixException( 50, "Root" );
	            MetaMatrixException cur  = root;
	            MetaMatrixException next;
	            for ( int i = 1; i <= 5; i++ ) {
	                next = new MetaMatrixException( 50+i, "Nested exception " + i );
	                cur.setChild( next );
	                cur = next;
	            }
	            throw root;
	        } catch ( MetaMatrixException e ) {
	            System.out.println( "Message is \n" + e.getMessage() );
	            System.out.println( "Full message is \n" + e.getFullMessage() );
	        }

	        System.out.println( "\n1 MetaMatrixException (InvocationTargetException)..." );
	        try {
	            throw new TestAppException(
	                new java.lang.reflect.InvocationTargetException(
	                    new java.lang.IllegalArgumentException( "You can't do that!" ) ) );
	        } catch ( MetaMatrixException e ) {
	            System.out.println( "Message is \n" + e.getMessage() );
	            System.out.println( "Full message is \n" + e.getFullMessage() );
	        }
	    }
	*/    
	} // END CLASS
