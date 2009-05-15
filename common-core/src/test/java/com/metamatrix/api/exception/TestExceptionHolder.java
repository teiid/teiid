package com.metamatrix.api.exception;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;

import junit.framework.TestCase;

import org.junit.Test;

import com.metamatrix.common.classloader.PostDelegatingClassLoader;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.ReflectionHelper;
import com.metamatrix.core.util.UnitTestUtil;

public class TestExceptionHolder extends TestCase {
	
	//## JDBC4.0-begin ##		
	@SuppressWarnings("all")
	public static class BadException extends MetaMatrixProcessingException {
		private Object obj;
		public BadException(String msg) {super(msg);}
		public BadException(Object obj) {
			this.obj = obj;
		}
	}
	
	@Test public void testDeserializationUnknownException() throws Exception {
		ClassLoader cl = new PostDelegatingClassLoader(new URL[] {UnitTestUtil.getTestDataFile("test.jar").toURI().toURL()}); //$NON-NLS-1$
		Object obj = ReflectionHelper.create("test.Test", null, cl); //$NON-NLS-1$
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(new ExceptionHolder(new BadException(obj)));
        oos.flush();
        
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        ExceptionHolder holder = (ExceptionHolder)ois.readObject();
        assertTrue(holder.getException() instanceof BadException);
        assertEquals("Remote exception: null ... Original type hierarchy [com.metamatrix.api.exception.TestExceptionHolder$BadException, com.metamatrix.api.exception.MetaMatrixProcessingException, com.metamatrix.api.exception.MetaMatrixException, com.metamatrix.core.MetaMatrixCoreException].", holder.getException().getMessage()); //$NON-NLS-1$
	}


	@SuppressWarnings("all")
	public static class BadException2 extends MetaMatrixProcessingException {
		public BadException2(String msg) {
			super(msg);
		}
		public BadException2(Throwable e, String msg) {
			super(e, msg);
		}
	}
	
	@Test public void testDeserializationUnknownChildException() throws Exception {
		ClassLoader cl = new PostDelegatingClassLoader(new URL[] {UnitTestUtil.getTestDataFile("test.jar").toURI().toURL()}); //$NON-NLS-1$
		Exception obj = (Exception)ReflectionHelper.create("test.UnknownException", null, cl); //$NON-NLS-1$
		obj.initCause(new SQLException("something bad happended")); //$NON-NLS-1$
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(new ExceptionHolder(new BadException2(obj, "I have foreign exception embedded in me"))); //$NON-NLS-1$
        oos.flush();
        
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        ExceptionHolder holder = (ExceptionHolder)ois.readObject();
        Throwable e = holder.getException();
        assertTrue(e instanceof BadException2);
        assertEquals("Remote exception: I have foreign exception embedded in me ... Original type hierarchy [com.metamatrix.api.exception.TestExceptionHolder$BadException2, com.metamatrix.api.exception.MetaMatrixProcessingException, com.metamatrix.api.exception.MetaMatrixException, com.metamatrix.core.MetaMatrixCoreException].", e.getMessage()); //$NON-NLS-1$
        
        // now unknown exception is not found, so promote known SQL exception up
        e = e.getCause();
        assertTrue(e instanceof SQLException);
        assertEquals("Remote exception: something bad happended ... Original type hierarchy [java.sql.SQLException].", e.getMessage()); //$NON-NLS-1$
	}	
	
	@Test public void testDeserializationUnknownChildException2() throws Exception {
		ClassLoader cl = new PostDelegatingClassLoader(new URL[] {UnitTestUtil.getTestDataFile("test.jar").toURI().toURL()}); //$NON-NLS-1$
		ArrayList<String> args = new ArrayList<String>();
		args.add("Unknown Exception"); //$NON-NLS-1$
		Exception obj = (Exception)ReflectionHelper.create("test.UnknownException", args, cl); //$NON-NLS-1$ 
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(new ExceptionHolder(obj));
        oos.flush();
        
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        ExceptionHolder holder = (ExceptionHolder)ois.readObject();
        Throwable e = holder.getException();
        assertTrue(e instanceof MetaMatrixRuntimeException);
        assertEquals("Unknown Exception", e.getMessage()); //$NON-NLS-1$
	}		
	//## JDBC4.0-end ##
	
	/*## JDBC3.0-JDK1.5-begin ##
	public void testPass(){
	// since the jar files required are built with 1.6, it will always fail, so just comment the test for 1.5
	} 
	## JDBC3.0-JDK1.5-end ##*/
}
