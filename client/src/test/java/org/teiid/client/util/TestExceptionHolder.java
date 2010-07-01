package org.teiid.client.util;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.SQLException;
import java.util.ArrayList;

import org.junit.Test;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.core.util.UnitTestUtil;


public class TestExceptionHolder {
	
	@SuppressWarnings("all")
	public static class BadException extends TeiidProcessingException {
		private Object obj;
		public BadException(String msg) {super(msg);}
		public BadException(Object obj) {
			this.obj = obj;
		}
	}
	
	@Test public void testDeserializationUnknownException() throws Exception {
		ClassLoader cl = new URLClassLoader(new URL[] {UnitTestUtil.getTestDataFile("test.jar").toURI().toURL()}); //$NON-NLS-1$
		Object obj = ReflectionHelper.create("test.Test", null, cl); //$NON-NLS-1$
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(new ExceptionHolder(new BadException(obj)));
        oos.flush();
        
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        ExceptionHolder holder = (ExceptionHolder)ois.readObject();
        assertTrue(holder.getException() instanceof BadException);
        assertEquals("Remote org.teiid.client.util.TestExceptionHolder$BadException: null", holder.getException().getMessage()); //$NON-NLS-1$
	}


	@SuppressWarnings("all")
	public static class BadException2 extends TeiidProcessingException {
		public BadException2(String msg) {
			super(msg);
		}
		public BadException2(Throwable e, String msg) {
			super(e, msg);
		}
	}
	
	@Test public void testDeserializationUnknownChildException() throws Exception {
		ClassLoader cl = new URLClassLoader(new URL[] {UnitTestUtil.getTestDataFile("test.jar").toURI().toURL()}); //$NON-NLS-1$
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
        assertEquals("Remote org.teiid.client.util.TestExceptionHolder$BadException2: I have foreign exception embedded in me", e.getMessage()); //$NON-NLS-1$
        
        // now unknown exception is not found, so promote known SQL exception up
        e = e.getCause();
        assertTrue(e instanceof SQLException);
        assertEquals("Remote java.sql.SQLException: something bad happended", e.getMessage()); //$NON-NLS-1$
	}	
	
	@Test public void testDeserializationUnknownChildException2() throws Exception {
		ClassLoader cl = new URLClassLoader(new URL[] {UnitTestUtil.getTestDataFile("test.jar").toURI().toURL()}); //$NON-NLS-1$
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
        assertTrue(e instanceof TeiidRuntimeException);
        assertEquals("Unknown Exception", e.getMessage()); //$NON-NLS-1$
	}	
	
	private static class NotSerializable {
		
	}

	@Test public void testDeserializationNotSerializable() throws Exception {
		Exception ex = new TeiidException() {
			NotSerializable ns = new NotSerializable();
		};
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(new ExceptionHolder(ex));
        oos.flush();
        
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        ExceptionHolder holder = (ExceptionHolder)ois.readObject();
        Throwable e = holder.getException();
        assertTrue(e instanceof TeiidException);
	}
}
