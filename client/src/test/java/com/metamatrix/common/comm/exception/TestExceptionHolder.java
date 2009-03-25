package com.metamatrix.common.comm.exception;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URL;

import org.junit.Test;

import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.classloader.NonDelegatingClassLoader;
import com.metamatrix.core.util.ReflectionHelper;
import com.metamatrix.core.util.UnitTestUtil;

public class TestExceptionHolder {
		
	@SuppressWarnings("all")
	public static class BadException extends MetaMatrixProcessingException {
		private Object obj;
		public BadException(Object obj) {
			this.obj = obj;
		}
	}
	
	@Test public void testDeserializationUnknownException() throws Exception {
		ClassLoader cl = new NonDelegatingClassLoader(new URL[] {UnitTestUtil.getTestDataFile("test.jar").toURI().toURL()}); //$NON-NLS-1$
		Object obj = ReflectionHelper.create("Test", null, cl); //$NON-NLS-1$
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(new ExceptionHolder(new BadException(obj)));
        oos.flush();
        
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        ExceptionHolder holder = (ExceptionHolder)ois.readObject();
        assertTrue(holder.getException() instanceof MetaMatrixProcessingException);
        assertEquals("Remote exception: null ... Original type hierarchy [com.metamatrix.common.comm.exception.TestExceptionHolder$BadException, com.metamatrix.api.exception.MetaMatrixProcessingException, com.metamatrix.api.exception.MetaMatrixException, com.metamatrix.core.MetaMatrixCoreException].", holder.getException().getMessage()); //$NON-NLS-1$
	}

}
