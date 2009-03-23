package com.metamatrix.api.exception;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.sql.BatchUpdateException;
import java.sql.SQLException;

import javax.naming.OperationNotSupportedException;

import junit.framework.TestCase;

import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.util.UnitTestUtil;

public class TestMetaMatrixCoreException extends TestCase {
	
	public void testSQLExceptionUnrolling() throws Exception {
		SQLException se = new BatchUpdateException("foo", new int[] {1}); //$NON-NLS-1$
		MetaMatrixCoreException mmce = new MetaMatrixCoreException(se);
		
		mmce = UnitTestUtil.helpSerialize(mmce);
		assertEquals(SQLException.class, mmce.getCause().getClass());
		assertEquals("foo", mmce.getMessage()); //$NON-NLS-1$
		assertEquals("java.sql.BatchUpdateException: foo", mmce.getCause().getMessage()); //$NON-NLS-1$
	}
	
	public void testInitCause() throws Exception {
		MetaMatrixCoreException mmce = new MetaMatrixCoreException();
		mmce.initCause(new Exception());
		try {
			mmce.initCause(new Exception());
			fail();
		} catch (IllegalStateException e) {
			
		}
	}
	
	public void defer_testDeserializationUnknownException() throws Exception {
		MetaMatrixCoreException mmce = new MetaMatrixCoreException(new OperationNotSupportedException());
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(mmce);
        oos.flush();
        
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray())) {
        	@Override
        	protected Class<?> resolveClass(ObjectStreamClass desc)
        			throws IOException, ClassNotFoundException {
        		if (desc.getName().equals(OperationNotSupportedException.class.getName())) {
        			throw new ClassNotFoundException();
        		}
        		return super.resolveClass(desc);
        	}
        	
        };
        
        mmce = (MetaMatrixComponentException)ois.readObject();
	}

}
