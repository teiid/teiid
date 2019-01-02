package org.teiid.test.framework.exception;

/**
 * The TransactionRuntimeException is thrown outside of running a test.
 * When this is thrown, the testing process should stop.
 * 
 * 
 * @author vanhalbert
 *
 */
public class TransactionRuntimeException extends RuntimeException{
    /**
	 * @since
	 */
	private static final long serialVersionUID = 1L;
	
	public TransactionRuntimeException(Exception e){
        super(e);
    }
    public TransactionRuntimeException(String msg){
        super(msg);
    }
}
