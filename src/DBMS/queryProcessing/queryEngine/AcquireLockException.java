package DBMS.queryProcessing.queryEngine;

public class AcquireLockException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public AcquireLockException() {
		super("Could not acquire a lock");
	}
	
}
