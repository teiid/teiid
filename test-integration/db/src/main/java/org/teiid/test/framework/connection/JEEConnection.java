/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.connection;

import java.sql.Connection;
import java.util.Properties;

import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;

/**
 * JEE (JNDI) Connection Strategy, when the test is run inside an application
 * server. Make sure all the jndi names are set correctly in the properties
 * file.
 */
@SuppressWarnings("nls")
public class JEEConnection extends ConnectionStrategy {

    public static final String DS_JNDINAME = "ds-jndiname"; //$NON-NLS-1$

    private String jndi_name = null;


    public JEEConnection(Properties props) throws QueryTestFailedException {
	super(props);
    }

    public Connection getConnection() throws QueryTestFailedException {
	validate();
	try {
	    InitialContext ctx = new InitialContext();
	    DataSource source = (DataSource) ctx.lookup(jndi_name);

	    if (source == null) {
		String msg = "Unable to find jndi source " + jndi_name;//$NON-NLS-1$

		QueryTestFailedException mme = new QueryTestFailedException(msg);//$NON-NLS-1$
		throw mme;
	    }
	    Connection conn = source.getConnection();
	    return conn;
	} catch (QueryTestFailedException qtfe) {
	    throw qtfe;
	} catch (Exception e) {
	    throw new QueryTestFailedException(e);
	}
    }

    public void shutdown() {
	super.shutdown();
	// no connection management here; app server takes care of these..
    }

    public void validate() {
	// TODO Auto-generated method stub

	jndi_name = getEnvironment().getProperty(DS_JNDINAME);
	if (jndi_name == null || jndi_name.length() == 0) {
	    throw new TransactionRuntimeException("Property " + DS_JNDINAME
		    + " was not specified");
	}
    }
}
