/*
 * JBoss, Home of Professional Open Source
 * Copyright 2006, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 *
 * (C) 2005-2006,
 * @author JBoss Inc.
 */
/*
 * Copyright (C) 1999-2001 by HP Bluestone Software, Inc. All rights Reserved.
 *
 * HP Arjuna Labs,
 * Newcastle upon Tyne,
 * Tyne and Wear,
 * UK.
 *
 * $Id: PeriodicRecovery.java 2342 2006-03-30 13:06:17Z  $
 */

package com.arjuna.ats.internal.arjuna.recovery;

import java.lang.InterruptedException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Vector;
import java.net.*;
import java.io.*;

import com.arjuna.ats.arjuna.recovery.RecoveryModule;
import com.arjuna.ats.arjuna.recovery.RecoveryEnvironment;
import com.arjuna.ats.arjuna.common.arjPropertyManager;

import com.arjuna.ats.arjuna.logging.FacilityCode;
import com.arjuna.ats.arjuna.logging.tsLogger;

import com.arjuna.common.util.logging.*;

/**
 * Threaded object to perform the periodic recovery. Instantiated in
 * the RecoveryManager. The work is actually completed by the recovery
 * modules. These modules are dynamically loaded. The modules to load
 * are specified by properties beginning with "RecoveryExtension"
 * <P>
 * @author
 * @version $Id: PeriodicRecovery.java 2342 2006-03-30 13:06:17Z  $
 *
 * @message com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_1 [com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_1] - Attempt to load recovery module with null class name!
 * @message com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_2 [com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_2] - Recovery module {0} does not conform to RecoveryModule interface
 * @message com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_3 [com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_3] - Loading recovery module: {0}
 * @message com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_4 [com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_4] - Loading recovery module: {0}
 * @message com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_5 [com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_5] - Loading recovery module: could not find class {0}
 * @message com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_6 [com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_6] - {0} has inappropriate value ( {1} )
 * @message com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_7 [com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_7] - {0} has inappropriate value ( {1} )
 * @message com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_8 [com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_8] - Invalid port specified {0}
 * @message com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_9 [com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_9] - Could not create recovery listener {0}
 * @message com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_10 [com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_10] - Ignoring request to scan because RecoveryManager state is: {0}
 */

/*
 * To be removed after 4.5 see JBTM-434
 */
public class PeriodicRecovery extends Thread
{

/*
 * TODO uncomment for JDK 1.5.
 *
   public static enum State
   {
       created, active, terminated, suspended, scanning
   }
*/
    public class State
    {
        public static final int created = 0;
        public static final int active = 1;
        public static final int terminated = 2;
        public static final int suspended = 3;
        public static final int scanning  = 4;

        private State () {}
    }

   public PeriodicRecovery (boolean threaded)
   {
	  setDaemon(true);
      initialise();

      // Load the recovery modules that actually do the work.

      loadModules();

      try
      {
	  _workerService = new WorkerService(this);

	  _listener = new Listener(getServerSocket(), _workerService);
	  _listener.setDaemon(true);
      }
      catch (Exception ex)
      {
	  if (tsLogger.arjLoggerI18N.isWarnEnabled())
	  {
	      tsLogger.arjLoggerI18N.warn("com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_9", new Object[]{ex});
	  }
      }

      if (threaded)
      {
	  start();
      }

      _listener.start();
   }

    public int getStatus ()
    {
	synchronized (_stateLock)
	    {
		return _currentState;
	    }
    }

    public void setStatus (int s)
    {
	synchronized (_stateLock)
	    {
		_currentState = s;
	    }
    }

   public void shutdown ()
   {
       setStatus(State.terminated);

      this.interrupt();
   }

   public void suspendScan (boolean async)
   {
       synchronized (_signal)
       {
	   setStatus(State.suspended);

	   this.interrupt();

	   if (!async)
	   {
	       try
	       {
		   _signal.wait();
	       }
	       catch (InterruptedException ex)
	       {
	       }
	   }
       }
   }

   public void resumeScan ()
   {
       /*
        * If it's suspended, then it has to be blocked
        * on the lock.
        */

       if (getStatus() == State.suspended)
       {
           setStatus(State.active);

           synchronized (_suspendLock)
           {
               _suspendLock.notify();
           }
       }
   }

   /**
    * Return the port specified by the property
    * com.arjuna.ats.internal.arjuna.recovery.recoveryPort,
    * otherwise return a default port.
    */

    public static final ServerSocket getServerSocket () throws IOException
    {
	    if (_socket == null)
	    {
		// TODO these properties should be documented!!

		String tsmPortStr = arjPropertyManager.propertyManager.getProperty(com.arjuna.ats.arjuna.common.Environment.RECOVERY_MANAGER_PORT);
		int port = 0;

		if (tsmPortStr != null)
		{
		    try
		    {
			port = Integer.parseInt( tsmPortStr );
		    }
		    catch (Exception ex)
		    {
			if (tsLogger.arjLoggerI18N.isWarnEnabled())
			{
			    tsLogger.arjLoggerI18N.warn("com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_8", new Object[]{ex});
			}
		    }
		}

		_socket = new ServerSocket(port);
	    }

	return _socket;
    }

   /**
    * Start the background thread to perform the periodic recovery
    */

   public void run ()
   {
       boolean finished = false;

       do
       {
	   checkSuspended();

	   finished = doWork(true);

       } while (!finished);
   }

    /**
     * Perform the recovery scans on all registered modules.
     *
     * @param boolean periodic If <code>true</code> then this is being called
     * as part of the normal periodic running of the manager and we'll sleep
     * after phase 2 work. Otherwise, we're being called directly and there should
     * be no sleep after phase 2.
     *
     * @return <code>true</code> if the manager has been instructed to finish,
     * <code>false</code> otherwise.
     */

    public final synchronized boolean doWork (boolean periodic)
    {
	boolean interrupted = false;

	/*
	 * If we're suspended or already scanning, then ignore.
	 */

	synchronized (_stateLock)
	{
	    if (getStatus() != State.active)
	    {
		if (tsLogger.arjLoggerI18N.isInfoEnabled())
		{
		    tsLogger.arjLoggerI18N.info("com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_10", new Object[]{new Integer(getStatus())});
		}

		return false;
	    }

	    setStatus(State.scanning);
	}

	tsLogger.arjLogger.info("Periodic recovery - first pass <" +
				_theTimestamper.format(new Date()) + ">" );

	Enumeration modules = _recoveryModules.elements();

	while (modules.hasMoreElements())
	{
	    RecoveryModule m = (RecoveryModule) modules.nextElement();

	    m.periodicWorkFirstPass();

	    if (tsLogger.arjLogger.isDebugEnabled())
	    {
		tsLogger.arjLogger.debug( DebugLevel.FUNCTIONS,
					  VisibilityLevel.VIS_PUBLIC,
					  FacilityCode.FAC_CRASH_RECOVERY,
					  " " );
	    }
	}

	if (interrupted)
	{
	    interrupted = false;

	    _workerService.signalDone();
	}

	// wait for a bit to avoid catching (too many) transactions etc. that
	// are really progressing quite happily

	try
	{
	    Thread.sleep( _backoffPeriod * 1000 );
	}
	catch ( InterruptedException ie )
	{
	    interrupted = true;
	}

	if (getStatus() == State.terminated)
	{
	    return true;
	}
	else
	{
	    checkSuspended();

	    setStatus(State.scanning);
	}

	tsLogger.arjLogger.info("Periodic recovery - second pass <"+
				_theTimestamper.format(new Date()) + ">" );

	modules = _recoveryModules.elements();

	while (modules.hasMoreElements())
	{
	    RecoveryModule m = (RecoveryModule) modules.nextElement();

	    m.periodicWorkSecondPass();

	    if (tsLogger.arjLogger.isDebugEnabled())
	    {
		tsLogger.arjLogger.debug ( DebugLevel.FUNCTIONS, VisibilityLevel.VIS_PUBLIC, FacilityCode.FAC_CRASH_RECOVERY, " " );
	    }
	}

	try
	{
	    if (!interrupted && periodic)
		Thread.sleep( _recoveryPeriod * 1000 );
	}
	catch ( InterruptedException ie )
	{
	    interrupted = true;
	}

	if (getStatus() == State.terminated)
	{
	    return true;
	}
	else
	{
	    checkSuspended();

	    // make sure we're scanning again.

	    setStatus(State.active);
	}

	return false; // keep going
    }

    /**
     * Add the specified module to the end of the recovery module list.
     * There is no way to specify relative ordering of recovery modules
     * with respect to modules loaded via the property file.
     *
     * @param RecoveryModule module The module to append.
     */

    public final void addModule (RecoveryModule module)
    {
	_recoveryModules.add(module);
    }

    /**
     * @return the recovery modules.
     */

    public final Vector getModules ()
    {
	return _recoveryModules;
    }

    /**
     * Load recovery modules prior to starting to recovery. The property
     * name of each module is used to indicate relative ordering.
     */

   private final static void loadModules ()
   {
      // scan the relevant properties so as to get them into sort order
       Properties properties = arjPropertyManager.propertyManager.getProperties();

      if (properties != null)
      {
         Vector moduleNames = new Vector();
         Enumeration names = properties.propertyNames();

         while (names.hasMoreElements())
         {
            String attrName = (String) names.nextElement();

            if (attrName.startsWith(RecoveryEnvironment.MODULE_PROPERTY_PREFIX))
            {
               // this is one of ours - put it in the right place
               int position = 0;

               while ( position < moduleNames.size() &&
                       attrName.compareTo( (String)moduleNames.elementAt(position)) > 0 )
               {
                  position++;
               }
               moduleNames.add(position,attrName);
            }
         }
         // now go through again and load them
         names = moduleNames.elements();

         while (names.hasMoreElements())
         {
            String attrName = (String) names.nextElement();

            loadModule(properties.getProperty(attrName));
         }
      }
   }

   private final static void loadModule (String className)
   {
       if (tsLogger.arjLogger.isDebugEnabled())
       {
         tsLogger.arjLogger.debug( DebugLevel.FUNCTIONS,
				   VisibilityLevel.VIS_PRIVATE,
				   FacilityCode.FAC_CRASH_RECOVERY,
				   "Loading recovery module "+
				   className );
       }

      if (className == null)
      {
  	  if (tsLogger.arjLoggerI18N.isWarnEnabled())
	      tsLogger.arjLoggerI18N.warn("com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_1");

         return;
      }
      else
      {
         try
         {
	     Class c = Thread.currentThread().getContextClassLoader().loadClass( className );

            try
            {
               RecoveryModule m = (RecoveryModule) c.newInstance();
               _recoveryModules.add(m);
            }
            catch (ClassCastException e)
            {
		if (tsLogger.arjLoggerI18N.isWarnEnabled())
		{
		    tsLogger.arjLoggerI18N.warn("com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_2",
						new Object[]{className});
		}
            }
            catch (IllegalAccessException iae)
            {
		if (tsLogger.arjLoggerI18N.isWarnEnabled())
		{
		    tsLogger.arjLoggerI18N.warn("com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_3",
						new Object[]{iae});
		}
            }
            catch (InstantiationException ie)
            {
		if (tsLogger.arjLoggerI18N.isWarnEnabled())
		{
		    tsLogger.arjLoggerI18N.warn("com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_4",
						new Object[]{ie});
		}
            }

            c = null;
         }
         catch ( ClassNotFoundException cnfe )
         {
 	     if (tsLogger.arjLoggerI18N.isWarnEnabled())
	     {
		 tsLogger.arjLoggerI18N.warn("com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_5",
					     new Object[]{className});
	     }
         }
      }
   }

    private void checkSuspended ()
    {
	synchronized (_signal)
	{
	    _signal.notify();
	}

	if (getStatus() == State.suspended)
	{
	    while (getStatus() == State.suspended)
	    {
		try
		{
		    synchronized (_suspendLock)
		    {
			_suspendLock.wait();
		    }
		}
		catch (InterruptedException ex)
		{
		}
	    }

	    setStatus(State.active);
	}
    }

   private final void initialise ()
   {
       _recoveryModules = new Vector();
       setStatus(State.active);
   }

   // this refers to the modules specified in the recovery manager
   // property file which are dynamically loaded.
   private static Vector _recoveryModules = null;

   // back off period is the time between the first and second pass.
   // recovery period is the time between the second pass and the start
   // of the first pass.
   private static int _backoffPeriod = 0;
   private static int _recoveryPeriod = 0;

   // default values for the above
   private static final int _defaultBackoffPeriod = 10;
   private static final int _defaultRecoveryPeriod = 120;

   // exit thread flag
   private static int _currentState = State.created;
   private static Object _stateLock = new Object();

   private static SimpleDateFormat _theTimestamper = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss");

    private static ServerSocket _socket = null;

    private static Listener _listener = null;
    private static WorkerService _workerService = null;

    private Object _suspendLock = new Object();
    private Object _signal = new Object();

   /*
    * Read the system properties to set the configurable options
    *
    * Note: if we start and stop the service then changes to the timeouts
    * won't be reflected. We will need to modify this eventually.
    */

   static
   {
      _recoveryPeriod = _defaultRecoveryPeriod;

      String recoveryPeriodString =
         arjPropertyManager.propertyManager.getProperty(com.arjuna.ats.arjuna.common.Environment.PERIODIC_RECOVERY_PERIOD );

      if ( recoveryPeriodString != null )
      {
         try
         {
            Integer recoveryPeriodInteger = new Integer( recoveryPeriodString );
            _recoveryPeriod = recoveryPeriodInteger.intValue();

	    if (tsLogger.arjLogger.isDebugEnabled())
	    {
               tsLogger.arjLogger.debug
                  ( DebugLevel.FUNCTIONS,
                    VisibilityLevel.VIS_PRIVATE,
                    FacilityCode.FAC_CRASH_RECOVERY,
                    "com.arjuna.ats.arjuna.recovery.PeriodicRecovery" +
                    ": Recovery period set to " + _recoveryPeriod + " seconds" );
	    }
         }
         catch (NumberFormatException e)
         {
	     if (tsLogger.arjLoggerI18N.isWarnEnabled())
	     {
		 tsLogger.arjLoggerI18N.warn("com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_6",
					     new Object[]{com.arjuna.ats.arjuna.common.Environment.PERIODIC_RECOVERY_PERIOD, recoveryPeriodString});
	     }
         }
      }

      _backoffPeriod = _defaultBackoffPeriod;

      String backoffPeriodString=
         arjPropertyManager.propertyManager.getProperty(com.arjuna.ats.arjuna.common.Environment.RECOVERY_BACKOFF_PERIOD);


      if (backoffPeriodString != null)
      {
         try
         {
            Integer backoffPeriodInteger = new Integer(backoffPeriodString);
            _backoffPeriod = backoffPeriodInteger.intValue();

	    if (tsLogger.arjLogger.isDebugEnabled())
	    {
               tsLogger.arjLogger.debug
                  ( DebugLevel.FUNCTIONS,
                    VisibilityLevel.VIS_PRIVATE,
                    FacilityCode.FAC_CRASH_RECOVERY,
                    "PeriodicRecovery" +
                    ": Backoff period set to " + _backoffPeriod + " seconds" );
	    }
         }
         catch (NumberFormatException e)
         {
     	     if (tsLogger.arjLoggerI18N.isWarnEnabled())
	     {
		 tsLogger.arjLoggerI18N.warn("com.arjuna.ats.internal.arjuna.recovery.PeriodicRecovery_7",
					     new Object[]{com.arjuna.ats.arjuna.common.Environment.RECOVERY_BACKOFF_PERIOD, backoffPeriodString});
	     }
         }
      }
   }

}








