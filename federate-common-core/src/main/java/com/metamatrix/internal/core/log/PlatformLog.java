/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.internal.core.log;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.metamatrix.core.log.LogListener;
import com.metamatrix.core.log.LogMessage;
import com.metamatrix.core.log.Logger;
import com.metamatrix.core.log.SystemLogWriter;

/**
 * The PlatformLog class is designed to be extended by any {@link Logger} implementation that is to
 * quickly accept logging requests and return immediately with very little overhead.  Each of the log
 * messages is enqueued in the <code>log</code> methods, and a separate thread then processes the 
 * messages in the queue and sends them to each of the loggers that have been registered as receivers.
 * <p>
 * Listeners (or destinations) of log messages can be configured on the fly while the
 * class is being used to log messages.  The methods to {@link #addListener(LogListener) add}
 * and {@link #removeListener(LogListener) remove} listeners are thread safe and can be called
 * by any thread even while the class is processing log messages.  However, the point at which
 * the new listener will start receiving messages or the removed listener will stop receiving
 * messages is dictated entirely by the number of messages that are in the queue at the
 * time the {@link #addListener(LogListener) add} or {@link #removeListener(LogListener) remove} 
 * methods are called.
 * </p><p>
 * There is a {@link #getInstance() singleton instance} that is started automatically
 * and that, by default, has only one listener that sends non-error messages to 
 * {@link java.lang.System.out System.out} and error messages to 
 * {@link java.lang.System.err System.err}.  This instance can of course be configured
 * differently once running (see above).  
 * </p><p>
 * Because this class has internal resources that should be released when the log is no longer
 * needed, the {@link #shutdown(boolean)} method can be called to signal such a condition.
 * When this method is called with a parameter of <code>true</code>, the log attempts to 
 * process all content and will continue to do so until no content has been found
 * for a short amount of time.  When this method is called with a parameter of <code>false</code>,
 * the log will stop processing content immediately (meaning some content already enqueued
 * will not be processed).  In either case, the {@link #shutdown(boolean)} method blocks
 * until the log has indeed been shutdown.
 * </p><p>
 * <b>Note that the {@link #shutdown(boolean)} method should not be called on the
 * {@link #getInstance() singleton instance}</p>, since that instance automatically shuts
 * itself down upon VM termination (by processing all remaining content).  Thus, the
 * general rule for whether {@link #shutdown(boolean)} should be called is that if
 * some component instantiated a new PlatformLog instance, then some component should
 * call {@link #shutdown(boolean)}.
 * </p><p>
 * This class attempts to have a minimum footprint during normal operation conditions.
 * This is achieved by having a worker thread process the enqueued content, and having
 * that thread terminate if there is nothing to log.  Of course, if the thread has been
 * terminated when new content is enqueued, a new thread will be allocated.  In fact,
 * upon instantiation, there is no thread created; only upon the first call to
 * {@link #logMessage(IStatus, long, String)} will a thread be allocated.
 * </p>
 */
public class PlatformLog implements LogListener {
    
    // =========================================================================
    //                      Static Members
    // =========================================================================
    private static final PlatformLog INSTANCE = new PlatformLog();
    private static ShutdownThread SHUTDOWNTHREAD = null;
    private static final String SHUTDOWN_HOOK_INSTALLED_PROPERTY = "shutdownHookInstalled"; //$NON-NLS-1$
    
    static {
        String hook = System.getProperty(SHUTDOWN_HOOK_INSTALLED_PROPERTY);
        if ( hook == null || hook.equalsIgnoreCase(String.valueOf(Boolean.FALSE))) {
            /**
             * By default, add a listener to write to System.out and System.err,
             * and that is automatically shutdown upon VM termination.
             */
            SHUTDOWNTHREAD = new ShutdownThread(INSTANCE);
            INSTANCE.addListener(  new SystemLogWriter() );
            try {
                Runtime.getRuntime().addShutdownHook(SHUTDOWNTHREAD);
                System.setProperty(SHUTDOWN_HOOK_INSTALLED_PROPERTY, Boolean.TRUE.toString());
            } catch (IllegalStateException e) {
                //ignore: this happens if we try to register the shutdown hook after the system
                //is already shutting down.  there's nothing we can do about it.
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }
    
    public static PlatformLog getInstance() {
        return INSTANCE;
    }

    private static final String DEFAULT_LOG_WORKER_THREAD_NAME = "LogWorker"; //$NON-NLS-1$
    
    private static final long DEFAULT_TIMEOUT = 60000;       // time to wait for a message
    private static final long SHUTDOWN_TIMEOUT = 20000;             // max time to wait for shutdown

    /**
     * Flag specifying whether to write debugging statements to {@link #DEBUG_STREAM}.
     * These statements are <i>not</i> internationalized, and are really only for
     * the purpose of manually debugging this class.
     */
    private static boolean DEBUG_PLATFORM_LOG = false;  // set to true for debugging statements
    private static final PrintStream DEBUG_STREAM = System.err;
    
    // =========================================================================
    //                      Instance Members
    // =========================================================================
    
    /**
     * The {@link LogListener} instances that are to receive the queue contents.
     */
    private final List<LogListener> logListeners = new ArrayList<LogListener>();
    
    /**
     * The worker that takes content from the queue and sends to the various registered receivers.
     */
    private ThreadPoolExecutor executor;
    private String name;
    
    private boolean shutdownRequested = false;
    
    /**
     * Construct an instance of PlatformLog.
     */
    public PlatformLog() {
        this(DEFAULT_LOG_WORKER_THREAD_NAME);
    }

    /**
     * Construct an instance of PlatformLog.
     */
    public PlatformLog( final String name ) {
    	this.name = name;
        init();
    }

	private void init() {
		this.executor = new ThreadPoolExecutor(1, 1, DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
			public Thread newThread(Runnable r) {
				if ( DEBUG_PLATFORM_LOG ) {
                    DEBUG_STREAM.println("Creating and starting a new LogWorker"); //$NON-NLS-1$
                }
				Thread t =new Thread(r);
				t.setDaemon(true);
				t.setName(name);
				return t;
			}});
		this.executor.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
		this.executor.allowCoreThreadTimeOut(true);
	}
    
    public List getLogListeners() {
    	synchronized (this.logListeners) {
            return new ArrayList(this.logListeners);
		}
    }
    
    /**
     * Used by the DQP to ensure that the shutdown thread is removed from the VM 
     * in which the DQP is embedded.
     * @since 4.2
     */
    public static void deregisterShutdownHook() {
        try {
            if (SHUTDOWNTHREAD != null) {
                Runtime.getRuntime().removeShutdownHook(SHUTDOWNTHREAD);
            }
        } catch (IllegalStateException e) {
            //ignore: this happens if we try to register the shutdown hook after the system
            //is already shutting down.  there's nothing we can do about it.
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
    
    /**
     * Shut down and process all content.  This method is equivalent to calling
     * {@link #shutdown(boolean)} with a parameter value of <code>true</code>.
     */
    public void shutdown() {
        this.shutdown(true);
    }
    
    /**
     * This method blocks until all messages have been processed, and should be called only by
     * if the application news up a PlatformLog.  This method should never be called on
     * the {@link PlatformLog#getInstance() singleton}, since that is automatically called by
     * the VM upon termination.
     * <p>
     * Once called, this method clears all listeners (meaning additional calls to 
     * {@link #logMessage(IStatus, long, String) logMessage} will not actually enqueue
     * any message.  
     * </p><p>
     * Once this method is called, there is no way to restart the log.
     * </p>
     */
    public synchronized void shutdown( final boolean processRemainingContent ) {
        if ( this.shutdownRequested ) {
        	return;
        }
        this.shutdownRequested = true;
        if (processRemainingContent) {
        	this.executor.shutdown();
        } else {
        	this.executor.shutdownNow();
        }
        
        // Block until worker has completed
        try {
			this.executor.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			if ( DEBUG_PLATFORM_LOG ) {
                DEBUG_STREAM.println("PlatformLog.waitMethod returning because timeout exceeded"); //$NON-NLS-1$
            }
		}
		
        if ( DEBUG_PLATFORM_LOG ) {
            DEBUG_STREAM.println("PlatformLog has been shutdown"); //$NON-NLS-1$
        }
        
        // Remove all listeners so that additional calls to logMessage(...)
        // do not put anything into the queue
        synchronized (this.logListeners) {
            final Iterator iter = this.logListeners.iterator();
            while (iter.hasNext()) {
                final LogListener listener = (LogListener)iter.next();
                try {
                    listener.shutdown();
                } catch ( Throwable t ) {
                    // ignore it
                }
            }
            this.logListeners.clear();
        }
    }
    
    public synchronized void start() {
    	if (!this.shutdownRequested) {
    		return;
    	}
    	this.shutdownRequested = false;
    	init();
    }
    
    // =========================================================================
    //                      Receiver management methods
    // =========================================================================
    
    public void addListener( final LogListener listener ) {
        if ( listener != null ) {
            if ( DEBUG_PLATFORM_LOG ) {
                DEBUG_STREAM.println("Adding to PlatformLog a new log listener " + listener); //$NON-NLS-1$
            }
            synchronized (this.logListeners) {
                // replace if already exists (Set behavior but we use an array
                // since we want to retain order)
                this.logListeners.remove(listener);
                this.logListeners.add(listener);
            }
        }
    }

    public void removeListener( final LogListener listener) {
        if ( listener != null ) {
            if ( DEBUG_PLATFORM_LOG ) {
                DEBUG_STREAM.println("Removing from PlatformLog log listener " + listener); //$NON-NLS-1$
            }
            synchronized (this.logListeners) {
                this.logListeners.remove(listener);
            }
            listener.shutdown();
        }
    }
    
    // =========================================================================
    //                      LogListener methods
    // =========================================================================

    /**
     * Notifies all listeners of the platform log.  This includes the console log, if 
     * used, and the platform log file.  All Plugin log messages get funnelled
     * through here as well.
     */
    public void logMessage(final LogMessage msg) {
        // Create the message and put it on the queue ...
        final Message messageObj = new Message(msg);
        if ( DEBUG_PLATFORM_LOG ) {
            DEBUG_STREAM.println("Enqueuing message " + messageObj ); //$NON-NLS-1$
        }
    	executor.execute(messageObj);
    }

    // =========================================================================
    //                      Queue content class(es)
    // =========================================================================
    
    class Message implements Runnable {
        private final LogMessage msg;
        
        Message( final LogMessage msg) {
            this.msg = msg;
        }

        public void run() {
        	synchronized (logListeners) {
        		int size = logListeners.size();
	        	for (int i = 0; i < size; i++) {
	        		logListeners.get(i).logMessage(this.msg);	
				}
        	}
        }
        
        public String toString() {
            return msg.getText();
        }        
    }

}
     
class ShutdownThread extends Thread {
	private static final String SHUTDOWN_THREAD_NAME = "Shutdown"; //$NON-NLS-1$
	private PlatformLog log;

	ShutdownThread(final PlatformLog platformLog) {
		super(SHUTDOWN_THREAD_NAME);
		this.log = platformLog;
	}

	public void run() {
		final boolean processRemainingContent = true;
		this.log.shutdown(processRemainingContent);
	}

}
    
