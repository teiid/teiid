/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
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

package com.metamatrix.console.models;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.Timer;

import com.metamatrix.console.connections.ConnectionInfo;
  
/**
 * Abstract superclass of timed managers - timed managers maintain data which
 *becomes stale after a certain amount of time, and needs periodic
 *refreshing.<P>
 *
 * Clients can specify a refresh rate in seconds, and dynamically enable or
 *disable auto refreshing.<P>
 *
 * A TimedManager has a <CODE>true</CODE> property, which subclasses should
 *set to <CODE>true</CODE> when about to start a lengthy network operation,
 *and then set to <CODE>false</CODE> when and if the operation finishes.
 *The TimedManager class will ignore it's internal timer while
 *busy, since it is in the middle of refreshing itself anyway.  <B>Note:</B>
 *it is a good idea to set the code that sets <CODE>busy</CODE> back to
 *<CODE>false</CODE> in a <CODE>finally</CODE> block, to ensure that the
 *TimedManager will be un-busied in the event of a failed network operation.<P>
 *
 *Example:
 *
 *<PRE>
 *public class SomeDataManager extends TimedManager{
 *
 *    public Collection retrieveSomeData(){
 *        Collection result;
 *        try{
 *            super.setBusy(true);
 *            //HERE'S THE OPERATION THAT CAN TAKE SOME TIME, AND
 *            //POTENTIALLY FAIL --------------------------------
 *            result = remoteProxy.retrieveSomeData;
 *            //-------------------------------------------------
 *        }catch(SomeException e){
 *            //do something
 *        }catch(OtherException e){
 *            //do something
 *        }finally{
 *            super.setBusy(false);
 *        }
 *    }
 *}
 *</PRE>
 *
 */
public abstract class TimedManager extends Manager implements ActionListener{

    /**
     * Default refresh rate (seconds) used if none is otherwise
     *specified.
     */
    public static final int DEFAULT_DELAY = 60;

    private Timer timer;
    private int refreshRate;
    private boolean isAutoRefreshEnabled;
    private boolean busy = false;

    /**
     * Creates a TimedManager with the default refresh rate
     * @see TimedManager#DEFAULT_DELAY
     */
    public TimedManager(ConnectionInfo connection) {
        this(connection, DEFAULT_DELAY, true);
    }

    /**
     * Creates a TimedManager with the specified refresh rate
     * @param refreshRateSeconds rate at which TimedManager refreshes itself
     */
    public TimedManager(ConnectionInfo connection, int refreshRateSeconds){
        this(connection, refreshRateSeconds, true);       
    }

    /**
     * Creates a TimedManager with the specified refresh rate
     * @param refreshRateSeconds rate at which TimedManager refreshes itself
     * @param isAutoRefreshEnabled controls whether TimedManager will
     *refresh itself or not
     */
    public TimedManager(ConnectionInfo connection,
    		int refreshRateSeconds, boolean isAutoRefreshEnabled){
    	super(connection);
        setRefreshRate(refreshRateSeconds);
        setIsAutoRefreshEnabled(isAutoRefreshEnabled);
    }

    /**
     * Calls super.init() and also initializes timing behavior.
     * @see TimedManager#setRefreshRate
     */
    public void init(){
        super.init();
        setTimer(new Timer(getRefreshRate()*1000, this));
    }

    /**
     * If the timer isn't already started, and autoRefresh is enabled,
     *starts this Timed Manager's 
     *clock ticking until a Manager's data in memory is
     *considered stale.  Subclasses of TimedManager should call this
     *method every time a request is made for its data.<P>
     *
     * Note that this method may always be called - it will only
     *start the timer if autoRefresh is enabled.<P>
     *
     * There is no explicit stopTimer method, rather the refresh method
     *causes the timer to stop.  (The refresh method is either called 
     *by some outside code, or by this TimedManager when it's timer
     *runs out.)
     * @see TimedManager#refresh
     * @see TimedManager#setIsAutoRefreshEnabled
     */
    public void startTimer(){
        if (!getTimer().isRunning() && getIsAutoRefreshEnabled()){
            getTimer().start();
        }
    }

    /**
     * This method calls super.refresh and, additionally, stops the timer
     *running.  Subclasses of this TimedManager class must be sure to call
     *the start() method whenever it retrieves its data.
     * @see TimedManager#startTimer
     * @see Manager#refresh
     */
    public void refresh(boolean setIsStale){
        getTimer().stop();
        if (setIsStale) {
            super.refresh();
        }
    }

    public void refresh() {
        refresh(true);
    }
    
    /**
     * This method, called by this Manager's Timer, indicates that
     *time has run out.  In response, this manager triggers its own
     *refresh.
     * @see TimedManager#refresh
     */
    public void actionPerformed(ActionEvent e){
        if (!isBusy()){
            this.refresh();
        }
    }

    //GETTERS-SETTERS

    /**
     * @return time, in seconds, after which data is stale
     */
    public int getRefreshRate(){
        return refreshRate;
    }

    /**
     * Subclasses of TimedManager must be sure to set their refresh rate, in
     *addition to other initialization behavior.
     *
     * @param seconds time after which data is stale
     */
    public void setRefreshRate(int seconds){
        //Timer CLASS HANDLES CASE WHERE TIMER IS ALREADY RUNNING
        refreshRate = seconds;

        boolean started = false;
        if (getTimer() != null){
            getTimer().removeActionListener(this);
            if (getTimer().isRunning()){
                started = true;
            }
        }

        setTimer(new Timer(getRefreshRate()*1000, this));
        if (started){
            startTimer();
        }

    }

    /**
     * If the autoRefresh feature of TimedManager is enabled, the timer will
     *run and the Manager will be notified each time the timer runs out, and
     *will call it's own refresh() method.
     *Disabling autoRefresh means that a TimedManager can only be refreshed
     *by explicitly having its refresh() method called.
     * @return boolean indicating if AutoRefresh is enabled or not
     */
    public boolean getIsAutoRefreshEnabled(){
        return isAutoRefreshEnabled;
    }

    /**
     * If the autoRefresh feature of TimedManager is enabled, the timer will
     *run and the Manager will be notified each time the timer runs out, and
     *will call it's own refresh() method.
     *Disabling autoRefresh means that a TimedManager can only be refreshed
     *by explicitly having its refresh() method called.
     * @paran isAutoRefreshEnabled boolean indicating if AutoRefresh is 
     *enabled or not
     */
    public void setIsAutoRefreshEnabled(boolean isAutoRefreshEnabled){
        this.isAutoRefreshEnabled = isAutoRefreshEnabled;
        if ((!isAutoRefreshEnabled) && getTimer() != null){
            if (getTimer().isRunning()){
                getTimer().stop();
            }
        }
        if ((isAutoRefreshEnabled) && getTimer() != null){
            startTimer();
        }
    }

    /**
     * Returns the Timer object used by TimedManager.  It is not intended that
     *it will ever be necessary to access this object directly.
     */
    protected Timer getTimer(){
        return timer;
    }

    private void setTimer(Timer timer){
        if (this.timer != null){
            this.timer.removeActionListener(this);
        }
        this.timer = timer;
    }

    /**
     * Indicates whether this TimedManager is in the middle of a network
     *call or not.  TimedManager will ignore its internal timer during this
     *time, if auto-refresh is enabled.<P>
     *
     *Managers should set themselves to busy right before
     *beginning a potentially long network operation, and set themselves
     *to not busy right after the operation completes.
     * @return boolean indicating whether TimedManager is in the middle of an
     *atomic lengthy call
     */
    public boolean isBusy(){
        return busy;
    }

    /**
     * Indicates whether this TimedManager is in the middle of a network
     *call or not.  TimedManager will ignore its internal timer during this
     *time, if auto-refresh is enabled.<P>
     *
     *Managers should set themselves to busy right before
     *beginning a potentially long network operation, and set themselves
     *to not busy right after the operation completes.
     * @param busy indicates TimedManager is in the middle of an atomic
     *network call
     */
    public synchronized void setBusy(boolean busy){
        this.busy = busy;
    }
}
