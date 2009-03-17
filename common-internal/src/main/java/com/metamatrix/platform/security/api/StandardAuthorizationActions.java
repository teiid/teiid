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

package com.metamatrix.platform.security.api;

import java.io.Serializable;
import java.util.*;


/**
 * The static and closed set of basic authorization actions.  Actions include "create", "read",
 * "update", and "delete".
 * <p>
 * The instances are static to both close the set and minimize VM resource requirements.
 * @see AuthorizationActions
 */
public class StandardAuthorizationActions implements Serializable, AuthorizationActions {

    public static final int NONE_VALUE          = 0;
    public static final int DATA_CREATE_VALUE   = 1;
    public static final int DATA_READ_VALUE     = 2;
    public static final int DATA_UPDATE_VALUE   = 4;
    public static final int DATA_DELETE_VALUE   = 8;
    public static final int ALL_VALUE = DATA_CREATE_VALUE | DATA_READ_VALUE | DATA_UPDATE_VALUE | DATA_DELETE_VALUE;

    public static final String NONE_LABEL           = "None"; //$NON-NLS-1$
    public static final String DATA_CREATE_LABEL    = "Create"; //$NON-NLS-1$
    public static final String DATA_READ_LABEL      = "Read"; //$NON-NLS-1$
    public static final String DATA_UPDATE_LABEL    = "Update"; //$NON-NLS-1$
    public static final String DATA_DELETE_LABEL    = "Delete"; //$NON-NLS-1$

    public static final AuthorizationActions NONE           = new StandardAuthorizationActions(NONE_VALUE,   new String[]{NONE_LABEL});
    public static final AuthorizationActions DATA_CREATE    = new StandardAuthorizationActions(DATA_CREATE_VALUE, new String[]{DATA_CREATE_LABEL});
    public static final AuthorizationActions DATA_UPDATE    = new StandardAuthorizationActions(DATA_UPDATE_VALUE, new String[]{DATA_UPDATE_LABEL});
    public static final AuthorizationActions DATA_READ      = new StandardAuthorizationActions(DATA_READ_VALUE, new String[]{DATA_READ_LABEL});
    public static final AuthorizationActions DATA_DELETE    = new StandardAuthorizationActions(DATA_DELETE_VALUE, new String[]{DATA_DELETE_LABEL});
    public static final AuthorizationActions ALL            = new StandardAuthorizationActions(ALL_VALUE,    new String[]{DATA_CREATE_LABEL,DATA_READ_LABEL,DATA_UPDATE_LABEL,DATA_DELETE_LABEL});

    private static final int LABELS_COUNT = 6;
    private static Map actionSet = new HashMap();

    private int actions;
    private String label;
    private String[] labels;
    private Collection labelCollection;

    static {
        addAction( NONE_VALUE,  new String[] {NONE_LABEL} );

        addAction( DATA_CREATE_VALUE,   new String[] {DATA_CREATE_LABEL} );

        addAction( DATA_READ_VALUE,                      new String[] {DATA_READ_LABEL} );
        addAction( DATA_CREATE_VALUE | DATA_READ_VALUE,  new String[] {DATA_CREATE_LABEL,DATA_READ_LABEL} );
        
        addAction( DATA_UPDATE_VALUE,                                                               new String[] {DATA_UPDATE_LABEL} );
        addAction( DATA_CREATE_VALUE | DATA_UPDATE_VALUE,                                           new String[] {DATA_CREATE_LABEL,DATA_UPDATE_LABEL} );
        addAction( DATA_READ_VALUE | DATA_UPDATE_VALUE,                                             new String[] {DATA_READ_LABEL,DATA_UPDATE_LABEL} );
        addAction( DATA_CREATE_VALUE | DATA_READ_VALUE | DATA_UPDATE_VALUE,                         new String[] {DATA_CREATE_LABEL,DATA_READ_LABEL,DATA_UPDATE_LABEL} );

        addAction( DATA_DELETE_VALUE,                                                                                   new String[] {DATA_DELETE_LABEL} );
        addAction( DATA_CREATE_VALUE | DATA_DELETE_VALUE,                                                               new String[] {DATA_CREATE_LABEL,DATA_DELETE_LABEL} );
        addAction( DATA_READ_VALUE | DATA_DELETE_VALUE,                                                                 new String[] {DATA_READ_LABEL,DATA_DELETE_LABEL} );
        addAction( DATA_CREATE_VALUE | DATA_READ_VALUE | DATA_DELETE_VALUE,                                             new String[] {DATA_CREATE_LABEL,DATA_READ_LABEL,DATA_DELETE_LABEL} );
        addAction( DATA_UPDATE_VALUE | DATA_DELETE_VALUE,                                                               new String[] {DATA_UPDATE_LABEL,DATA_DELETE_LABEL} );
        addAction( DATA_CREATE_VALUE | DATA_UPDATE_VALUE | DATA_DELETE_VALUE,                                           new String[] {DATA_CREATE_LABEL,DATA_UPDATE_LABEL,DATA_DELETE_LABEL} );
        addAction( DATA_READ_VALUE | DATA_UPDATE_VALUE | DATA_DELETE_VALUE,                                             new String[] {DATA_READ_LABEL,DATA_UPDATE_LABEL,DATA_DELETE_LABEL} );
        addAction( ALL_VALUE,                         new String[] {DATA_CREATE_LABEL,DATA_READ_LABEL,DATA_UPDATE_LABEL,DATA_DELETE_LABEL} );
    }

    private static void addAction( int values, String[] labels ) {
        actionSet.put( new Integer(values), new StandardAuthorizationActions(values,labels) );
    }

    /**
     * Constructor that is used to instantiate the an instances of this class
     * with the specified actions.  This method assumes that the action
     * value is in the correct range.
     * @param actions the set of actions (logical inclusive OR)
     * @param labels the strings that make up the individual labels for this authorization
     * @throws IllegalArgumentException if the specified value is not correct.
     */
    private StandardAuthorizationActions(int actions, String[] labels) {
        this.actions = actions;
        this.labels = labels;
        this.label = ""; //$NON-NLS-1$
        this.labelCollection = new ArrayList( LABELS_COUNT );
        for (int i=0; i!= this.labels.length; ++i ) {
            if ( i!=0 ) {
                this.label = this.label + ","; //$NON-NLS-1$
            }
            this.label = this.label + this.labels[i];
            this.labelCollection.add(this.labels[i]);
        }
    }

    /**
     * Obtain the AuthorizationActions instance that is associated with the specified
     * set of actions.
     * @param actions the set of actions (logical inclusive OR)
     * @return the instance associated with the set of action, or null if the
     * actions do not correspond to an existing instance
     * @throws IllegalArgumentException if the specified set of actions is
     * invalid.
     */
    public static AuthorizationActions getAuthorizationActions(int actions) {
        if (actions > ALL_VALUE || actions < 0 ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0066));
        }
        AuthorizationActions results = (AuthorizationActions) actionSet.get( new Integer(actions) );
        if (results == null) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0067, actions));
        }
        return (AuthorizationActions) actionSet.get( new Integer(actions) );
    }

    /**
     * Obtain the AuthorizationActions instance that is associated with the specified
     * set of labels.
     * @param labels the set of labels
     * @return the instance associated with the set of labels, or null if the
     * labels do not correspond to an existing instance
     * @throws IllegalArgumentException if the specified set of actions is
     * invalid.
     */
    public static AuthorizationActions getAuthorizationActions(String[] labels) {
        if (labels == null || labels.length == 0 || labels.length > LABELS_COUNT ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0068, (Object[])labels));
        }
        Iterator iter = actionSet.values().iterator();
        while ( iter.hasNext() ) {
            AuthorizationActions action = (AuthorizationActions) iter.next();
            if ( action.containsLabels(labels) ) {
                return action;
            }
        }
        return null;
    }

    /**
     * Obtain the AuthorizationActions instance that is associated with the specified
     * set of labels.
     * @param labels the set of labels, with labels all being separated by commas,
     * and must correspond exactly to the <code>getLabel</code> of one of the actions.
     * @return the instance associated with the set of labels, or null if the
     * labels do not correspond to an existing instance
     * @throws IllegalArgumentException if the specified set of actions is
     * invalid.
     */
    public static AuthorizationActions getAuthorizationActions(String labels) {
        if (labels == null || labels.length() == 0  ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0068, labels));
        }
        Iterator iter = actionSet.values().iterator();
        while ( iter.hasNext() ) {
            AuthorizationActions action = (AuthorizationActions) iter.next();
            if ( action.getLabel().equals(labels) ) {
                return action;
            }
        }
        return null;
    }

    /**
     * Obtain the AuthorizationActions instance that is associated with the specified
     * set of labels.
     * @param labels the set of labels
     * @return the instance associated with the set of labels, or null if the
     * labels do not correspond to an existing instance
     * @throws IllegalArgumentException if the specified set of actions is
     * invalid.
     */
    public static AuthorizationActions getAuthorizationActions(Collection labels) {
        if (labels == null  ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0068, labels));
        }
        int labelCount = labels.size();
        if (labelCount == 0 || labelCount > LABELS_COUNT ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0068, labels));
        }
        Iterator iter = actionSet.values().iterator();
        while ( iter.hasNext() ) {
            AuthorizationActions action = (AuthorizationActions) iter.next();
            if ( action.containsLabels(labels) && labels.size() == action.getLabelCount() ) {
                return action;
            }
        }
        return null;
    }

    /**
     * Get the <code>AuthorizationActions</code> that or in <code>actions</code> <i>OR</i> are in
     * <code>otherActions</code>. May be <code>StandardAuthorizationActions.NONE</code> if there
     * are no actions in either argument.
     * @param actions
     * @param otherActions
     * @return The logical OR of the <code>AuthorizationActions<code> in both args or
     * <code>StandardAuthorizationActions.NONE</code> if none exist.
     */
    public static AuthorizationActions getORedActions(AuthorizationActions actions,
                                                      AuthorizationActions otherActions) {
        int oredValue = (actions.getValue() | otherActions.getValue());
        AuthorizationActions oredActions =
            StandardAuthorizationActions.getAuthorizationActions(oredValue);
        return oredActions;
    }

    /**
     * Get the <code>AuthorizationActions</code> in <code>actions</code> that are also in
     * <code>sharedActions</code>. May be <code>StandardAuthorizationActions.NONE</code> if there
     * are no common actions.
     * @param actions
     * @param sharedActions
     * @return The <code>AuthorizationActions<code> common to both args or
     * <code>StandardAuthorizationActions.NONE</code> if none exist.
     */
    public static AuthorizationActions getCommonActions(AuthorizationActions actions,
                                                        AuthorizationActions sharedActions) {
        int commonValue = (actions.getValue() & sharedActions.getValue());
        AuthorizationActions commonActions =
            StandardAuthorizationActions.getAuthorizationActions(commonValue);
        return commonActions;
    }

    /**
     * Get the <code>AuthorizationActions</code> in <code>actions</code> that are also in
     * <code>sharedActions</code>. May be <code>StandardAuthorizationActions.NONE</code> if there
     * are no common actions.
     * @param actions
     * @param sharedActions
     * @return The <code>AuthorizationActions<code> common to both args or
     * <code>StandardAuthorizationActions.NONE</code> if none exist.
     */
    public static AuthorizationActions getCommonActions(int actions,
                                                        int sharedActions) {
        int commonValue = actions & sharedActions;
        AuthorizationActions commonActions =
            StandardAuthorizationActions.getAuthorizationActions(commonValue);
        return commonActions;
    }

    /**
     * Get the <code>AuthorizationActions</code> in <code>actions</code> that are <i>NOT</i> in
     * <code>sharedActions</code>. May be <code>actions</code> if there are no common actions.
     * @param actions
     * @param sharedActions
     * @return The <code>AuthorizationActions<code> in <code>actions</code> that are <i>NOT</i> in
     * <code>sharedActions</code>.
     */
    public static AuthorizationActions getIndependantActions(AuthorizationActions actions,
                                                             AuthorizationActions sharedActions) {
        int independantValue = (actions.getValue() ^ sharedActions.getValue());
        AuthorizationActions independantActions =
            StandardAuthorizationActions.getAuthorizationActions(independantValue);
        return independantActions;
    }

    /**
     * Get the <code>AuthorizationActions</code> in <code>actions</code> that are <i>NOT</i> in
     * <code>sharedActions</code>. May be <code>actions</code> if there are no common actions.
     * @param actions
     * @param sharedActions
     * @return The <code>AuthorizationActions<code> in <code>actions</code> that are <i>NOT</i> in
     * <code>sharedActions</code>.
     */
    public static AuthorizationActions getIndependantActions(int actions,
                                                             int sharedActions) {
        int independantValue = actions ^ sharedActions;
        AuthorizationActions independantActions =
            StandardAuthorizationActions.getAuthorizationActions(independantValue);
        return independantActions;
    }

    /**
     * Obtain the formatted String[] version of the given <code>actionsValue</code>.
     * @param actionsValue The set of actions to determiine labels.
     * @return The String[] of labels associated with the set of actions.
     * @throws IllegalArgumentException if the specified set of actions is
     * invalid.
     */
    public static String[] getActionsLabels(int actionsValue) {
        AuthorizationActions actions = StandardAuthorizationActions.getAuthorizationActions(actionsValue);
        String[] actionLabels = new String[] {};
        if ( actions != null ) {
            actionLabels = actions.getLabels();
        }
        return actionLabels;
    }

    /**
     * Obtain the formatted String version of the given <code>actionsValue</code>.
     * @param actionsValue The set of actions to determiine labels.
     * @return The formatted action string associated with the set of actions.
     * @throws IllegalArgumentException if the specified set of actions is
     * invalid.
     */
    public static String getActionsString(int actionsValue) {
        StringBuffer actionBuf = new StringBuffer();
        AuthorizationActions actions = StandardAuthorizationActions.getAuthorizationActions(actionsValue);
        if ( actions != null ) {
            String[] actionLables = actions.getLabels();
            actionBuf.append("{"); //$NON-NLS-1$
            for ( int i=0; i<actionLables.length; i++ ) {
                actionBuf.append(actionLables[i] + ", "); //$NON-NLS-1$
            }
            actionBuf.replace(actionBuf.length() - 2, actionBuf.length(), "}"); //$NON-NLS-1$
        }
        return actionBuf.toString();
    }

    /**
     * Return the value of this action.
     * @return the value of this action.
     */
    public int getValue() {
        return this.actions;
    }

    /**
     * Return the number of actions.
     * @return the number of actions.
     */
    public int getLabelCount() {
        return this.labelCollection.size();
    }

    /**
     * Return the label of this action.
     * @return the label of this action.
     */
    public String getLabel() {
        return this.label;
    }

    /**
     * Return the set of labels of this action.
     * @return the set of labels of this action.
     */
    public String[] getLabels() {
        return this.labels;
    }

    /**
     * Return whether this instance contains the specified label
     * @param label the single label that is to be checked
     * @return true if this instance contains the specified label, or false otherwise
     */
    public boolean containsLabel( String label ) {
        return this.labelCollection.contains( label );
    }

    /**
     * Return whether this instance contains all of the specified labels
     * @param labels the array of labels that are to be checked
     * @return true if this instance contains all of the specified labels, or false otherwise
     */
    public boolean containsLabels( String[] labels ) {
        return this.labelCollection.containsAll( Arrays.asList(labels) );
    }

    /**
     * Return whether this instance contains all of the specified labels
     * @param labels the set of labels that are to be checked
     * @return true if this instance contains all of the specified labels, or false otherwise
     */
    public boolean containsLabels(Collection labels){
        return this.labelCollection.containsAll(labels);
    }

    /**
     * Returns the stringified representation for this user ID. This is in the form <I>username@domain</I>.
     * <br>
     * @return the string representation
     */
    public String toString() {
        return this.label;
    }


    /**
     * Returns true if the specified object is semantically equal to this instance.
     * Note:  this method is consistent with <code>compareTo()</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    public boolean equals(Object obj) {
        // Check if instances are identical ...
        if (this == obj) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        if (obj instanceof StandardAuthorizationActions) {
            return compareFields((StandardAuthorizationActions)obj) == 0;
        }

        // Otherwise not comparable ...
        return false;
    }

    /**
     * Returns the hashCode for the object.
     * <p>
     * @return the hashCode for the object
     */
    public int hashCode() {
        return this.actions;
    }

    /**
     * Compares this AuthorizationActions to another Object. If the Object is a AuthorizationActions,
     * this function compares the attributes. Otherwise, it throws a ClassCastException
     * (as AuthorizationActions instances are comparable only to other AuthorizationActions instances).
     * Note: this method is consistent with <code>equals()</code>.
     * <p>
     * @param obj the authorization to compare this instance against.
     * @return a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the specified object, respectively.
     * @throws ClassCastException if the specified object's type prevents it from being compared to this AuthorizationActions.
     */
    public int compareTo(Object obj) {
        // Check if instances are identical...
        if (this == obj) {
            return 0;
        }
        if (obj == null) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0069));
        }

        // Check if object can be compared to this one...
        if (obj instanceof StandardAuthorizationActions) {
            return compareFields((StandardAuthorizationActions)obj);
        }

        // Otherwise not comparable ...
        throw new ClassCastException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0070, obj.getClass()));
    }

    /**
     * Checks if the specified authorization's actions are "implied by" this object's actions.
     * <P>
     * More specifically, this method returns true if:<p>
     * <ul>
     * <li> <i>action</i> is an instanceof StandardAuthorizationActions, and <p>
     * <li> <i>action</i> is a proper subset of this
     * object's actions
     * </ul>
     * @param that the authorization action to check against.
     * @return true if the specified authorization action is implied by this object, false if not
     */
    public boolean implies(AuthorizationActions that) {
        if (that == null || !( that instanceof StandardAuthorizationActions ) ) {
            return false;
        }

        return ( this.getValue() & that.getValue() ) == that.getValue();
    }

    /**
     * Compares this object with the specified object for order. Returns a
     * negative integer, zero, or a positive integer as this object is less
     * than, equal to, or greater than the specified object. <p>
     *
     * This method assumes that all type-checking has already been performed,
     * and compares the action portion of this object with <i>obj</i>. <p>
     *
     * @param obj the object that this instance is to be compared to.
     * @return A negative integer, zero, or a positive integer as this object
     *         is less than, equal to, or greater than the specified object
     */
    int compareFields(StandardAuthorizationActions obj) {
        return (obj.getValue() < this.getValue()) ? -1 : (obj.getValue() == this.getValue()) ? 0 : 1;
    }
}





