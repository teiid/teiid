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

package com.metamatrix.api.core.message;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Iterator;
import java.util.ListIterator;
import com.metamatrix.common.CommonPlugin;

/**
 * An ordered collection of {@link Message} instances.
 * This class implements {@link java.util.List} and thus can be used in the same
 * exact manner as a {@link java.util.List List}.  However, there are additional methods
 * on this class that provide various mechanisms for finding or removing Message instances that
 * match particular categories, such as those that apply to a specific target or
 * those with one of a set of message types.
 * <p>
 * This class contains a {@link MessageList.Statistics Statistics} object that maintains
 * by message type the counts of Message instances contained by this object.  Note that
 * these statistics do reflect changes to the MessageList object.
 * </p> 
 * 
 * @since	  3.0
 */
public class MessageList implements List {
    
    /**
     * Class that maintains statistics for the MessageList's Message instances
     * categorized by various types.  Instances of this class are automatically
     * created by MessageList and dynamically reflect any changes to its
     * MessageList.
     */
    public class Statistics {
        private int errors;    
        private int warnings;
        private int notifications;
        private int none;
        private int others;
        /**
         * No-arg constructor
         */
        protected Statistics() {
        }
        /**
         * Clear the current statistics
         */
        protected void clear() {
            errors = 0;
            warnings = 0;
            notifications = 0;
            none = 0;
            none = 0;
        }
        /**
         * Add a message to the statistics.
         * @param msg the Message to be added; if null, this method does nothing
         */
        protected void addMessage( Message msg ) {
            if ( msg != null ) {
                switch ( msg.getType() ) {
                    case MessageTypes.ERROR_MESSAGE:
                        ++errors;
                        break;
                    case MessageTypes.WARNING_MESSAGE:
                        ++warnings;
                        break;
                    case MessageTypes.NOTIFICATION_MESSAGE:
                        ++notifications;
                        break;
                    case MessageTypes.NULL_MESSAGE:
                        ++none;
                        break;
                    default:
                        ++others;
                        break;
                }
            }
        }
        /**
         * Remove a message from the statistics.
         * @param msg the Message to be removed; if null, this method does nothing
         */
        protected void removeMessage( Message msg ) {
            if ( msg != null ) {
                switch ( msg.getType() ) {
                    case MessageTypes.ERROR_MESSAGE:
                        --errors;
                        if ( errors < 0 ) {
                            errors = 0;
                        }
                        break;
                    case MessageTypes.WARNING_MESSAGE:
                        --warnings;
                        if ( warnings < 0 ) {
                            warnings = 0;
                        }
                        break;
                    case MessageTypes.NOTIFICATION_MESSAGE:
                        --notifications;
                        if ( notifications < 0 ) {
                            notifications = 0;
                        }
                        break;
                    case MessageTypes.NULL_MESSAGE:
                        --none;
                        if ( none < 0 ) {
                            none = 0;
                        }
                        break;
                    default:
                        --others;
                        if ( others < 0 ) {
                            others = 0;
                        }
                        break;
                }
            }
        }

        /**
         * Gets the number of Message instances that have a type of
         * {@link MessageTypes#ERROR_MESSAGE MessageTypes.ERROR_MESSAGE}.
         * <p>
         * Note that this value may change as Message instances are added to and removed from
         * this Statistic's backing MessageList.
         * </p>  
         * @return Returns a the number of error Message instances.
         */
        public int getErrorCount() {
            return errors;
        }

        /**
         * Gets the number of Message instances that have a type of
         * {@link MessageTypes#WARNING_MESSAGE MessageTypes.WARNING_MESSAGE}.
         * <p>
         * Note that this value may change as Message instances are added to and removed from
         * this Statistic's backing MessageList.
         * </p>  
         * @return Returns a the number of warning Message instances.
         */
        public int getWarningCount() {
            return warnings;
        }
        
        /**
         * Gets the number of Message instances that have a type of
         * {@link MessageTypes#NOTIFICATION_MESSAGE MessageTypes.NOTIFICATION_MESSAGE}.
         * <p>
         * Note that this value may change as Message instances are added to and removed from
         * this Statistic's backing MessageList.
         * </p>  
         * @return Returns a the number of notification Message instances.
         */
        public int getNotificationCount() {
            return notifications;
        }

        /**
         * Gets the number of Message instances that have a type of
         * {@link MessageTypes#NULL_MESSAGE MessageTypes.NULL_MESSAGE}
         * (e.g., no type specified).
         * <p>
         * Note that this value may change as Message instances are added to and removed from
         * this Statistic's backing MessageList.
         * </p>  
         * @return Returns a the number of Message instances that have
         * no type specified.
         */
        public int getNoneCount() {
            return none;
        }

        /**
         * Gets the number of Message instances that have a type other than
         * {@link MessageTypes#ERROR_MESSAGE        MessageTypes.ERROR_MESSAGE},
         * {@link MessageTypes#WARNING_MESSAGE      MessageTypes.WARNING_MESSAGE},
         * {@link MessageTypes#NOTIFICATION_MESSAGE MessageTypes.NOTIFICATION_MESSAGE}, or
         * {@link MessageTypes#NULL_MESSAGE         MessageTypes.NULL_MESSAGE}.
         * <p>
         * Note that this value may change as Message instances are added to and removed from
         * this Statistic's backing MessageList.
         * </p>  
         * @return Returns a the number of Message instances that are
         * not errors, warnings, notifications or that have no types.
         */
        public int getOtherCount() {
            return others;
        }

        /**
         * Gets the total number of Message instances in the backing
         * MessageList instance.  This method returns exactly
         * the same value as {@link MessageList#size()}.
         * <p>
         * Note that this value may change as Message instances are added to and removed from
         * this Statistic's backing MessageList.
         * </p>  
         * @return Returns a the total number of error Message instances.
         */
        public int getTotalCount() {
            return MessageList.this.size();
        }
        
        /**
         * Return the string representation of this object.  This method returns a string
         * of the form:
         * <p>
         * <code>[ne] errors, [nw] warnings, [nn] notifications, [nt] no-type, [no] others, [sum] total</code>
         * </p>
         * <p>
         * where
         * <ul>
         *  <li><code>[ne]</code> represents the number of messages that have an error type</li>
         *  <li><code>[nw]</code> represents the number of messages that have a warning type</li>
         *  <li><code>[nn]</code> represents the number of messages that have a notification type</li>
         *  <li><code>[nt]</code> represents the number of messages that have no type</li>
         *  <li><code>[no]</code> represents the number of messages that have other types</li>
         *  <li><code>[sum]</code> represents the number of messages</li>
         * </ul>
         * </p>
         * @return the string representation
         */
        public String toString() {
            return "" + errors + " errors, " +    //$NON-NLS-1$ //$NON-NLS-2$
                        warnings + " warnings, " +     //$NON-NLS-1$
                        notifications + " notifications, " +     //$NON-NLS-1$
                        none + " no-type, " +     //$NON-NLS-1$
                        others + " others, " +     //$NON-NLS-1$
                        getTotalCount() + " total";     //$NON-NLS-1$
        }
    }


    private List messages;
    private Statistics stats;
    
    /**
     * Construct an object with no Message instances.
     */
    public MessageList() {
        this.messages = new LinkedList();
        this.stats = new Statistics();    
    }

    /**
     * Construct an object and add all of the Message instances in the specified collection.
     * @param messages the collection of Message instances that this object is to contain; if
     * null, this object will be empty.
     */
    public MessageList(Collection messages) {
        this();
        this.addAll(messages);    
    }

    /**
     * Return the Statistics instance that contains a breakdown by MessageType
     * of the number of Message instances in this object.  The returned
     * object is updated automatically whenever this MessageList is changed.
     * @return the statistics object for this MessageList; never null
     */ 
    public MessageList.Statistics getStatistics() {
        return this.stats;
    }

    /**
     * Remove all Message instances that have the specified target
     * @param messageTarget the target for which messages are to be removed from this container.
     * @return true if at least one message was removed, or false if no messages were removed
     */
    public boolean removeByTarget(Object messageTarget) {
        return removeMessagesWithTarget(this.messages,messageTarget);
    }

    /**
     * Remove all Message instances that are instances of the specified Class.
     * @param messageClass the Class whose instances are to be removed from this list.
     * @return true if at least one message was removed, or false if no messages were removed
     */
    public boolean removeByClass( Class messageClass ) {
        boolean removedAtLeastOne = false;
        if ( messageClass != null ) {
            Iterator iter = iterator();
            while (iter.hasNext()) {
                Message msg = (Message)iter.next();
                if ( msg != null && messageClass.isAssignableFrom(msg.getClass()) ) {
                    iter.remove();
                    removedAtLeastOne = true;   
                    messageRemoved(msg);
                }
            }
        }
        return removedAtLeastOne;
    }

    /**
     * Remove all Message instances that have the specified type
     * @param messageType the type for which messages are to be removed from this container.
     * @return true if at least one message was removed, or false if no messages were removed
     */
    public boolean removeByType( int messageType ) {
        boolean removedAtLeastOne = false;
        Iterator iter = iterator();
        while (iter.hasNext()) {
            Message msg = (Message)iter.next();
            if ( matchesType(msg,messageType)) {
                iter.remove();
                removedAtLeastOne = true; 
                messageRemoved(msg);               
            }
        }
        return removedAtLeastOne;
    }

    /**
     * Remove all Message instances that have the specified type
     * @param messageTypes the types for which messages are to be removed from this container.
     * @return true if at least one message was removed, or false if no messages were removed
     */
    public boolean removeByType( int[] messageTypes ) {
        boolean removedAtLeastOne = false;
        Iterator iter = iterator();
        while (iter.hasNext()) {
            Message msg = (Message)iter.next();
            if ( matchesType(msg,messageTypes)) {
                iter.remove();
                removedAtLeastOne = true;
                messageRemoved(msg);                 
            }
        }
        return removedAtLeastOne;
    }

    /**
     * Return an iterator over the Message instances that have the specified Object
     * as the message target.
     * @param target the message target
     * @return iterator the iterator that will expose only Message instances
     * that have the supplied target
     */
    public Iterator iteratorByTarget( Object target) {
        return new MessageTargetIterator(target,this.messages.listIterator());
    }

    /**
     * Return an iterator over the Message instances that have the specified message type.
     * @param messageType the message type
     * @return iterator the iterator that will expose only Message instances
     * that have the supplied type
     */
    public Iterator iteratorByType( int messageType ) {
        return iteratorByType(new int[]{messageType});
    }

    /**
     * Return an iterator over the Message instances that have a message type
     * included in the provided set.
     * @param messageTypes the array of message types over which this iterator
     * is to expose
     * @return iterator the iterator that will expose only Message instances
     * that have a type in the supplied array
     */
    public Iterator iteratorByType( int[] messageTypes ) {
        return new MessageTypeIterator(messageTypes,this.messages.listIterator());
    }

    /**
     * Returns a sublist of the Message instances that have the appropriate
     * message types.
     * @param messageTarget the message target
     * @return the sublist that is to contain all messages in this MessageList that have
     * the supplied target object
     */
    public List subListByTarget(Object messageTarget) {
        List result = new MessageList(this);
        removeMessagesWithTarget(result,messageTarget);
        return result;
    }

    /**
     * Returns a sublist of the Message instances that have the appropriate
     * message types.
     * @param messageType the message type
     * @return the sublist that is to contain all messages in this MessageList that have
     * the supplied type
     */
    public List subListByType(int messageType) {
        List result = new MessageList(this);
        Iterator iter = result.iterator();
        while (iter.hasNext()) {
            if ( !matchesType((Message)iter.next(),messageType)) {
                iter.remove();                
            }
        }
        return result;
    }

    /**
     * Returns a sublist of the Message instances that have the appropriate
     * message types.
     * @param messageTypes the array of message types
     * @return the sublist that is to contain all messages in this MessageList that have
     * a type that is in the supplied array
     */
    public List subListByType(int[] messageTypes) {
        List result = new MessageList(this);
        Iterator iter = result.iterator();
        while (iter.hasNext()) {
            if ( !matchesType((Message)iter.next(),messageTypes)) {
                iter.remove();                
            }
        }
        return result;
    }

    /**
     * Helper method to remove all instances from the List of Message objects where the
     * message target matches (using {@link #matchesTarget(Message, Object)}) the supplied
     * target.
     * @param messages the list of messages; may not be null
     * @param target the target
     * @return true if at least one message was removed from the list because its target
     * matched the supplied target
     */
    protected static boolean removeMessagesWithTarget( List messages, Object target ) {
        boolean removedAtLeastOne = false;
        Iterator iter = messages.iterator();
        while (iter.hasNext()) {
            Message msg = (Message)iter.next();
            if ( !matchesTarget(msg,target)) {
                iter.remove();
                removedAtLeastOne = true;
            }
        }
        return removedAtLeastOne;
    }

    /**
     * Helper method to determine whether the supplied message has a target that
     * is exactly the same as the supplied object.
     * @param msg the message
     * @param target the target
     * @return true if the message's target is the same object as the supplied target
     * (using the == operator), or false otherwise.
     */
    protected static boolean matchesTarget( Message msg, Object target ) {
        if ( msg != null ) {
            return ( msg.getTarget() == target );
        }
        return false;    
    }

    /**
     * Helper method to determine whether the supplied message has a type that
     * is exactly the same as one of the the supplied types.
     * @param msg the list of messages; may not be null
     * @param messageTypes the types
     * @return true if at the message's type is the same as one of the supplied types,
     * or false otherwise
     */
    protected static boolean matchesType( Message msg, int[] messageTypes ) {
        if ( msg != null && messageTypes != null ) {
            for (int i=0;i!=messageTypes.length;++i) {
                if ( msg.getType() == messageTypes[i] ) {
                    return true;    
                }    
            }    
        }
        return false;    
    }

    /**
     * Helper method to determine whether the supplied message has a type that
     * is exactly the same object as the supplied type.
     * @param msg the list of messages; may not be null
     * @param messageType the type
     * @return true if at the message's type is the same as the supplied type,
     * or false otherwise
     */
    protected static boolean matchesType( Message msg, int messageType ) {
        if ( msg != null ) {
            return ( msg.getType() == messageType );
        }
        return false;    
    }
    
    /**
     * Helper method to verify that the object is an instance of {@link Message}.
     * @param obj the Object that is to be verified
     * @throws IllegalArgumentException if the object is not null and the object
     * is not an instance of {@link Message}
     */
    protected static void verifyMessage( Object obj ) {
        if ( obj != null && !(obj instanceof Message ) ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString("MessageList.Only_Message_in_MessageList"));     //$NON-NLS-1$
        }    
    }

    // --------------------------------------------------------------------------------
    //                     L I S T       I M P L E M E N T A T I O N
    // --------------------------------------------------------------------------------

    /**
     * Overrides the implementation of {@link java.util.List}
     * @param index the index
     * @param message the message
     * @see List#add(int, Object)
     * @throws IllegalArgumentException if the messsage is not an instance of {@link Message}
     */
    public void add(int index, Object message) {
        verifyMessage(message);
        this.messages.add(index,message);
        this.stats.addMessage((Message)message);
        messageAdded((Message)message);
    }

    /**
     * Overrides the implementation of {@link java.util.List}
     * @param message the message
     * @return true if the message was added
     * @see Collection#add(Object)
     * @throws IllegalArgumentException if the messsage is not an instance of {@link Message}
     */
    public boolean add(Object message) {
        verifyMessage(message);
        if ( this.messages.add(message) ) {
            this.stats.addMessage((Message)message);
            messageAdded((Message)message);
            return true;
        }
        return false;
    }

    /**
     * Overrides the implementation of {@link java.util.List}
     * @param messages the messages to be added
     * @return true if all of the messages were added
     * @see Collection#addAll(Collection)
     * @throws IllegalArgumentException if the collection contains instances other than
     * {@link Message}
     */
    public boolean addAll(Collection messages) {
        if ( messages != null ) {
            List addedMsgs = new LinkedList();
            Iterator iter = messages.iterator();
            while (iter.hasNext()) {
                Object obj = iter.next();
                if ( obj instanceof Message ) {
                    this.messages.add(obj);
                    addedMsgs.add(obj);
                    this.stats.addMessage((Message)obj);
                    messageAdded((Message)obj);
                } else {
                    Iterator removeIter = addedMsgs.iterator();
                    while ( removeIter.hasNext() ) {
                        this.stats.removeMessage((Message)removeIter.next());    
                    }
                    throw new IllegalArgumentException(CommonPlugin.Util.getString("MessageList.Only_Message_in_MessageList"));     //$NON-NLS-1$
                }
            }
            return true;   
        } 
        return false;
    }

    /**
     * Overrides the implementation of {@link java.util.List}
     * @param index the index
     * @param messages the messages to be added
     * @return true if at least one message was added
     * @see List#addAll(int, Collection)
     * @throws IllegalArgumentException if the collection contains instances other than
     * {@link Message}
     */
    public boolean addAll(int index, Collection messages) {
        if ( messages != null ) {
            int addIndex = index;
            List addedMsgs = new LinkedList();
            Iterator iter = messages.iterator();
            while (iter.hasNext()) {
                Object obj = iter.next();
                if ( obj instanceof Message ) {
                    this.messages.add(addIndex,obj);
                    addedMsgs.add(obj);
                    this.stats.addMessage((Message)obj);
                    messageAdded((Message)obj);
                    ++addIndex;
                } else {
                    Iterator removeIter = addedMsgs.iterator();
                    while ( removeIter.hasNext() ) {
                        this.stats.removeMessage((Message)removeIter.next());    
                    }
                    throw new IllegalArgumentException(CommonPlugin.Util.getString("MessageList.Only_Message_in_MessageList"));     //$NON-NLS-1$
                }
            }
            return true;   
        } 
        return false;
    }

    /**
     * Overrides the implementation of {@link java.util.List}
     * @see Collection#clear()
     */
    public void clear() {
        this.messages.clear();
        this.stats.clear();
        messagesCleared();
    }

    /**
     * Overrides the implementation of {@link java.util.List}
     * @param message the message
     * @return true if this List contains the specified Message
     * @see Collection#contains(Object)
     */
    public boolean contains(Object message) {
        return this.messages.contains(message);
    }

    /**
     * Overrides the implementation of {@link java.util.List}
     * @param messages the messages
     * @return true if this List contains all of the specified Messages
     * @see Collection#containsAll(Collection)
     */
    public boolean containsAll(Collection messages) {
        return this.messages.containsAll(messages);
    }

    /**
     * Overrides the implementation of {@link java.util.List}
     * @param index the index at which the Message instance is to be returned
     * @return the Message object at the supplied index
     * @see List#get(int)
     */
    public Object get(int index) {
        return this.messages.get(index);
    }

    /**
     * Overrides the implementation of {@link java.util.List}
     * @param message the message
     * @return the index of the supplied message in this List
     * @see List#indexOf(Object)
     */
    public int indexOf(Object message) {
        return this.messages.indexOf(message);
    }

    /**
     * Overrides the implementation of {@link java.util.List}
     * @return true if this List contains no Message instances.
     * @see Collection#isEmpty()
     */
    public boolean isEmpty() {
        return this.messages.isEmpty();
    }

    /**
     * Overrides the implementation of {@link java.util.List}
     * @return the iterator
     * @see Collection#iterator()
     */
    public Iterator iterator() {
        return new MessageListIterator(this.messages.listIterator());
    }

    /**
     * Overrides the implementation of {@link java.util.List}
     * @param message the Message object
     * @return the last index at which the supplied Message exists in this List
     * @see List#lastIndexOf(Object)
     */
    public int lastIndexOf(Object message) {
        return this.messages.lastIndexOf(message);
    }

    /**
     * Overrides the implementation of {@link java.util.List}
     * @return the iterator
     * @see List#listIterator()
     */
    public ListIterator listIterator() {
        return new MessageListIterator(this.messages.listIterator());
    }

    /**
     * Overrides the implementation of {@link java.util.List}
     * @param index the index at which the iterator is to begin
     * @return the iterator
     * @see List#listIterator(int)
     */
    public ListIterator listIterator(int index) {
        return new MessageListIterator(this.messages.listIterator(index));
    }
    
    /**
     * Can be overridden by subclasses to do specific work on a message when it 
     * is being removed.  This implementation does nothing.
     * @param message the message being removed 
     */
    protected void messageRemoved(Message message){
    } 
    
    /**
     * Can be overridden by subclasses to do specific work on a message when it 
     * is being added.  This implementation does nothing.
     * @param message the message being added 
     */
    protected void messageAdded(Message message){
    } 
    
    /**
     * Can be overridden by subclasses to do specific work when this list is 
     * being cleared.  This implementation does nothing.
     */
    protected void messagesCleared(){
    } 

    /**
     * Overrides the implementation of {@link java.util.List}
     * @param index the index
     * @return the Message that was at the supplied index
     * @see List#remove(int)
     */
    public Object remove(int index) {
        Object result = this.messages.remove(index);
        this.stats.removeMessage((Message)result);
        messageRemoved((Message)result);
        return result;
    }

    /**
     * Overrides the implementation of {@link java.util.List}
     * @param message the Message to be removed
     * @return true if the Message was removed from this List, or false otherwise
     * @see Collection#remove(Object)
     */
    public boolean remove(Object message) {
        if ( this.messages.remove(message) ) {
            this.stats.removeMessage((Message)message);
            messageRemoved((Message)message);
            return true;
        }
        return false;
    }

    /**
     * Overrides the implementation of {@link java.util.List}
     * @param messages the Message instances to be removed
     * @return true if at least one Message was removed from this List, or false otherwise
     * @see Collection#removeAll(Collection)
     */
    public boolean removeAll(Collection messages) {
        boolean modified = false;
        Iterator iter = this.iterator();
        while (iter.hasNext()) {
            Message msg = (Message)iter.next();
            if(messages.contains(msg)) {
                iter.remove();
                modified = true;
                messageRemoved(msg);
            }
        } 
        
        return modified;
    }

    /**
     * Overrides the implementation of {@link java.util.List}
     * @param messages the Message instances to be removed
     * @return true if at least one Message was removed from this List, or false otherwise
     * @see Collection#retainAll(Collection)
     */
    public boolean retainAll(Collection messages) {
        boolean modified = false;
        Iterator iter = iterator();
        while (iter.hasNext()) {
            Message msg = (Message)iter.next();
            if(!messages.contains(msg)) {
                iter.remove();
                modified = true;
                messageRemoved(msg);
            }
        }
        
        return modified;
    }

    /**
     * Overrides the implementation of {@link java.util.List}
     * @param index the index
     * @param message the Message
     * @return the Message that was previously at the index
     * @see List#set(int, Object)
     */
    public Object set(int index, Object message) {
        verifyMessage(message);
        Message previous = (Message) this.messages.set(index,message);
        this.stats.removeMessage(previous);
        this.stats.addMessage((Message)message);
        return previous;
    }

    /**
     * Overrides the implementation of {@link java.util.List}
     * @return the number of messages currently in this List
     * @see Collection#size()
     */
    public int size() {
        return this.messages.size();
    }

    /**
     * Overrides the implementation of {@link java.util.List}
     * @param fromIndex the starting index (inclusive)
     * @param toIndex the ending index (exclusive)
     * @return the sublist
     * @see List#subList(int, int)
     */
    public List subList(int fromIndex, int toIndex) {
        return this.messages.subList(fromIndex,toIndex);
    }

    /**
     * Returns an array containing all of the elements in this list in proper
     * sequence.  Obeys the general contract of the
     * <tt>Collection.toArray</tt> method.
     *
     * @return an array containing all of the elements in this list in proper
     *         sequence.
     * @see Collection#toArray()
     */
    public Object[] toArray() {
        return this.messages.toArray();
    }

    /**
     * Returns an array containing all of the elements in this list in proper
     * sequence; the runtime type of the returned array is that of the
     * specified array.  Obeys the general contract of the
     * <tt>Collection.toArray(Object[])</tt> method.
     *
     * @param a the array into which the elements of this list are to
     *      be stored, if it is big enough; otherwise, a new array of the
     *      same runtime type is allocated for this purpose.
     * @return  an array containing the elements of this list.
     * 
     * @throws ArrayStoreException if the runtime type of the specified array
     *        is not a supertype of the runtime type of every element in
     *        this list.
     * @see Collection#toArray(Object[])
     */
    public Object[] toArray(Object[] a) {
        return this.messages.toArray(a);
    }

    // --------------------------------------------------------------------------------
    //                     N E S T E D   C L A S S E S
    // --------------------------------------------------------------------------------

    private class MessageListIterator implements ListIterator {
        private final ListIterator iter;
        private Message current;
        MessageListIterator( ListIterator iter ) {
            this.iter = iter;
        }
        
        /**
         * @see Iterator#hasNext()
         */
        public boolean hasNext() {
            return this.iter.hasNext();
        }

        /**
         * @see Iterator#next()
         */
        public Object next() {
            this.current = (Message) this.iter.next();
            return this.current;
        }

        /**
         * @see Iterator#remove()
         */
        public void remove() {
            if ( this.current != null ) {
                this.iter.remove();
                MessageList.this.stats.removeMessage(this.current); 
            }
        }
        
        /**
         * @see ListIterator#add(Object)
         */
        public void add(Object o) {
            MessageList.verifyMessage(o);
            this.iter.add(o);
            MessageList.this.stats.addMessage(this.current); 
        }

        /**
         * @see ListIterator#hasPrevious()
         */
        public boolean hasPrevious() {
            return this.iter.hasPrevious();
        }

        /**
         * @see ListIterator#nextIndex()
         */
        public int nextIndex() {
            return this.iter.nextIndex();
        }

        /**
         * @see ListIterator#previous()
         */
        public Object previous() {
            this.current = (Message) this.iter.previous();
            return this.current;
        }

        /**
         * @see ListIterator#previousIndex()
         */
        public int previousIndex() {
            return this.iter.previousIndex();
        }

        /**
         * @see ListIterator#set(Object)
         */
        public void set(Object o) {
            MessageList.verifyMessage(o);
            if ( this.current != null ) {
                this.iter.set(o);
                MessageList.this.stats.removeMessage(this.current);
                this.current = (Message) o; 
                MessageList.this.stats.addMessage(this.current); 
            }
        }

    }
    
    private abstract class MessageSubsetIterator extends MessageListIterator {
        MessageSubsetIterator( ListIterator iter ) {
            super(iter);
        }
        
        protected void setToNextMatchingMessage() {
            // Find the next message with matching types ...
            boolean foundNext = false;
            Message msg = null;
            while ( super.hasNext() ) {
                msg = (Message) super.next();
                if ( matchesCriteria(msg) ) {
                    foundNext = true;
                    break;    
                }    
            }
            // If we found one, then back the iterator up so that it's next() is our next()
            if ( foundNext && super.hasPrevious() ) {
                super.previous();    
            }
        }
        
        protected abstract boolean matchesCriteria( Message msg );

        /**
         * @see Iterator#hasNext()
         */
        public boolean hasNext() {
            setToNextMatchingMessage();
            return super.hasNext();
        }

        /**
         * @see Iterator#next()
         */
        public Object next() {
            setToNextMatchingMessage();
            return super.next();
        }
    }
    
    private class MessageTypeIterator extends MessageSubsetIterator {
        private final int[] messageTypes;
        MessageTypeIterator( int[] messageTypes, ListIterator iter ) {
            super(iter);
            int length = messageTypes.length;
            this.messageTypes = new int[length];
            for (int i=0;i!=length;++i) {
                this.messageTypes[i] = messageTypes[i];    
            }    
        }
        
        protected boolean matchesCriteria( Message msg ) {
            return MessageList.matchesType(msg,this.messageTypes);
        }
    }

    private class MessageTargetIterator extends MessageSubsetIterator {
        private final Object messageTarget;
        MessageTargetIterator( Object messageTarget, ListIterator iter ) {
            super(iter);
            this.messageTarget = messageTarget;
        }
        
        protected boolean matchesCriteria( Message msg ) {
            return MessageList.matchesTarget(msg,this.messageTarget);
        }
    }

}

