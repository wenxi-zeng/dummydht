/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gossip.crdt;

import org.apache.gossip.crdt.SharedMessageOrSet.Builder.Operation;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.BiConsumer;

/*
 * A immutable set
 */
public class SharedMessageOrSet implements CrdtAddRemoveSet<SharedMessage, Set<SharedMessage>, SharedMessageOrSet> {

    private final Map<SharedMessage, Set<UUID>> elements = new HashMap<>();
    private final Map<SharedMessage, Set<UUID>> tombstones = new HashMap<>();
    private final transient Set<SharedMessage> val;

    public SharedMessageOrSet(){
        val = computeValue();
    }

    SharedMessageOrSet(Map<SharedMessage, Set<UUID>> elements, Map<SharedMessage, Set<UUID>> tombstones){
        this.elements.putAll(elements);
        this.tombstones.putAll(tombstones);
        val = computeValue();
    }

    @SafeVarargs
    public SharedMessageOrSet(SharedMessage ... elements){
        this(new HashSet<>(Arrays.asList(elements)));
    }

    public SharedMessageOrSet(Set<SharedMessage> elements) {
        for (SharedMessage e: elements){
            internalAdd(e);
        }
        val = computeValue();
    }

    public SharedMessageOrSet(Builder<SharedMessage>builder){
        for (Builder<SharedMessage>.OrSetElement<SharedMessage> e: builder.elements){
            if (e.operation == Operation.ADD){
                internalAdd(e.element);
            } else {
                internalRemove(e.element);
            }
        }
        val = computeValue();
    }

    /**
     * This constructor is the way to remove elements from an existing set
     * @param set
     * @param builder
     */
    public SharedMessageOrSet(SharedMessageOrSet set, Builder<SharedMessage> builder){
        elements.putAll(set.elements);
        tombstones.putAll(set.tombstones);
        for (Builder<SharedMessage>.OrSetElement<SharedMessage> e: builder.elements){
            if (e.operation == Operation.ADD){
                internalAdd(e.element);
            } else {
                internalRemove(e.element);
            }
        }
        val = computeValue();
    }

    static Set<UUID> mergeSets(Set<UUID> a, Set<UUID> b) {
        if ((a == null || a.isEmpty()) && (b == null || b.isEmpty())) {
            return null;
        }
        Set<UUID> res = new HashSet<>(a);
        res.addAll(b);
        return res;
    }

    private void internalSetMerge(Map<SharedMessage, Set<UUID>> map, SharedMessage key, Set<UUID> value) {
        if (value == null) {
            return;
        }
        Set<UUID> backup = map.get(key);
        map.remove(key);
        map.put(key, backup);
        map.merge(key, value, SharedMessageOrSet::mergeSets);
    }

    public SharedMessageOrSet(SharedMessageOrSet left, SharedMessageOrSet right){
        BiConsumer<Map<SharedMessage, Set<UUID>>, Map<SharedMessage, Set<UUID>>> internalMerge = (items, other) -> {
            for (Entry<SharedMessage, Set<UUID>> l : other.entrySet()){
                internalSetMerge(items, l.getKey(), l.getValue());
            }
        };

        internalMerge.accept(elements, left.elements);
        internalMerge.accept(elements, right.elements);
        internalMerge.accept(tombstones, left.tombstones);
        internalMerge.accept(tombstones, right.tombstones);

        val = computeValue();
    }

    public SharedMessageOrSet add(SharedMessage e) {
        return this.merge(new SharedMessageOrSet(e));
    }

    public SharedMessageOrSet remove(SharedMessage e) {
        return new SharedMessageOrSet(this, new Builder<SharedMessage>().remove(e));
    }

    public Builder<SharedMessage> builder(){
        return new Builder<>();
    }

    @Override
    public SharedMessageOrSet merge(SharedMessageOrSet other) {
        return new SharedMessageOrSet(this, other);
    }

    private void internalAdd(SharedMessage element) {
        Set<UUID> toMerge = new HashSet<>();
        toMerge.add(UUID.randomUUID());
        internalSetMerge(elements, element, toMerge);
    }

    private void internalRemove(SharedMessage element){
        internalSetMerge(tombstones, element, elements.get(element));
    }

    /*
     * Computes the live values by analyzing the elements and tombstones
     */
    private Set<SharedMessage> computeValue(){
        Set<SharedMessage> values = new HashSet<>();
        for (Entry<SharedMessage, Set<UUID>> entry: elements.entrySet()){
            Set<UUID> deleteIds = tombstones.get(entry.getKey());
            // if not all tokens for current element are in tombstones
            if (deleteIds == null || !deleteIds.containsAll(entry.getValue())) {
                values.add(entry.getKey());
            }
        }
        return values;
    }

    @Override
    public Set<SharedMessage> value() {
        return val;
    }

    @Override
    public SharedMessageOrSet optimize() {
        return this;
    }

    public static class Builder<SharedMessage> {
        public static enum Operation {
            ADD, REMOVE
        };

        private class OrSetElement<EL> {
            EL element;
            Operation operation;

            private OrSetElement(EL element, Operation operation) {
                this.element = element;
                this.operation = operation;
            }
        }

        private List<OrSetElement<SharedMessage>> elements = new ArrayList<>();

        public Builder<SharedMessage> add(SharedMessage element) {
            elements.add(new OrSetElement<SharedMessage>(element, Operation.ADD));
            return this;
        }

        public Builder<SharedMessage> remove(SharedMessage element) {
            elements.add(new OrSetElement<SharedMessage>(element, Operation.REMOVE));
            return this;
        }

        public Builder<SharedMessage> mutate(SharedMessage element, Operation operation) {
            elements.add(new OrSetElement<SharedMessage>(element, operation));
            return this;
        }
    }


    public int size() {
        return value().size();
    }


    public boolean isEmpty() {
        return value().size() == 0;
    }


    public boolean contains(Object o) {
        return value().contains(o);
    }


    public Iterator<SharedMessage> iterator() {
        Iterator<SharedMessage> managed = value().iterator();
        return new Iterator<SharedMessage>() {

            @Override
            public void remove() {
                throw new IllegalArgumentException();
            }

            @Override
            public boolean hasNext() {
                return managed.hasNext();
            }

            @Override
            public SharedMessage next() {
                return managed.next();
            }

        };
    }

    public Object[] toArray() {
        return value().toArray();
    }

    public <T> T[] toArray(T[] a) {
        return value().toArray(a);
    }

    public boolean containsAll(Collection<?> c) {
        return this.value().containsAll(c);
    }

    public boolean addAll(Collection<? extends SharedMessage> c) {
        throw new IllegalArgumentException();
    }

    public boolean retainAll(Collection<?> c) {
        throw new IllegalArgumentException();
    }

    public boolean removeAll(Collection<?> c) {
        throw new IllegalArgumentException();
    }

    public void clear() {
        throw new IllegalArgumentException();
    }

    @Override
    public String toString() {
        return "SharedMessageOrSet [elements=" + elements + ", tombstones=" + tombstones + "]" ;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((value() == null) ? 0 : value().hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        @SuppressWarnings("rawtypes")
        SharedMessageOrSet other = (SharedMessageOrSet) obj;
        if (elements == null) {
            if (other.elements != null)
                return false;
        } else if (!value().equals(other.value()))
            return false;
        return true;
    }

    public Map<SharedMessage, Set<UUID>> getElements() {
        return elements;
    }

    public Map<SharedMessage, Set<UUID>> getTombstones() {
        return tombstones;
    }

}
