package commonmodels;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;

public class BinarySearchList extends ArrayList<Indexable> implements Serializable {

    /**
     * @param t Node to be added
     * @return false if virtual node with the same hash is already in list
     *          true if insertion succeed
     *
     *          This function first uses binary search {@see Collections.binarySearch} to locate where the new
     *          node should be added to.
     *
     *          Collections.binarySearch returns non-negative index if the node is found.
     *          Otherwise, returns -(insertion point) - 1.
     *
     *          Time Complexity O(log n)
     */
    @Override
    public boolean add(Indexable t) {
        int index = Collections.binarySearch(this, t);

        if (index >= 0) {
            // virtual node is already in the list
            return false;
        }
        else {
            index = -(index + 1);
            this.add(index, t);

            return true;
        }
    }

    /**
     * @param node dummy node with hash
     * @return the index where the hash is hosted.
     *
     *          This function uses binary search {@see Collections.binarySearch} to locate the
     *          host node of the given hash.
     *
     *          Collections.binarySearch returns non-negative index if the node is found.
     *          Otherwise, returns -(insertion point) - 1.
     *
     *          If the index is negative, -(index + 1) is the index of host node.
     *          If the index is greater than the size of list, the host is the first node in list.
     *          Otherwise, the index is the actual index of the host
     *
     *
     *          Time Complexity O(log n)
     */
    public Indexable findIndex(Indexable node) {
        int index = Collections.binarySearch(this, node);

        if (index < 0)
            index = -(index + 1);
        else if (index >= size())
            index = 0;

        node.setIndex(get(index).getIndex());
        return node;
    }

    /**
     * @param node dummy node with hash
     * @return the node where the hash is hosted.
     *
     *          This function uses binary search {@see Collections.binarySearch} to locate the
     *          host node of the given hash.
     *
     *          Collections.binarySearch returns non-negative index if the node is found.
     *          Otherwise, returns -(insertion point) - 1.
     *
     *          If the index is negative, -(index + 1) is the index of host node.
     *          If the index is greater than the size of list, the host is the first node in list.
     *          Otherwise, the index is the actual index of the host
     *
     *
     *          Time Complexity O(log n)
     */
    public Indexable findNode(Indexable node) {
        int index = Collections.binarySearch(this, node);

        if (index < 0)
            index = -(index + 1);
        else if (index >= size())
            index = 0;

        return get(index);
    }

    /**
     * @param index
     * @return node of the given index
     *
     *          Index is cached to the node, for fast access of its successor.
     */
    @Override
    public Indexable get(int index) {
        if (index < 0)
            index = size() + index;
        else if (index >= size())
            index = index % size();

        Indexable node = super.get(index);
        node.setIndex(index); // set current index in the table, for fast access to successor and predecessor

        return node;
    }

    /**
     * @param node source node
     * @return the successor of the given node.
     *          make sure you used find() or get(),
     *          before calling this method. otherwise,
     *          the returned index would be incorrect.
     *
     *          Time Complexity O(1)
     */
    public Indexable next(Indexable node) {
        if (node.getIndex() < 0) // no index cache, did you call find() or get()?
            return null;
        else if (node.getIndex() + 1 >= size()) // current node is the last element in list
            return get(0);
        else
            return get(node.getIndex() + 1);
    }

    /**
     * @param node source node
     * @return the predecessor of the given node
     *          make sure you used find() or get(),
     *          before calling this method. otherwise,
     *          the returned index would be incorrect.
     *
     *          Time Complexity O(1)
     */
    public Indexable pre(Indexable node) {
        if (node.getIndex() < 0) // no index cache, call find()
            node = findNode(node);

        if (node.getIndex() < 0) // still not found? maybe it's already removed
            return null;
        else if (node.getIndex() == 0) // current node is the first element in list
            return get(size() - 1);
        else
            return get(node.getIndex() - 1);
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();

        for (int i = 0; i < size(); i++) {
            result.append(get(i).toString()).append('\n');
        }
        return result.toString();
    }
}
