import java.util.ArrayList;
import java.util.Collections;

public class BinarySearchList extends ArrayList<VirtualNode> {

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
    public boolean add(VirtualNode t) {
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
     * @param hash
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
    public VirtualNode find(int hash) {
        VirtualNode node = new VirtualNode();
        node.setHash(hash);
        int index = Collections.binarySearch(this, node);

        if (index < 0)
            index = -(index + 1);
        else if (index >= size())
            index = 0;

        node = get(index);
        return node;
    }

    /**
     * @param index
     * @return node of the given index
     *
     *          Index is cached to the node, for fast access of its successor.
     */
    @Override
    public VirtualNode get(int index) {
        VirtualNode node = super.get(index);
        node.setIndex(index); // set current index in the table, for fast access to successor

        return node;
    }

    /**
     * @param node source node
     * @return the successor of the given node
     *
     *          Time Complexity O(1)
     */
    public VirtualNode next(VirtualNode node) {
        if (node.getIndex() < 0) // no index set
            return null;
        else if (node.getIndex() + 1 >= size()) // current node is the last element in list
            return get(0);
        else
            return get(node.getIndex() + 1);
    }
}
