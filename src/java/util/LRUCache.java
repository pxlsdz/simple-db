package util;
import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LRUCache {
    public static class Node {
        PageId pid;
        Page page;
        Node left;
        Node right;
        public Node(PageId pid, Page page){
            this.pid = pid;
            this.page = page;
        }
    }
    private final int maxSize;
    private Map<PageId, Node> posMap;
    private Node L, R;
    public LRUCache(int maxSize){
        L = new Node(null, null);
        R = new Node(null, null);
        L.right = R;
        R.left = L;
        posMap = new ConcurrentHashMap<>();
        this.maxSize = maxSize;
    }
    private void addHead(Node node){
        Node t = L.right;
        L.right = node;
        node.left = L;
        node.right = t;
        t.left = node;
    }
    private void removeNode(Node node){
        node.left.right = node.right;
        node.right.left = node.left;
    }
    private void removeTail(){
        R.left.left.right = R;
        R.left = R.left.left;
    }
    public void put(PageId pid, Page page){
        if(posMap.containsKey(pid)){
            Node node = posMap.get(pid);
            node.pid = pid;
            node.page = page;
            removeNode(node);
            addHead(node);
            posMap.put(pid, node);
        } else {
            if(posMap.size() >= maxSize){
                posMap.remove(R.left.pid);
                removeTail();
            }
            Node node = new Node(pid, page);
            addHead(node);
            posMap.put(pid, node);
        }
    }
    public Page get(PageId pid){
        if(posMap.containsKey(pid)){
            Node node = posMap.get(pid);
            removeNode(node);
            addHead(node);
            return node.page;
        }
        return null;
    }
    public boolean containsKey(PageId pid){
        return posMap.containsKey(pid);
    }
}
