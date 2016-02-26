package edu.ecnu.idse.TrajStore.core;


import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Created by zzg on 16-2-19.
 */
public class BPlusTree<T extends Comparable<T>, V> implements Serializable{
    private int factor;
    private static final int DEFAULT_FACTOR = 5;

    private int MIN_CHILDREN_FOR_INTERNAL;
    private int MAX_CHILDREN_FOR_INTERNAL;
    private int MIN_FOR_LEAF;
    private int MAX_FOR_LEAF;

    private Node<T, V> root = null;

    public BPlusTree() {
        this(DEFAULT_FACTOR);
    }


    public BPlusTree(int factor) {
        this.factor = factor;
        this.MIN_CHILDREN_FOR_INTERNAL = Double.valueOf(Math.ceil(1.0 * this.factor / 2)).intValue();  //上取整
        this.MAX_CHILDREN_FOR_INTERNAL = this.factor;
        this.MIN_FOR_LEAF = Double.valueOf(Math.floor(1.0 * this.factor / 2)).intValue(); //下取整
        this.MAX_FOR_LEAF = this.factor -1;
        this.root = new LeafNode<T,V>();
    }

    public void insert(T key, V value){
        if(key == null)
            throw new NullPointerException("must not be null for key");
        Node node = this.root.insert(key,value);
        if(node != null)
            this.root = node;
    }

    public V get(T key){
        return this.root.get(key);
    }

    public List<V> RangeSearch(T minValue, T maxValue){
        List<V> results = new ArrayList<V>();
        DFSSearch(root,results,minValue,maxValue);
        return results;
    }

    public void DFSSearch(Node<T,V> node, List<V> results, T minValue, T maxValue){

        T curKey = null;

        if(node instanceof LeafNode){
            LeafNode<T,V> leafNode = (LeafNode<T,V>)node;
            for(int i = 0 ;i < node.size;i++){
                curKey = (T)node.keys[i];
                if(curKey.compareTo(minValue)>=0 && curKey.compareTo(maxValue)<=0){ ///??????
                    results.add((V)leafNode.values[i]);
                }
            }
        }else{
            InternalNode<T,V> internalNode = (InternalNode<T,V>) node;
            int curSize = internalNode.size;
            if(maxValue.compareTo((T)internalNode.keys[0])<0){
                DFSSearch(internalNode.pointers[0],results,minValue,maxValue);
            }else if(minValue.compareTo((T)internalNode.keys[curSize -1]) >=0){
                DFSSearch(internalNode.pointers[curSize],results,minValue,maxValue);
            }else{
                //在其中某些节点上
                //找出大于 minValue的key 和 小于 maxValue的key
                int lowerBoundKeyIndex = 0;
                int upperBoundKeyIndex = 0;
                for(int i=0;i<node.size;i++){
                    curKey = (T) node.keys[i];
                    if(curKey.compareTo(minValue) > 0) {
                        lowerBoundKeyIndex = i;
                        break;
                    }
                }

                for(int i=node.size-1;i>=0;i--){
                    curKey = (T) node.keys[i];
                    if(curKey.compareTo(maxValue) <= 0) {
                        upperBoundKeyIndex = i;
                        break;
                    }
                }

                for(int j=lowerBoundKeyIndex;j <= upperBoundKeyIndex +1;j++){
                    DFSSearch(internalNode.pointers[j],results,minValue,maxValue);
                }
            }
        }
    }

    public void BFSTraverse(){

        System.out.println("Broad First Search");
        if(root instanceof LeafNode){
            for(int i=0;i<root.size;i++){
                System.out.println(root.keys[i]);
            }
            return;
        }
        Queue<Node<T,V>> myQueue = new LinkedList<Node<T,V>>();
        myQueue.add(root);
        Node<T,V> pnode = null;
        while(!myQueue.isEmpty()){
            pnode = myQueue.poll();
            for(int i=0;i<pnode.size;i++){
                System.out.println(pnode.keys[i]);
            }
            if(pnode instanceof InternalNode){
                InternalNode<T,V> internalNode = (InternalNode<T,V>) pnode;
                for(int i=0;i<internalNode.size+1;i++){
                    myQueue.add(internalNode.pointers[i]);
                }
            }


        }

    }

    public int height(){
        int height = 1;
        Node node = this.root;
        while(! (node instanceof LeafNode)){
            height++;
            node = ((InternalNode) node).pointers[0];
        }
        return  height;
    }

    /**
     * the abstract node definition, define the operation of leaf node and internal node.
     *
     * @author zzg
     *
     * @param <T>
     * @param <V>
     */
    abstract class Node<T extends Comparable<T>, V> implements Serializable{

        protected Node<T, V> parent;

        protected Object[] keys;

        protected int size;


        /**
         * if new parent node is created when insert the key-value, the created parent node is returned,
         * in other case, this method return null.
         *
         * @param key
         * @param value
         * @return
         */
        abstract Node<T, V> insert(T key, V value);

        abstract V get(T key);
    }


    class InternalNode<T extends Comparable<T>, V> extends Node<T, V> {
        private Node<T, V>[] pointers;

        public InternalNode() {
            this.size = 0;
            this.keys = new Object[MAX_CHILDREN_FOR_INTERNAL];
            this.pointers = new Node[MAX_CHILDREN_FOR_INTERNAL + 1];  //////////????????????????

        }

        public Node<T, V> insert(T key, V value) {
            int i = 0;
            for (; i < this.size; i++) {
                if (key.compareTo((T) this.keys[i]) < 0) break;
            }

            // insert until the leaf node
            return this.pointers[i].insert(key, value);
        }

        public V get(T key) {
            int i = 0;
            for (; i < this.size; i++) {
                if (key.compareTo((T) this.keys[i]) < 0) break;
            }

            return this.pointers[i].get(key);
        }

        private Node<T,V> insert(T key, Node<T,V> leftChild, Node<T,V> rightChild){
            if(this.size == 0){
                this.size++;
                this.pointers[0] = leftChild;
                this.pointers[1] = rightChild;
                this.keys[0] = key;
                leftChild.parent = this;
                rightChild.parent = this;
                return  this;
            }

            Object[] newKeys = new Object[MAX_CHILDREN_FOR_INTERNAL +1];
            Node[] newPointers = new Node[MAX_CHILDREN_FOR_INTERNAL +2];
            int i = 0;
            for(;i<this.size;i++){
                T curKey = (T) this.keys[i];
                if(curKey.compareTo(key) > 0)
                    break;
            }

            System.arraycopy(this.keys,0,newKeys,0,i);
            newKeys[i] = key;
            System.arraycopy(this.keys,i,newKeys,i+1,this.size - i);

            System.arraycopy(this.pointers,0,newPointers,0,i+1);
            newPointers[i+1] = rightChild;
            System.arraycopy(this.pointers,i+1,newPointers,i+2,this.size-i);
            this.size++;
            if(this.size <= MAX_CHILDREN_FOR_INTERNAL){
                System.arraycopy(newKeys,0,this.keys,0,this.size);
                System.arraycopy(newPointers,0,this.pointers,0,this.size+1);
                return null;
            }

            int m = (this.size/2);
            InternalNode<T,V> newNode = new InternalNode<T,V>();
            newNode.size= this.size-m -1;
            System.arraycopy(newKeys,m+1,newNode.keys,0, this.size - m -1);
            System.arraycopy(newPointers,m+1,newNode.pointers,0,this.size-m);

            //reset the children's parent to the new node
            for(int j=0; j<=newNode.size;j++){
                newNode.pointers[j].parent = newNode;
            }

            this.size = m;
            this.keys = new Object[MAX_CHILDREN_FOR_INTERNAL];
            this.pointers = new Node[MAX_CHILDREN_FOR_INTERNAL];
            System.arraycopy(newKeys,0,this.keys,0,m);
            System.arraycopy(newPointers,0,this.pointers,0,m+1);

            if(this.parent ==null){
                this.parent = new InternalNode<T,V>();
            }
            newNode.parent = this.parent;
            return ((InternalNode<T,V>)this.parent).insert((T)newKeys[m],this,newNode);
        }

    }

    class LeafNode<T extends Comparable<T>, V> extends Node<T, V> {
        public Object[] values;

        public LeafNode(){
            this.size = 0;
            this.keys = new Object[MAX_FOR_LEAF];
            this.values = new Object[MAX_FOR_LEAF];
            this.parent = null;
        }


        // if new parent node is created when insert the key-value, the new parent node is returned
        public Node<T,V> insert(T key, V value){
            Object[] newKeys = new Object[MAX_FOR_LEAF + 1];
            Object[] newValues = new Object[MAX_FOR_LEAF + 1];

            int i = 0;
            for(;i < this.size;i++){
                T curKey = (T) this.keys[i];
                if(curKey.compareTo(key) ==0){
                    this.values[i] = value;
                    return null;
                }
                if(curKey.compareTo(key) > 0)
                    break;
            }
            System.arraycopy(this.keys,0,newKeys,0,i);
            newKeys[i] = key;
            System.arraycopy(this.keys,i,newKeys,i+1,this.size -i);

            System.arraycopy(this.values,0,newValues,0,i);
            newValues[i] = value;
            System.arraycopy(this.values,i,newValues,i+1,this.size - i);

            this.size++;
            if(this.size <= MAX_FOR_LEAF){
                System.arraycopy(newKeys,0,this.keys,0,this.size);
                System.arraycopy(newValues,0,this.values,0,this.size);
                return null;
            }

            // the size is over limit; need split this node
            int m = this.size/2;

            this.keys = new Object[MAX_FOR_LEAF];
            this.values = new Object[MAX_FOR_LEAF];
            System.arraycopy(newKeys,0,this.keys,0,m);
            System.arraycopy(newValues,0,this.values,0,m);
            LeafNode<T,V> newNode = new LeafNode<T,V>();
            newNode.size = this.size - m;
            System.arraycopy(newKeys,m,newNode.keys,0,newNode.size);
            System.arraycopy(newValues,m,newNode.values,0,newNode.size);

            this.size = m;
            if(this.parent == null){
                this.parent = new InternalNode<T,V>();
            }
            newNode.parent = this.parent;

            //charu bing返回
            return ((InternalNode<T,V>) this.parent).insert((T)newNode.keys[0],this,newNode);
        }

        public V get(T key){
            if(this.size ==0)
                return null;
            int start = 0;
            int end = this.size;
            int middle = (start + end)>>1;
            T midKey = (T) this.keys[middle];
            while(start <end){
                midKey = (T) this.keys[middle];
                if(midKey.compareTo(key) == 0){
                    break;
                }
                if(midKey.compareTo(key) < 0){
                    end = middle - 1;
                }else {
                    start = middle + 1;
                }
                middle = (start + end)>>1;
            }

            return midKey.compareTo(key) == 0? (V)this.values[middle] : null;
        }

    }

    public static void main(String[] args){
        BPlusTree<Integer,String> myTree = new BPlusTree<Integer,String>(4);
        int max =10000;
        long start = System.currentTimeMillis();
        for (int i = 0; i<max; i++){
       //     System.out.println("add "+i);
            myTree.insert(i,String.valueOf(i));
        //    myTree.BFSTraverse();
        }
        System.out.println("B plus tree construction cost: "+(System.currentTimeMillis() - start)/1000 +" seconds.");
        System.out.println("the height of the tree "+myTree.height());
        List <String> res = myTree.RangeSearch(2001,2008);
        for(String s: res){
            System.out.println(s);
        }

        try
        {
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("my.out"));//输出流保存的文件名为 my.out ；ObjectOutputStream能把Object输出成Byte流
            oos.writeObject(myTree);
            oos.flush();  //缓冲流
            oos.close(); //关闭流
        } catch (FileNotFoundException e)
        {
            e.printStackTrace();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        ObjectInputStream oin = null;//局部变量必须要初始化
        try
        {
            oin = new ObjectInputStream(new FileInputStream("my.out"));
        } catch (FileNotFoundException e1)
        {
            e1.printStackTrace();
        } catch (IOException e1)
        {
            e1.printStackTrace();
        }
        BPlusTree mts = null;
        try {
            mts = (BPlusTree ) oin.readObject();//由Object对象向下转型为MyTest对象
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        List <String> mtsresult = myTree.RangeSearch(1,5);
        for(String s: mtsresult){
            System.out.println(s);
        }
    }
}