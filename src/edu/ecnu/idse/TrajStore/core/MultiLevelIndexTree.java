package edu.ecnu.idse.TrajStore.core;

import java.io.*;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by zzg on 16-2-21.
 */
public class MultiLevelIndexTree implements Serializable {
    private int HASH_NUM;
    private static int DEFAULT_HASH_NUM =3;

    private static int Qurd = 4;
    public MLITInternalNode root = null;

    public MultiLevelIndexTree(int hash_num, CellInfo cellInfo){
        HASH_NUM = hash_num;
        this.root = new MLITInternalNode();
        this.root.setInfo(cellInfo);
        this.root.creatChird();
    }


   /* public void addNode(CellInfo info){
        MLITNode node = this.root.insert(info);
        if(node != null){
            this.root = node;
        }
    }*/

    public void insertCell(CellInfo info){
        this.root.insert(info);
    }

    public void addNode(CellInfo info){
        insertMLITNode(this.root,info);
    }

    public void printTree(){
        DFSTraversIndex(this.root);
/*
        System.out.println(this.root.info);
        if(((MLITInternalNode)this.root).children !=null){
            for(MLITNode child: ((MLITInternalNode)this.root).children){
                DFSTraversIndex(child);
            }
        }
*/

    }

    public void DFSTraversIndex(MLITNode p){
        if(p != null){
            System.out.println(p.info);
            if(p instanceof MLITInternalNode){
                for(MLITNode child: ((MLITInternalNode)p).children){
                    DFSTraversIndex(child);
                }
            }
        }
    }

    public void BFSprintTree(){
        System.out.println("Broad First Search");

        Queue<MLITNode> myQueue = new LinkedList<MLITNode>();
        myQueue.add(root);
        MLITNode pnode = null;
        while(!myQueue.isEmpty()){
            pnode = myQueue.poll();
            System.out.println(pnode.info+"\t"+pnode.layer);

            if(pnode instanceof MLITInternalNode){
                MLITInternalNode internalNode = (MLITInternalNode) pnode;
                for(int i=0;i<4;i++){
                    myQueue.add(internalNode.children[i]);
                }
            }
        }
    }



    public void insertMLITNode(MLITNode node, CellInfo cellInfo){

        //判断已添加的节点是否已经村存在，若是则 修改该节点所对应的ID
        if(node.cellEqual(cellInfo)){
            node.info.set(cellInfo);
            System.out.println(cellInfo.toString() +"  add successfully!");
        }else{//不相同 ,则 该节点变为中间节点，同时使其 重新划分成四个节点
            //找出该节点 为与其副节点的那个方向
            MLITNode parent  = node.parent;
            Point cp = cellInfo.getCenterPoint();

            if(node instanceof MLITLeafNode){

                MLITInternalNode newParent = new MLITInternalNode(node.info,node.layer);
                newParent.creatChird();     //增加4个叶子节点
                newParent.parent = parent;
                int dir = -1;
                for (int i = 0; i < 4; i++) {
                    if(newParent.children[i].contains(cp)){
                        dir = i;
                        break;
                    }
                }
                node = newParent;
                insertMLITNode(((MLITInternalNode)node).children[dir],cellInfo);
            }else {
                //internal node, insert into one of his child
                int dir = -1;
                for (int i = 0; i < 4; i++) {
                    if(((MLITInternalNode)node).children[i].contains(cp)){
                        dir = i;
                        break;
                    }
                }
                insertMLITNode(((MLITInternalNode)node).children[dir],cellInfo);
            }
        }

    }

    interface Query{
        public MLITNode SpatialPointQuery(Point p);
        public MLITNode SpatialRangeQuery(Rectangle rect);
        public MLITNode SpatialTemporalRangeQuery(Rectangle rect,int minTime,int maxTime);

    }

    class MLITNode implements Serializable,Query{
        public CellInfo info;
        public byte layer;

        public MLITNode parent;

        public MLITNode(){
            info = null;
            parent = null;
            layer = 0;
        }

        public MLITNode(CellInfo cellInfo,byte lay){
            this.info = new CellInfo(cellInfo);
            this.layer = lay;
            parent = null;
        }

        public boolean contains(Point p){
            return info.contains(p);
        }

        public void setInfo(CellInfo cellInfo) {
            if(this.info ==null){
                this.info = new CellInfo(cellInfo);
                ;
            }else{
                this.info.set(cellInfo);
            }

        }

        public void setID(int id){
            this.info.cellId = id;
        }



        public boolean isIntersected(Rectangle rect){
            return info.isIntersected(rect);
        }

        public boolean cellEqual(CellInfo q) {
            double m1 = this.info.x1;
            double m2 = this.info.y1;
            double m3 = this.info.x2;
            double m4 = this.info.y2;
            double n1 = q.x1;
            double n2 = q.y1;
            double n3 = q.x2;
            double n4 = q.y2;
            double err = 0.000001;

            if ((Math.abs(n1 - m1) < err) && (Math.abs(n2 - m2) < err) && (Math.abs(n3 - m3) < err)
                    && (Math.abs(n4 - m4) < err)) {
                return true;
            } else {
                return false;
            }
        }

        public MLITNode SpatialPointQuery(Point p){
            return new MLITNode();
        }

        public MLITNode SpatialRangeQuery(Rectangle rect){
            return  new MLITNode();

        }
        public MLITNode SpatialTemporalRangeQuery(Rectangle rect,int minTime,int maxTime){
            return  new MLITNode();
        }

    }

    class MLITInternalNode extends MLITNode{
        public MLITNode[] children = null;

        public MLITInternalNode(){
            this(new CellInfo(),(byte) 0);
        }

        public MLITInternalNode(CellInfo cellInfo,byte layer){

            super(cellInfo,layer);
            this.creatChird();
            parent = null;
        }

        public MLITNode[] getChildren(){
            return children;
        }

        public void creatChird() {

            double x1 = this.info.x1;
            double x2 = this.info.x2;
            double y1 = this.info.y1;
            double y2 = this.info.y2;
            double width = (x2 - x1) / 2;
            double height = (y2 - y1) / 2;
            this.children = new MLITNode[Qurd];
            this.children[0] = new MLITLeafNode(new CellInfo(-1, x1, y1 + height, x1 + width, y2),(byte) (this.layer + 1));
            this.children[1] = new MLITLeafNode(new CellInfo(-1, x1 + width, y1 + height, x2, y2),(byte) (this.layer + 1));
            this.children[2] = new MLITLeafNode(new CellInfo(-1, x1, y1, x1 + width, y1 + height),(byte) (this.layer + 1));
            this.children[3] = new MLITLeafNode(new CellInfo(-1, x1 + width, y1, x2, y1 + height),(byte) (this.layer + 1));
            for(int i=0;i<Qurd;i++){
                this.children[i].parent = this;
            }
        }

        public void insert(CellInfo cellInfo){
            if(this.cellEqual(cellInfo)){
                this.info.set(cellInfo);
            }else{
                int dir = -1;
                Point cp = cellInfo.getCenterPoint();
                for(int i=0; i < 4;i++){
                    if(this.children[i].contains(cp)){
                        dir = i;
                        break;
                    }

                }
                if(this.children[dir] instanceof MLITInternalNode){
                    ((MLITInternalNode)this.children[dir]).insert(cellInfo);
                }else {
                    ((MLITLeafNode)this.children[dir]).insert(cellInfo,dir);
                }
            }
        }


        public MLITNode SpatialPointQuery(Point p){
            return new MLITLeafNode();
        }

        public MLITNode  SpatialRangeQuery(Rectangle rect){
            return new MLITLeafNode();
        }

        public MLITNode SpatialTemporalRangeQuery(Rectangle rect,int minTime,int maxTime){
            return new MLITLeafNode();
        }

    }

    class MLITLeafNode extends MLITNode {
        public BPlusTree<Long, String>[] forests = null;

        public void insert(CellInfo cellInfo,int direction){
            if(this.cellEqual(cellInfo)){
                this.setInfo(cellInfo);
            }else{
                MLITInternalNode parent  = (MLITInternalNode)this.parent;

                MLITInternalNode newParent = new MLITInternalNode(this.info,this.layer);
                newParent.creatChird();     //增加4个叶子节点
                newParent.parent = parent;

                parent.children[direction] = newParent;

                int dir = -1;
                Point cp = cellInfo.getCenterPoint();
                for (int i = 0; i < 4; i++) {
                    if(newParent.children[i].contains(cp)){
                        dir = i;
                        break;
                    }
                }
                //     System.out.println("test print");
                //     printTree();
                this.setInfo(newParent.children[dir].info);
                this.layer = newParent.children[dir].layer;
                this.parent = newParent;

                newParent.children[dir] = this;
                //      System.out.println("test1 print");
                //       printTree();
                this.insert(cellInfo,dir);
            }
        }

        public MLITLeafNode() {
            this(null);
        }

        public MLITLeafNode(CellInfo cellInfo) {
            this(cellInfo, (byte) 0);
        }

        public MLITLeafNode(CellInfo cellInfo, byte layer) {
            info = cellInfo;
            this.layer = layer;
            parent = null;
        }



        public MLITNode SpatialPointQuery(Point p) {
            return new MLITLeafNode();
        }

        public MLITNode SpatialRangeQuery(Rectangle rect) {
            return new MLITLeafNode();
        }

        public MLITNode SpatialTemporalRangeQuery(Rectangle rect, int minTime, int maxTime) {
            return new MLITLeafNode();
        }

    }

    public static void main(String[] args) throws IOException{
        // initial spatial index from file
        File f = new File("/home/zzg/Downloads/32.txt");
        BufferedReader br = new BufferedReader(new FileReader(f));
        String line = null;
        String[] tokens = null;
        CellInfo rootInfo = new CellInfo(1,115.750000,39.500000,117.200000,40.500000);        ///

        MultiLevelIndexTree mlitree = new MultiLevelIndexTree(3,rootInfo);
        while((line = br.readLine())!=null){
            tokens = line.split(" ");
            System.out.println(line);
            CellInfo info = new CellInfo(Integer.parseInt(tokens[0]),new Rectangle( Double.parseDouble(tokens[2]),
                    Double.parseDouble(tokens[3]),
                    Double.parseDouble(tokens[4]),
                    Double.parseDouble(tokens[5]) ));
            mlitree.insertCell(info);
            //       System.out.println("main print");
            //      mlitree.printTree();
        }

        mlitree.printTree();

        // construct the temporal index from the file list.
    }

}
