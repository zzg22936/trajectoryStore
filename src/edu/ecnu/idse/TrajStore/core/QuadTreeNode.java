package edu.ecnu.idse.TrajStore.core;

/**
 * Created by zzg on 16-2-17.
 */
public class QuadTreeNode {
    public static final int NW = 0, NE = 1, SW = 2, SE = 3;
	/*
	 * a quadrant defined below:
	 * 		NW(0) | NE(1)
	 * -----------|-----------
	 * 		SW(2) | SE(3)
	 */

    private CellInfo info =null;
    // node number
    private static int Num = 0;
    public byte layer;
    public QuadTreeNode[] node = null;

   public QuadTreeNode(CellInfo in,byte layer){
       info = new CellInfo(in);
       Num++;
       this.layer = layer;
    }

    public QuadTreeNode[] getChildren(){
        return  node;
    }

    public QuadTreeNode(double x1, double y1, double x2, double y2,byte layer){
        info = new CellInfo(Num,x1,y1,x2,y2);
        Num++;
        this.layer = layer;
    }

    public void creatChird() {
        double x1 = this.info.x1;
        double x2 = this.info.x2;
        double y1 = this.info.y1;
        double y2 = this.info.y2;
        double width = (x2 - x1) / 2;
        double height = (y2 - y1) / 2;

        this.node = new QuadTreeNode[4];

        this.node[0] = new QuadTreeNode(x1, y1 + height, x1 + width, y2,(byte) (this.layer + 1));

        this.node[1] = new QuadTreeNode(x1 + width, y1 + height, x2, y2,(byte) (this.layer + 1));

        this.node[2] = new QuadTreeNode(x1, y1, x1 + width, y1 + height,(byte) (this.layer + 1));

        this.node[3] = new QuadTreeNode(x1 + width, y1, x2, y1 + height,(byte) (this.layer + 1));
    }

    public boolean hasChild() {
       if(node!= null && node.length!=0)
           return true;
       else{
           return false;
       }

    }

    public void insertNode(CellInfo qinfo) {
        if(this.hasChild()==false){
            this.creatChird();
        }
        Point cPoint = qinfo.getCenterPoint();
        int dir = -1;
        for (int i = 0; i < 4; i++) {
            if (this.node[i].contains (cPoint)) {
                dir = i;
                break;
            }
        }
        if(!this.node[dir].cellEqual(qinfo)){
            this.node[dir].insertNode(qinfo);
        }
    }

    public boolean contains(Point p){
        return info.contains(p);
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
/*        System.out.println("lb x diff = " + Math.abs(n1 - m1));
        System.out.println("lb y diff =  " + Math.abs(n2 - m2));
        System.out.println("rt x diff = " + Math.abs(n3 - m3));
        System.out.println("rt y diff =  " + Math.abs(m4 - n4));*/
        if ((Math.abs(n1 - m1) < err) && (Math.abs(n2 - m2) < err) && (Math.abs(n3 - m3) < err)
                && (Math.abs(n4 - m4) < err)) {
            return true;
        } else {
            return false;
        }
    }
}
