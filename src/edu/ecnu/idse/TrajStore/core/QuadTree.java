package edu.ecnu.idse.TrajStore.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zzg on 16-2-17.
 */
public class QuadTree {
    public QuadTreeNode root;
    public int depth;

    public QuadTree(CellInfo info){
        depth =1;
        root=new QuadTreeNode(info,(byte) 0);
    }

    /* inserts a node into root */
    public void insert(CellInfo qinfo) {
        root.insertNode(qinfo);
    }

    // given a point, to query the leave node contains the point
    public QuadTreeNode SpatialPointQuery(Point p){
        if(!root.contains(p))
            return null;

        return   recursiveSpatialPointQuery(root,p);
    }

    private QuadTreeNode recursiveSpatialPointQuery(QuadTreeNode q,Point p){

        if(!q.hasChild()){
            return q;
        }else{
            int dir = -1;
            QuadTreeNode [] children =root.getChildren();
            for(int i=0;i< children.length;i++){
                if(children[i].contains(p)){
                    dir = i;
                    break;
                }
            }
            return   recursiveSpatialPointQuery(children[dir],p);
        }

    }

    // given a point, to query the leave node contains the point
    public List<QuadTreeNode> SpatialRangeQuery(Rectangle rect){
        if(!root.isIntersected(rect))
            return null;

        List<QuadTreeNode> list = new ArrayList<QuadTreeNode>();
        recursiveSpatialRangeQuery(root,rect,list);
        return list;
    }

    public void recursiveSpatialRangeQuery(QuadTreeNode q,Rectangle rect,List<QuadTreeNode> list){
        if(!q.hasChild() && q.isIntersected(rect)){
            list.add(q);
        }else {
            QuadTreeNode [] children =root.getChildren();
            for(int i=0;i< children.length;i++){
                if(children[i].isIntersected(rect)){
                    recursiveSpatialRangeQuery(children[i],rect,list);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException{
        // initial spatial index from file
        File f = new File("");
        BufferedReader br = new BufferedReader(new FileReader(f));
        String line = null;
        String[] tokens = null;
        while((line = br.readLine())!=null){
            tokens = line.split(" ");

        }
        // construct the temporal index from the file list.
    }

}
