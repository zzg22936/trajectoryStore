package edu.ecnu.idse.TrajStore.core;

import edu.ecnu.idse.TrajStore.io.TextSerializerHelper;
import org.apache.hadoop.io.Text;

import java.awt.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zzg on 15-12-31.
 */
public class MoPoint implements Shape, Comparable<MoPoint> {
    public int id;
    public Point point;
    public MoPoint() {
        this.id = -1;
        this.point = new Point();
    }

    public MoPoint(MoPoint s) {
        this.id = s.id;
        this.point = new Point(s.point);
    }

    public MoPoint(int id, Point p){
        this.id = id;
        this.point = new Point(p);
    }

    public MoPoint(int id, double x, double y, double z){
        this.id = id;
        this.point = new Point(x,y,z);
    }

    public Point getPoint(){
        return this.point;
    }

    public int getID(){
        return id;
    }

    public long getTimeStamp(){
        return (long)(point.getZ());
    }

    public double getLongitude(){
        return point.getX();
    }

    public double getLatitude(){
        return point.getY();
    }

    public void set(int id, double x, double y, double z) {
        this.id = id;
        this.point.set(x,y,z);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        point.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        point.readFields(in);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof MoPoint){
            MoPoint r2 =(MoPoint) obj;
            return this.id == r2.id && this.point.equals(r2.point);
        }else{
            return false;
        }
    }

    public double distanceTo(MoPoint s) {
        return  this.point.distanceTo(s.point);
    }

    @Override
    public double distanceTo(double px, double py, double pz) {
       return point.distanceTo(px,py,pz);
    }

    // 平面距离
    public double distanceTo(Point p) {
        return   this.point.flatDistance(p);
    }

    public double timeDiff(MoPoint m){
        return Math.abs(m.point.z - this.point.z);
    }

    public MoPoint clone() {
        return new MoPoint(this.id, this.point);
    }

    @Override
    public Cubic getMBC(){
      return  point.getMBC();
    }

    @Override
    public Rectangle getMBR(){
        return point.getMBR();
    }

    @Override
    public boolean isIntersected(Shape s) {
        return getMBC().isIntersected(s);
    }


    public Shape getIntersection(Shape s) {
        return getMBC().getIntersection(s);
    }

    @Override
    public String toString() {
        return id+","+ point.toString();
    }

    @Override
    public Text toText(Text text) {
        TextSerializerHelper.serializeInt(id, text, ',');
        point.toText(text);
        return text;
    }

    @Override
    public void fromText(Text text) {
        id = TextSerializerHelper.consumeInt(text, ',');
        point.fromText(text);
    }

    public int compareTo(MoPoint mo){
        if (id < mo.id)
            return -1;
        if (id > mo.id)
            return 1;
        return point.compareTo(mo.point);
    }

    public int compareTo(Point o){
       return point.compareTo(o);
    }

    @Override
    public void draw(Graphics g, Cubic fileMBC, int imageWidth,
                     int imageHeight, double scale) {
        point.draw(g,fileMBC,imageWidth,imageHeight,scale);
    }

    @Override
    public void draw(Graphics g, double xscale, double yscale) {
        point.draw(g,xscale,yscale);
    }
}
