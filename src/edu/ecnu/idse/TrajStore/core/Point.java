package edu.ecnu.idse.TrajStore.core;

import edu.ecnu.idse.TrajStore.io.TextSerializerHelper;
import org.apache.hadoop.io.Text;

import java.awt.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zzg on 15-12-23.
 */
public class Point implements Shape, Comparable<Point> {

    public double x;
    public double y;
    public double z;

    public Point() {
        this(0, 0, 0);
    }

    public Point(double x, double y, double z) {
        set(x, y, z);
    }

    public Point(Point s) {
        this.x = s.x;
        this.y = s.y;
        this.z = s.z;
    }

    public void set(double x, double y, double z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(x);
        out.writeDouble(y);
        out.writeDouble(z);
    }

    public boolean isValid() {
        return !Double.isNaN(x);
    }

    public void invalidate() {
        this.x = Double.NaN;
    }

    public void readFields(DataInput in) throws IOException {
        this.x = in.readDouble();
        this.y = in.readDouble();
        this.z = in.readDouble();
    }

    public int compareTo(Shape s) {
        Point pt2 = (Point) s;

        // Sort by id
        double difference = this.z -pt2.z;
        if(difference ==0){
            difference = this.x - pt2.x;
        }
        if (difference == 0) {
            difference = this.y - pt2.y;
        }
        if (difference == 0)
            return 0;
        return difference > 0 ? 1 : -1;
    }

    public boolean equals(Object obj) {
        Point r2 = (Point) obj;
        return this.z == r2.z && this.x == r2.x && this.y == r2.y;
    }


    public double distanceTo(Point s) {
        double dx = s.x - this.x;
        double dy = s.y - this.y;
        double dz = s.z - this.z;
        return Math.sqrt(dx*dx+dy*dy+dz*dz);
    }
    // 两时空点之间的 平面距离
    public double flatDistance(Point s){
        double dx = s.x - this.x;
        double dy = s.y - this.y;
        return Math.sqrt(dx*dx+dy*dy);
    }

    public Point clone() {
        return new Point(this.x, this.y, this.z);
    }
    @Override
    public Cubic getMBC() {
        return new Cubic(x, y, z, x + Math.ulp(x), y + Math.ulp(y), z+Math.ulp(z));
    }
    @Override
    public Rectangle getMBR() {
        return new Rectangle(x, y, x + Math.ulp(x), y + Math.ulp(y));
    }

    @Override
    public double distanceTo(double px, double py, double pz) {
        double dx = x - px;
        double dy = y - py;
        double dz = z - pz;
        return Math.sqrt(dx * dx + dy * dy + dz * dz);
    }

    public boolean isIntersected(Shape s) {
        return getMBC().isIntersected(s);
    }

    public Shape getIntersection(Shape s) {
        return getMBC().getIntersection(s);
    }

    public String toString() {
        return x+","+y+","+z;
    }

    public Text toText(Text text) {
        TextSerializerHelper.serializeDouble(x, text, ',');
        TextSerializerHelper.serializeDouble(y, text, ',');
        TextSerializerHelper.serializeDouble(z, text, '\0');
        return text;
    }

    public void fromText(Text text) {
        x = TextSerializerHelper.consumeDouble(text, ',');
        y = TextSerializerHelper.consumeDouble(text, ',');
        z = TextSerializerHelper.consumeDouble(text, '\0');
    }

    public int compareTo(Point o) {
        if (z < o.z)
            return -1;
        if (z > o.z)
            return 1;
        if (x < o.x)
            return -1;
        if (x > o.x)
            return 1;
        if (y < o.y)
            return -1;
        if (y > o.y)
            return 1;
        return 0;
    }

    public double getX(){
        return x;
    }

    public double getY(){
        return y;
    }

    public double getZ(){
        return z;
    }
    //无法绘制时间维数据
    @Override
    public void draw(Graphics g, Cubic fileMBC, int imageWidth,
                     int imageHeight, double scale) {
        int imageX = (int) Math.round((this.x - fileMBC.x1) * imageWidth / fileMBC.getWidth());
        int imageY = (int) Math.round((this.y - fileMBC.y1) * imageHeight / fileMBC.getHeight());

        g.fillRect(imageX, imageY, 1, 1);
    }

    @Override
    public void draw(Graphics g, double xscale, double yscale) {
        int imgx = (int) Math.round(x * xscale);
        int imgy = (int) Math.round(y * yscale);
        g.fillRect(imgx, imgy, 1, 1);
    }
}
