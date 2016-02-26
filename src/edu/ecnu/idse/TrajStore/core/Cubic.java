package edu.ecnu.idse.TrajStore.core;

import edu.ecnu.idse.TrajStore.io.TextSerializerHelper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.awt.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zzg on 15-12-23.
 */
public class Cubic implements Shape,WritableComparable<Cubic> {
    //the min corner
    public double x1;
    public double y1;
    public double z1;
    //the max corner
    public double x2;
    public double y2;
    public double z2;

    public Cubic(){
        this(0,0,0,0,0,0);
    }

    public Cubic(Cubic c){
        this(c.x1, c.y1, c.z1, c.x2, c.y2, c.z2);
    }



    public Cubic(double x1,	double y1,	double z1,	double x2,	double y2,	double z2){
        this.set(x1, y1, z1, x2, y2, z2);
    }

    public void set(Shape s){
        Cubic mbc = s.getMBC();
        set(mbc.x1, mbc.y1, mbc.z1, mbc.x2, mbc.y2,mbc.z2);
    }

    public void set(String str) {
        String[] par = str.split(",");
        if(par.length != 6){
            System.out.println("initial failed");
            this.set(0,0,0,0,0,0);
        }else {
            this.set(Double.parseDouble(par[0]),
                    Double.parseDouble(par[1]),
                    Double.parseDouble(par[2]),
                    Double.parseDouble(par[3]),
                    Double.parseDouble(par[4]),
                    Double.parseDouble(par[5]));
        }
    }

    public void set(double x1,	double y1,	double z1,	double x2,	double y2,	double z2) {
        this.x1 = x1;
        this.y1 = y1;
        this.z1 = z1;
        this.x2 = x2;
        this.y2 = y2;
        this.z2 = z2;
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(x1);
        out.writeDouble(y1);
        out.writeDouble(z1);
        out.writeDouble(x2);
        out.writeDouble(y2);
        out.writeDouble(z2);
    }

    public void readFields(DataInput in) throws IOException {
        this.x1 = in.readDouble();
        this.y1 = in.readDouble();
        this.z1 = in.readDouble();
        this.x2 = in.readDouble();
        this.y2 = in.readDouble();
        this.z2 = in.readDouble();
    }

    @Override
    public int compareTo(Cubic r2) {
        if (this.z1 < r2.z1)
            return -1;
        if (this.z1 > r2.z1)
            return 1;

        if (this.x1 < r2.x1)
            return -1;
        if (this.x1 > r2.x1)
            return 1;
        if (this.y1 < r2.y1)
            return -1;
        if (this.y1 > r2.y1)
            return 1;

        if (this.x2 < r2.x2)
            return -1;
        if (this.x2 > r2.x2)
            return 1;
        if (this.y2 < r2.y2)
            return -1;
        if (this.y2 > r2.y2)
            return 1;
        if (this.z2 < r2.z2)
            return -1;
        if (this.z2 > r2.z2)
            return 1;
        return 0;
    }

    public int compareTo(Shape s) {
        Cubic cubic2 = (Cubic) s;
        // sort by z
        if(this.z1 < cubic2.z2)
            return -1;
        if(this.z1 > cubic2.z2)
            return 1;

        // Sort by x1 then y1
        if (this.x1 < cubic2.x1)
            return -1;
        if (this.x1 > cubic2.x1)
            return 1;
        if (this.y1 < cubic2.y1)
            return -1;
        if (this.y1 > cubic2.y1)
            return 1;

        // Sort by x2 then y2
        if (this.x2 < cubic2.x2)
            return -1;
        if (this.x2 > cubic2.x2)
            return 1;
        if (this.y2 < cubic2.y2)
            return -1;
        if (this.y2 > cubic2.y2)
            return 1;
        return 0;
    }

    public boolean equals(Object obj) {
        Cubic r2 = (Cubic) obj;
        boolean result = this.z1 == r2.z2 && this.x1 == r2.x1 && this.y1 == r2.y1
                && this.x2 == r2.x2 && this.y2 == r2.y2;
        return result;
    }

    public boolean contains(double rx1, double ry1, double rz1, double rx2, double ry2, double rz2) {
        return rx1 >= x1 && rx2 <= x2 && ry1 >= y1 && ry2 <= y2 && rz1>=z1 && rz2 <=z2;
    }

    public boolean contains(double px, double py, double pz){
        return px>=x1 && px<= x2 && py>=y1 && py<= y2 &&  pz>=z1 && pz<= z2;
    }

    public boolean contains(Point p){
        return p.x>=x1 && p.x<= x2 && p.y>=y1 && p.y<= y2 &&  p.z>=z1 && p.z<= z2;
    }

    @Override
    public Cubic clone(){
        return new Cubic(this);
    }

    @Override
    public Rectangle getMBR() {
        return new Rectangle(x1, y1, x2, y2);
    }


    @Override
    public Cubic getMBC() {
        return new Cubic(this);
    }

    public double distanceTo(double px, double py, double pz) {




        return 0;
    }

    public boolean isIntersected(Shape s) {
        if(s instanceof Point){
            Point pt = (Point)s;
            return pt.z >= z1 && pt.z< z2 && pt.x >= x1 && pt.x < x2 && pt.y >= y1 && pt.y < y2;
        }
        Cubic c = s.getMBC();
        if(c == null)
            return false;
        return (this.x2 > c.x1 && c.x2 > this.x1 && this.y2 > c.y1 && c.y2 > this.y1 && this.z2 > c.z1 && c.z2 > this.z1 );
    }

    public boolean isValid() {
        return !Double.isNaN(x1);
    }

    public Point getCenterPoint() {
        return new Point((x1 + x2) / 2, (y1 + y2)/2, (z1 + z2)/2);
    }

    public void invalidate() {
        this.x1 = Double.NaN;
    }

    public Cubic getIntersection(Shape s) {
        if (!s.isIntersected(this))
            return null;
        Cubic r = s.getMBC();
        double ix1 = Math.max(this.x1, r.x1);
        double ix2 = Math.min(this.x2, r.x2);
        double iy1 = Math.max(this.y1, r.y1);
        double iy2 = Math.min(this.y2, r.y2);
        double iz1 = Math.max(this.z1, r.z1);
        double iz2 = Math.min(this.x2, r.z2);
        return new Cubic(ix1, iy1, ix2, iy2,iz1,iz2);
    }

    @Override
    public Text toText(Text text) {
        TextSerializerHelper.serializeDouble(x1, text, ',');
        TextSerializerHelper.serializeDouble(y1, text, ',');
        TextSerializerHelper.serializeDouble(z1, text, ',');
        TextSerializerHelper.serializeDouble(x2, text, ',');
        TextSerializerHelper.serializeDouble(y2, text, ',');
        TextSerializerHelper.serializeDouble(z2, text, '\0');
        return text;
    }

    @Override
    public void fromText(Text text) {
        x1 = TextSerializerHelper.consumeDouble(text, ',');
        y1 = TextSerializerHelper.consumeDouble(text, ',');
        z1 = TextSerializerHelper.consumeDouble(text, ',');
        x2 = TextSerializerHelper.consumeDouble(text, ',');
        y2 = TextSerializerHelper.consumeDouble(text, ',');
        z2 = TextSerializerHelper.consumeDouble(text, '\0');
    }

    @Override
    public String toString() {
        return "Cubic: ("+x1+","+y1+","+z1+")-("+x2+","+y2+","+z2+")";
    }

    public double getHeight() {
        return y2 - y1;
    }

    public double getWidth() {
        return x2 - x1;
    }

    public double getDeepth(){
        return z2 -z1;
    }


    @Override
    public void draw(Graphics g, Cubic fileMBC, int imageWidth,
                     int imageHeight, double scale) {
        int s_x1 = (int) Math.round((this.x1 - fileMBC.x1) * imageWidth / fileMBC.getWidth());
        int s_y1 = (int) Math.round((this.y1 - fileMBC.y1) * imageHeight / fileMBC.getHeight());
        int s_x2 = (int) Math.round((this.x2 - fileMBC.x1) * imageWidth / fileMBC.getWidth());
        int s_y2 = (int) Math.round((this.y2 - fileMBC.y1) * imageHeight / fileMBC.getHeight());
        g.fillRect(s_x1, s_y1, s_x2 - s_x1 + 1, s_y2 - s_y1 + 1);
    }

    @Override
    public void draw(Graphics g, double xscale, double yscale) {
        int imgx1 = (int) Math.round(this.x1 * xscale);
        int imgy1 = (int) Math.round(this.y1 * yscale);
        int imgx2 = (int) Math.round(this.x2 * xscale);
        int imgy2 = (int) Math.round(this.y2 * yscale);
        g.fillRect(imgx1, imgy1, imgx2 - imgx1 + 1, imgy2 - imgy1 + 1);
    }

    public void expand(final Shape s) {
        Cubic c = s.getMBC();
        if (c.x1 < this.x1)
            this.x1 = c.x1;
        if (c.x2 > this.x2)
            this.x2 = c.x2;
        if (c.y1 < this.y1)
            this.y1 = c.y1;
        if (c.y2 > this.y2)
            this.y2 = c.y2;
        if (c.z1 < this.z1)
            this.z1 = c.z1;
        if (c.z2 < this.z2)
            this.z2 = c.z2;
    }
}

