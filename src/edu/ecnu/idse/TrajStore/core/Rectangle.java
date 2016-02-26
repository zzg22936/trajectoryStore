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

public class Rectangle  implements Shape, Comparable<Rectangle>{

    public double x1;
    public double y1;
    public double x2;
    public double y2;

    public Rectangle() {
        this(0, 0, 0, 0);
    }

    /**
     * Constructs a new <code>Rectangle</code>, initialized to match
     * the values of the specified <code>Rectangle</code>.
     * @param r  the <code>Rectangle</code> from which to copy initial values
     *           to a newly constructed <code>Rectangle</code>
     * @since 1.1
     */
    public Rectangle(Rectangle r) {
        this(r.x1, r.y1, r.x2, r.y2);
    }

    public Rectangle(double x1, double y1, double x2, double y2) {
        this.set(x1, y1, x2, y2);
    }

    public void set(Shape s) {
        Rectangle mbr = s.getMBR();
        set(mbr.x1, mbr.y1, mbr.x2, mbr.y2);
    }

    public void set(double x1, double y1, double x2, double y2) {
        this.x1 = x1;
        this.y1 = y1;
        this.x2 = x2;
        this.y2 = y2;
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(x1);
        out.writeDouble(y1);
        out.writeDouble(x2);
        out.writeDouble(y2);
    }

    public void readFields(DataInput in) throws IOException {
        this.x1 = in.readDouble();
        this.y1 = in.readDouble();
        this.x2 = in.readDouble();
        this.y2 = in.readDouble();
    }

    /**
     * Comparison is done by lexicographic ordering of attributes
     * < x1, y1, x2, y2>
     */
    public int compareTo(Shape s) {
        Rectangle rect2 = (Rectangle) s;
        // Sort by x1 then y1
        if (this.x1 < rect2.x1)
            return -1;
        if (this.x1 > rect2.x1)
            return 1;
        if (this.y1 < rect2.y1)
            return -1;
        if (this.y1 > rect2.y1)
            return 1;

        // Sort by x2 then y2
        if (this.x2 < rect2.x2)
            return -1;
        if (this.x2 > rect2.x2)
            return 1;
        if (this.y2 < rect2.y2)
            return -1;
        if (this.y2 > rect2.y2)
            return 1;
        return 0;
    }

    public boolean equals(Object obj) {
        Rectangle r2 = (Rectangle) obj;
        boolean result = this.x1 == r2.x1 && this.y1 == r2.y1
                && this.x2 == r2.x2 && this.y2 == r2.y2;
        return result;
    }

    @Override
    public double distanceTo(double px, double py, double pz) {
        return this.getMaxDistanceTo(px, py);
    }

    /**
     * Maximum distance to the perimeter of the Rectangle
     * @param px
     * @param py
     * @return
     */
    public double getMaxDistanceTo(double px, double py) {
        double dx = Math.max(px - this.x1, this.x2 - px);
        double dy = Math.max(py - this.y1, this.y2 - py);

        return Math.sqrt(dx*dx+dy*dy);
    }

    public double getMinDistanceTo(double px, double py) {
        double dx, dy;
        if (px < this.x1)
            dx = this.x1 - px;
        else if (px < this.x2)
            dx = 0;
        else
            dx = px - this.x2;

        if (py < this.y1)
            dy = this.y1 - py;
        else if (py < this.y2)
            dy = 0;
        else
            dy = py - this.y2;

        if (dx > 0 && dy > 0)
            return Math.sqrt(dx * dx + dy * dy);
        return Math.max(dx, dy);
    }

    public double getMinDistance(Rectangle r2) {
        // dx is the horizontal gap between the two rectangles. If their x ranges
        // overlap, dx is zero
        double dx = 0;
        if (r2.x1 > this.x2)
            dx = r2.x1 - this.x2;
        else if (this.x1 > r2.x2)
            dx = this.x1 - r2.x2;

        double dy = 0;
        if (r2.y1 > this.y2)
            dy = r2.y1 - this.y2;
        else if (this.y1 > r2.y2)
            dy = this.y1 - r2.y2;

        // Case 1: Overlapping rectangles
        if (dx == 0 && dy == 0)
            return 0;

        // Case 2: Overlapping in one dimension only
        if (dx == 0 || dy == 0)
            return dx + dy;

        // Case 3: Not overlapping in any dimension
        return Math.sqrt(dx * dx + dy * dy);
    }

    public double getMaxDistance(Rectangle r2) {
        double xmin = Math.min(this.x1, r2.x1);
        double xmax = Math.max(this.x2, r2.x2);
        double ymin = Math.min(this.y1, r2.y1);
        double ymax = Math.max(this.y2, r2.y2);
        double dx = xmax - xmin;
        double dy = ymax - ymin;
        return Math.sqrt(dx * dx + dy * dy);
    }

    @Override
    public Rectangle clone() {
        return new Rectangle(this);
    }

    @Override
    public Rectangle getMBR() {
        return new Rectangle(this);
    }

    @Override
    public Cubic getMBC() {
        return new Cubic(this.x1,this.y1,0,this.x2,this.y2,Math.ulp(0));
    }

    public boolean isIntersected(Shape s) {
        if (s instanceof Point) {
            Point pt = (Point)s;
            return pt.x >= x1 && pt.x < x2 && pt.y >= y1 && pt.y < y2;
        }
        Rectangle r = s.getMBR();
        if (r == null)
            return false;
        return (this.x2 > r.x1 && r.x2 > this.x1 && this.y2 > r.y1 && r.y2 > this.y1);
    }

    public Rectangle IntersectedRecangle(Rectangle rect){
        return new Rectangle(Math.max(this.x1,rect.x1),Math.max(this.y1,rect.y1),Math.min(this.x2,rect.x2),Math.min(this.y2,rect.y2));
    }
//	  public Rectangle getIntersection(Shape s) {
//	    if (!s.isIntersected(this))
//	      return null;
//	    Rectangle r = s.getMBR();
//	    double ix1 = Math.max(this.x1, r.x1);
//	    double ix2 = Math.min(this.x2, r.x2);
//	    double iy1 = Math.max(this.y1, r.y1);
//	    double iy2 = Math.min(this.y2, r.y2);
//	    return new Rectangle(ix1, iy1, ix2, iy2);
//	  }

    public boolean contains(Point p) {
        return contains(p.x, p.y);
    }

    public boolean contains(double x, double y) {
        return x >= x1 && x < x2 && y >= y1 && y < y2;
    }

    public boolean contains(Rectangle r) {
        return contains(r.x1, r.y1, r.x2, r.y2);
    }

/*	  public Rectangle union(final Shape s) {
	    Rectangle r = s.getMBR();
	    double ux1 = Math.min(x1, r.x1);
	    double ux2 = Math.max(x2, r.x2);
	    double uy1 = Math.min(y1, r.y1);
	    double uy2 = Math.max(y2, r.y2);
	    return new Rectangle(ux1, uy1, ux2, uy2);
	  }*/


    public boolean contains(double rx1, double ry1, double rx2, double ry2) {
        return rx1 >= x1 && rx2 <= x2 && ry1 >= y1 && ry2 <= y2;
    }

    public Point getCenterPoint(){
        return new Point((x1+x2)/2, (y1+y2)/2, 0);
    }

    @Override
    public Text toText(Text text) {
        TextSerializerHelper.serializeDouble(x1, text, ',');
        TextSerializerHelper.serializeDouble(y1, text, ',');
        TextSerializerHelper.serializeDouble(x2, text, ',');
        TextSerializerHelper.serializeDouble(y2, text, '\0');
        return text;
    }

    @Override
    public void fromText(Text text) {
        x1 = TextSerializerHelper.consumeDouble(text, ',');
        y1 = TextSerializerHelper.consumeDouble(text, ',');
        x2 = TextSerializerHelper.consumeDouble(text, ',');
        y2 = TextSerializerHelper.consumeDouble(text, '\0');
    }

    @Override
    public String toString() {
        return "Rectangle: ("+x1+","+y1+")-("+x2+","+y2+")";
    }

    public boolean isValid() {
        return !Double.isNaN(x1);
    }

    public void invalidate() {
        this.x1 = Double.NaN;
    }

    public double getHeight() {
        return y2 - y1;
    }

    public double getWidth() {
        return x2 - x1;
    }

    /**
     * Compares two rectangles according to a lexicographic order of the
     * attributes (x1, y1, x2, y2)
     */
    @Override
    public int compareTo(Rectangle r2) {
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

        return 0;
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

    public Rectangle buffer(double dw, double dh) {
        return new Rectangle(this.x1 - dw, this.y1 - dh, this.x2 + dw, this.y2 + dh);
    }

    /**
     * Returns a new rectangle after translating with the given amount
     */
    public Rectangle translate(double dx, double dy) {
        return new Rectangle(this.x1 + dx, this.y1 + dy, this.x2 + dx, this.y2 + dy);
    }

    public static void main(String[] args){
        Rectangle rect1 = new Rectangle(0.0,0.0,3,6);
        Rectangle rect2 = new Rectangle(4,7,6,9);
        if(rect1.isIntersected(rect2)) {
            Rectangle rect3 = rect1.IntersectedRecangle(rect2);
            System.out.println(rect3.toString());
        }
    }

}
