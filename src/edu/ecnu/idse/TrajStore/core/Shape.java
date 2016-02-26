package edu.ecnu.idse.TrajStore.core;

import java.awt.Graphics;

import org.apache.hadoop.io.Writable;

import edu.ecnu.idse.TrajStore.io.TextSerializable;

public interface Shape extends Writable, Cloneable, TextSerializable {

    /**
     * Returns minimum bounding cubic for this shape.
     * @return
     */
    public Cubic getMBC();

    public Rectangle getMBR();
    /*
     * Gets the distance of this shape to the given point.
     */
    public double distanceTo(double x, double y, double z);

    /**
     * Returns true if this shape is intersected with the given shape
     * @param s
     * @return
     */
    public boolean isIntersected(final Shape s);

    /**
     * Returns a clone of this shape
     * @return
     * @throws CloneNotSupportedException
     */
    public Shape clone();

    /**
     * Draws a shape to the given graphics.
     * @param g - the graphics or canvas to draw to
     * @param fileMBR - the MBR of the file in which the shape is contained
     * @param imageWidth - width of the image to draw
     * @param imageHeight - height of the image to draw
     * @param scale - the scale used to convert shape coordinates to image coordinates
     * @deprecated - see
     */
    @Deprecated
    public void draw(Graphics g, Cubic fileMBC, int imageWidth, int imageHeight, double scale);
    /**
     * Draws the shape to the given graphics and scale.
     * @param g - the graphics to draw the shape to.
     * @param xscale - scale of the image x-axis in terms of pixels per points.
     * @param yscale - scale of the image y-axis in terms of pixels per points.
     */
    public void draw(Graphics g, double xscale, double yscale);
}
