package edu.ecnu.idse.TrajStore.operations;

import edu.ecnu.idse.TrajStore.core.CellInfo;
import edu.ecnu.idse.TrajStore.core.Point;
import edu.ecnu.idse.TrajStore.core.Rectangle;

/**
 * Created by zzg on 16-2-28.
 */
public interface SpatialTemporalQuery {
    public CellInfo SpatialPointQuery(Point p);
    public CellInfo[] SpatialRangeQuery(Rectangle rect);
    public CellInfo[] SpatialTemporalRangeQuery(Rectangle rect,int minTime,int maxTime);
}
