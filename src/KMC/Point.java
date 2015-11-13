package KMC;
import java.io.*; // DataInput/DataOuput
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.io.*; // Writable

/**
 * A Point is some ordered list of floats.
 * 
 * A Point implements WritableComparable so that Hadoop can serialize
 * and send Point objects across machines.
 *
 * NOTE: This implementation is NOT complete.  As mentioned above, you need
 * to implement WritableComparable at minimum.  Modify this class as you see fit.
 */
public class Point implements WritableComparable{
    /**
     * Construct a Point with the given dimensions [dim]. The coordinates should all be 0.
     * For example:
     * Constructing a Point(2) should create a point (x_0 = 0, x_1 = 0)
     */
	
	int dimension;
	//key is in which dimension, value is the point's value in that dimension
	//Point(x_0:1.0, x_1:2.0, x_2:3.0) could be expressed as <0,1.0>,<1,2.0>,<2,3.0>
	HashMap<Integer, Float> pointCoord = new HashMap<Integer, Float>();
	
	
    public Point(int dim)
    {
    	//handle input dim is non-positive
    	if(dim <= 0){
    		System.out.println("not a valid dimension");
    	}else{
    		this.dimension = dim;
    		for(int i = 0; i < dim; i++){
    			this.pointCoord.put(i, (float) 0.0);
    		}
    		
    	}
        //System.out.println("TODO");
        //System.exit(1);
    }

    /**
     * Construct a point from a properly formatted string (i.e. line from a test file)
     * @param str A string with coordinates that are space-delimited.
     * For example: 
     * Given the formatted string str="1 3 4 5"
     * Produce a Point {x_0 = 1, x_1 = 3, x_2 = 4, x_3 = 5}
     */
    public Point(String str)
    {
    	if(str == null || str.length() == 0){
    		System.out.println("input string is empty");
    	}else{
    		String[] tempStore = str.split("\\s+");
    		this.dimension = tempStore.length;
    		
    		for(int i = 0; i < this.dimension; i++){
    			this.pointCoord.put(i, Float.parseFloat(tempStore[i]));
    		}
    	}
        
    	//System.out.println("TODO");
        //System.exit(1);
    }

    /**
     * Copy constructor
     */
    public Point(Point other)
    {
    	this.dimension = other.dimension;
    	
    	for(int i = 0; i < this.dimension; i++){
    		this.pointCoord.put(i, other.pointCoord.get(i));
    	}
        //System.out.println("TODO");
        //System.exit(1);
    }

    /**
     * @return The dimension of the point.  For example, the point [x=0, y=1] has
     * a dimension of 2.
     */
    public int getDimension()
    {
    	return this.dimension;
        //System.out.println("TODO");
        //System.exit(1);
        //return 0;
    }

    /**
     * Converts a point to a string.  Note that this must be formatted EXACTLY
     * for the autograder to be able to read your answer.
     * Example:
     * Given a point with coordinates {x=1, y=1, z=3}
     * Return the string "1 1 3"
     */
    public String toString()
    {
    	if(this.pointCoord == null || this.pointCoord.keySet().size() == 0 || this.dimension == 0){
    		System.out.println("point is empty");
    		return null;
    	}else{
    		StringBuilder sb = new StringBuilder();
    		
    		for(int i = 0; i < this.dimension; i++){
    			if(i == this.dimension - 1){
    				sb.append(this.pointCoord.get(i).toString());
    			}else{
    				sb.append(this.pointCoord.get(i).toString() + " ");	
    			}
    		}
    		
    		String res = sb.toString();
    		
    		return res;	
    	}
        //System.out.println("TODO");
        //System.exit(1);
        //return null;
    }

    /**
     * One of the WritableComparable methods you need to implement.
     * See the Hadoop documentation for more details.
     * You should order the points "lexicographically" in the order of the coordinates.
     * Comparing two points of different dimensions results in undefined behavior.
     */
    @Override
    public int compareTo(Object o)
    {   
    	Point p = (Point)o;
    	if(this.dimension != p.dimension){
    		System.out.println("two points have different dimensions, undefined behavior");
    		return 0;
    	}else{
    		for(int i = 0; i < this.dimension; i++){
    			if(this.pointCoord.get(i) == p.pointCoord.get(i)){
    				continue;
    			}else{
    				return this.pointCoord.get(i) < p.pointCoord.get(i) ? -1:1;
    			}
    		}
    		
    		return 0;
    	}
        //System.out.println("TODO");
        //System.exit(1);
        //return 0;
    }

    /**
     * @return The L2 distance between two points.
     */
    public static final float distance(Point x, Point y)
    {
    	int dim1 = x.dimension;
    	int dim2 = y.dimension;
    	
    	if(dim1 != dim2){
    		System.out.println("points in different dimension, can not calculate distance");
    		return (float)0.0;
    	}else{
    		float sum = (float)0.0;
    		
    		for(int i = 0; i < dim1; i++){
    			sum += Math.pow(x.pointCoord.get(i) - y.pointCoord.get(i), 2.0);
    		}
    		
    		float res = (float)Math.sqrt(sum);
    		
    		return res;
    	}
        //System.out.println("TODO");
        //System.exit(1);
        //return (float)0.0;
    }

    /**
     * @return A new point equal to [x]+[y]
     */
    public static final Point addPoints(Point x, Point y)
    {
        if(x.dimension != y.dimension){
        	System.out.println("points have different dimensions, can not be added directly");
        	return null;
        }else{
        	Point res = new Point(x.dimension);
        	
        	for(int i = 0; i < x.dimension; i++){
        		res.pointCoord.put(i, x.pointCoord.get(i) + y.pointCoord.get(i));
        	}
        	
        	return res;
        }
    	
    	//System.out.println("TODO");
        //System.exit(1);
        //return null;
    }

    /**
     * @return A new point equal to [c][x]
     */
    //c = 1 / num of points
    //Point x = sum of all points associate with a particular centroid
    public static final Point multiplyScalar(Point x, float c)
    {
    	int dim = x.dimension;
    	Point res = new Point(dim);
    	
    	for(int i = 0; i < dim; i++){
    		res.pointCoord.put(i, x.pointCoord.get(i) * c);
    	}
    	
    	return res;
        //System.out.println("TODO");
        //System.exit(1);
        //return null;
    }

    //deserialize the fields of in object
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		String inputString = arg0.readLine();
		
		if(inputString == null || inputString.length() == 0){
    		System.out.println("input string is empty");
    	}else{
    		String[] tempStore = inputString.split("\\s+");
    		this.dimension = tempStore.length;
    		
    		for(int i = 0; i < this.dimension; i++){
    			this.pointCoord.put(i, Float.parseFloat(tempStore[i]));
    		}
    	}
	}
	
	//serialize the field of output arg0
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		for(int i = 0; i < this.dimension; i++){
			if(i == this.dimension - 1){
				arg0.writeFloat(this.pointCoord.get(i));
				arg0.writeChars("\n");
			}else{
				arg0.writeFloat(this.pointCoord.get(i));
				//TODO: why cannot use \s
				arg0.writeChars("\t");
			}
		}
		
	}


}
