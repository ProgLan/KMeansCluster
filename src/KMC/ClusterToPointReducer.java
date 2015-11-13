package KMC;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;
import java.util.ArrayList;

/** 
 * You can modify this class as you see fit, as long as you correctly update the
 * global centroids.
 */
public class ClusterToPointReducer extends Reducer<Text, Text, Text, Text>
{
	
	public void reduce(Point key, Iterable<Point> values, Context context){
		
		int numOfPoints = 0;
		Point sum = new Point(key.dimension);
		
		for(Point p: values){
			sum = Point.addPoints(sum, p);
			numOfPoints++;
		}
		
		Point newCentroid = Point.multiplyScalar(sum, 1/numOfPoints);
		
		int index = KMeans.centroids.indexOf(key);
		
		KMeans.centroids.set(index, newCentroid);
		
	}
}
