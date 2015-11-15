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
public class ClusterToPointReducer extends Reducer<IntWritable, Point, IntWritable, Point>
{
	
	
	
	public void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException{
		
		float numOfPoints = 0.0f;
		Point sum = new Point(KMeans.centroids.get(0).getDimension());
		
		for(Point p: values){
			sum = Point.addPoints(sum, p);
			numOfPoints++;
		}
		
		//System.out.println("numOfPoit" + numOfPoints);
		
		Point newCentroid = Point.multiplyScalar(sum, 1.0f/numOfPoints);
		
		//int index = KMeans.centroids.indexOf(key);
		System.out.println("key" + key.get());
		KMeans.centroids.set(key.get(), newCentroid);
		
		context.write(key, newCentroid);
		
	}
}
