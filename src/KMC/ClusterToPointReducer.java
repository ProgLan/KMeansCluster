package KMC;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;

/** 
 * You can modify this class as you see fit, as long as you correctly update the
 * global centroids.
 */
public class ClusterToPointReducer extends Reducer<Text, Text, Text, Text>
{
	//update globla centroids
	public void reduce(Point key, Iterable<Point> values, Context context){
		
		
		for(Point p: values){
			
		}
		
		
	}
}
