package KMC;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * You can modify this class as you see fit.  You may assume that the global
 * centroids have been correctly initialized.
 */
public class PointToClusterMapper extends Mapper<LongWritable, Text, Point, Point>
{
	
	
	public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
			//TODO
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens()){
				
				
			}
			
			//TODO for each points, find the closest centroid
			
            Point p = new Point(key.toString());    
            assertTrue(p.getDimension() == dimension, "Invalid Dimension");
            // Map all the points to the same key, so reducer can find centroids
            //TODO, mapper's output key and value
            context.write(KMeans.one, p);
        }

}
