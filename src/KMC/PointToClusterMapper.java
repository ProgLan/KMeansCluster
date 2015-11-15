package KMC;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * You can modify this class as you see fit.  You may assume that the global
 * centroids have been correctly initialized.
 */
public class PointToClusterMapper extends Mapper<Text, Text, IntWritable, Point>
{
	
	
	public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException
        {
			float minDis = Float.MAX_VALUE;
			Point closest = null;
			Point p = new Point(key.toString());
			int closestIndex = 0;
			
			for(int i = 0; i < KMeans.centroids.size(); i++){
				Point c = new Point(KMeans.centroids.get(i));
				
				float dis = Point.distance(p, c);
				
				if(dis < minDis){
					closest = c;
					minDis = dis;
					closestIndex = i;
				}
			}
			
			context.write(new IntWritable(closestIndex), p);
			
        }

}
