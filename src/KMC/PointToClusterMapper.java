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
public class PointToClusterMapper extends Mapper<LongWritable, Text, Point, Point>
{
	
	
	public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
			ArrayList<Point> centroids = KMeans.centroids;
		
			String inputString = value.toString();
			String[] tokens = inputString.split("\\s+");
			
			if(centroids == null || centroids.size() == 0){
				System.out.println("centroids is null or empty, cannot figure out the point's dimension");
			}else{
				int dim = centroids.get(0).dimension;
				
				for(int i = 0; i < tokens.length; i+=dim){
					Point p = new Point(dim);
					float minDis = Float.MAX_VALUE;
					Point closestCentroid = new Point(dim);
					
					//construct a point from input token
					for(int j = i; j < dim + i; j++){
						p.pointCoord.set(j, Float.parseFloat(tokens[j]));
					}
					
					//find the closet centroid
					for(int k = 0; k < centroids.size(); k++){
						if(Point.distance(p, centroids.get(k)) < minDis){
							closestCentroid = centroids.get(k);
							minDis = Point.distance(p, centroids.get(k));
						}
					}
					
					context.write(closestCentroid, p);
				}
			}
        }

}
