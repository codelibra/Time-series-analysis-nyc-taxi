import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper; 

/**
 * @author Akshay
 * This class appends the grid center to input file for both drop-off and pickup coordinates
 * order of appending:
 * pickup gridid, pickup grid long, pickup grid lat, dropoff gridid, dropoff grid long, dropoff grid lat
 */
public class GridAppenderMapper extends Mapper<LongWritable, Text, Text,Text> 
{
    //WARNING: CHANGING ANY OF THESE CONSTANTS WITHOUT CORRESPONDING CHANGES TO 
    // GridFileWriter WILL PRODUCE ERRONEOUS OUTPUTS
    static final int NUMBER_LONGITUDINAL_BINS = 300;
    static final int NUMBER_LATITUDINAL_BINS = 500;
    //Some useful lat/lon points of NewYork:
    static final double[] bottom_left= {40.61, -74.06};
    static final double[] top_left = {40.91, -74.06};
    static final double[] bottom_right = {40.61, -73.77};
    static final double[] top_right= {40.91,-73.77};

    // CONSTANTS OF THIS CLASS-- ASSUME INPUT FILE HEADER
    static final int PICKUP_LATITUDE_INDEX = 3;
    static final int PICKUP_LONGITUDE_INDEX = 2;
    static final int DROPOFF_LATITUDE_INDEX = 5;
    static final int DROPOFF_LONGITUDE_INDEX = 4;
    
    public static double[] divideLongitude(double minLongitude, double maxLongitude)
    {
        double difference = (maxLongitude - minLongitude)/NUMBER_LONGITUDINAL_BINS;
        double current_point = minLongitude;
        double[] center = new double[NUMBER_LONGITUDINAL_BINS];
        for(int i=0;i<NUMBER_LONGITUDINAL_BINS;i++)
        {
            double next_point = current_point + difference;
            center[i] = (current_point + next_point)/2.0;
            current_point = next_point;
        }
        return center;
    }
    
    public static double[] divideLatitude(double minLatitude, double maxLatitude)
    {
        double difference = (maxLatitude - minLatitude)/NUMBER_LATITUDINAL_BINS;
        double current_point = minLatitude;
        double[] center = new double[NUMBER_LATITUDINAL_BINS];
        for(int i=0;i<NUMBER_LATITUDINAL_BINS;i++)
        {
            double next_point = current_point + difference;
            center[i] = (current_point + next_point)/2.0;
            current_point = next_point;
        }
        return center;
    }
    
    /*
     * This is the method which calculates nearest center for a given lat/long
     * Returns: GridId, Grid center lat, Grid center long
     */
    public static double[] getGridCenter(double latitude, double longitude)
    {
        double[] longitudinal_centers = divideLongitude(bottom_left[1],bottom_right[1]);
        double[] latitudinal_centers = divideLatitude(bottom_left[0],top_left[0]);
        
        double longitudinal_diff = Math.abs(longitudinal_centers[1]-longitudinal_centers[0]);
        double latitudinal_diff =  Math.abs(latitudinal_centers[1]-latitudinal_centers[0]);
        
        for(int i=0;i<NUMBER_LONGITUDINAL_BINS;i++)
        {
            for(int j=0;j<NUMBER_LATITUDINAL_BINS;j++)
            {
                int gridID = i*NUMBER_LATITUDINAL_BINS+j;
                if(Math.abs(longitude-longitudinal_centers[i])<=longitudinal_diff/2.0 &&
                   Math.abs(latitude-latitudinal_centers[j])<=latitudinal_diff/2.0)
                {
                    double[] gridCenter = new double[3];
                    gridCenter[0] = gridID;
                    gridCenter[1] = latitudinal_centers[j];
                    gridCenter[2] = longitudinal_centers[i];
                    return gridCenter;
                }
            }
        }
        double[] gridCenter = new double[3];
        gridCenter[0] = -1;
        gridCenter[1] = -1;
        gridCenter[2] = -1;

        return gridCenter;
    }
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {
        String[] inputLine = value.toString().split(",");
        
        try{
        Double pickUpLongitude  = Double.parseDouble(inputLine[PICKUP_LONGITUDE_INDEX]);
        Double pickUpLatitude   = Double.parseDouble(inputLine[PICKUP_LATITUDE_INDEX]);
        double[] pickUpGridCenter = getGridCenter(pickUpLatitude,pickUpLongitude); 
    
          
           
   
        
        
        Double dropOffLongitude  = Double.parseDouble(inputLine[DROPOFF_LONGITUDE_INDEX]);
        Double dropOffLatitude   = Double.parseDouble(inputLine[DROPOFF_LATITUDE_INDEX]);
        double[] dropOffGridCenter = getGridCenter(dropOffLatitude,dropOffLongitude);
  
         
                   
 
       
      
        StringBuilder output = new StringBuilder();
        output.append(value.toString());
        output.append(",");
        
        //pickups
        output.append((int)pickUpGridCenter[0]);
        output.append(",");
        output.append(pickUpGridCenter[2]);
        output.append(",");
        output.append(pickUpGridCenter[1]);
        output.append(",");
        
        //dropoffs
        output.append((int)dropOffGridCenter[0]);
        output.append(",");
        output.append(dropOffGridCenter[2]);
        output.append(",");
        output.append(dropOffGridCenter[1]);
     	

	context.write(new Text(""), new Text(output.toString()));



        }catch(Exception e){}
            
    }   
}
