import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author Akshay
 * 
 * THIS PROGRAM EXPECTS A FILENAME AS COMMAND LINE ARGUMENT
 * 
 * References: 1. https://github.com/chrishavlin/nyc_taxi_viz/blob/master/src/taxi_plotmod.py
 *             2. https://chrishavlin.wordpress.com/2016/10/16/taxi-tracers-mapping-nyc/
 */
public class GridFileWriter {

    //WARNING: CHANGING ANY OF THESE CONSTANTS WITHOUT CORRESPONDING CHANGES TO 
    //GridAppenderMapper WILL PRODUCE ERRONEOUS OUTPUTS
    static final int NUMBER_LONGITUDINAL_BINS = 300;
    static final int NUMBER_LATITUDINAL_BINS = 500;
    static final String FILE_HEADER = "grid_id,center_latitude,center_longitude";
    //Some useful lat/lon points of NewYork:
    static final double[] bottom_left= {40.61, -74.06};
    static final double[] top_left = {40.91, -74.06};
    static final double[] bottom_right = {40.61, -73.77};
    static final double[] top_right= {40.91,-73.77};

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
    
    public static void main(String[] args) throws FileNotFoundException, IOException 
    {
        double[] longitudinal_centers = divideLongitude(bottom_left[1],bottom_right[1]);
        double[] latitudinal_centers = divideLatitude(bottom_left[0],top_left[0]);
        
        FileWriter writer = new FileWriter(args[0]);
        writer.append(FILE_HEADER.toString());
        writer.append("\n");
        
        for(int i=0;i<NUMBER_LONGITUDINAL_BINS;i++)
        {
            for(int j=0;j<NUMBER_LATITUDINAL_BINS;j++)
            {
                StringBuilder line = new StringBuilder();
                //grid id
                line.append(i*NUMBER_LATITUDINAL_BINS+j);
                line.append(",");
                // grid latitude
                line.append(latitudinal_centers[j]);
                line.append(",");
                //grid longitude
                line.append(longitudinal_centers[i]);
                line.append("\n");
                
                writer.append(line.toString());
            }
        }
        System.out.println("CSV file was created successfully at " + args[0]);
        writer.flush();
        writer.close();
    }
}
