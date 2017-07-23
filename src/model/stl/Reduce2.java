import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.github.servicenow.ds.stats.stl.SeasonalTrendLoess;
import java.util.*;

public class Reduce2 extends Reducer<Text, Text, Text, Text> {
 
public class ObjectComparator implements Comparator{

    public int compare(Object obj1, Object obj2) {
        String str1 = (String)obj1;
        String str2 = (String)obj2;
        Integer year1 = Integer.parseInt(str1.split(",")[0].split("_")[1]);
        Integer week1 =  Integer.parseInt(str1.split(",")[0].split("_")[0]);
        Integer year2 = Integer.parseInt(str2.split(",")[0].split("_")[1]);
        Integer week2 =  Integer.parseInt(str2.split(",")[0].split("_")[0]);
        if(year1.equals(year2)) return week1.compareTo(week2);
        else return year1.compareTo(year2);
   }
}

 @Override
 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {  
     ArrayList<String> valuesList = new ArrayList<String>();
     for(Text value : values){
        valuesList.add(value.toString());
     }

     Collections.sort(valuesList, new ObjectComparator());
     double[] freqData = new double[valuesList.size()];
     
     for(int i=0;i<valuesList.size();++i){
          freqData[i] = Double.parseDouble(valuesList.get(i).split(",")[3]);
     }
     if(valuesList.size()<52*2){
          System.out.println(key.toString()+" -> "+valuesList.size());
          return;
     }
     SeasonalTrendLoess.Builder builder = new SeasonalTrendLoess.Builder();
     SeasonalTrendLoess smoother = builder.
            setPeriodLength(52).    // Data has a period of 12
            setSeasonalWidth(7).   // Monthly data smoothed over 35 years
            setNonRobust().         // Not expecting outliers, so no robustness iterations
            buildSmoother(freqData);

     SeasonalTrendLoess.Decomposition stl = smoother.decompose();
     double[] seasonal = stl.getSeasonal();
     double[] trend = stl.getTrend();
     double[] residual = stl.getResidual();

     for(int i=0;i<valuesList.size();++i){
          String output = valuesList.get(i) +"," + seasonal[i]+","+trend[i]+","+residual[i];
          context.write(key, new Text(output));
     }
 }
}
