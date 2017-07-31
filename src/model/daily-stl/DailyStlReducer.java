import org.apache.commons.lang.ArrayUtils; 
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.github.servicenow.ds.stats.stl.SeasonalTrendLoess;
import java.util.*;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DailyStlReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	try {
            Map<String, Double> exisitingValuesMap = new HashMap<>();
            String longitude="";
	    String latitude="";
            for (Text value : values) {
                String[] splitValues = value.toString().split(",");
		longitude = splitValues[1];
		latitude  = splitValues[2];
                exisitingValuesMap.put(splitValues[0], Double.parseDouble(splitValues[3]));
            }

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Calendar calendar = Calendar.getInstance();
            Date startDate = dateFormat.parse("2009-01-01");
            calendar.setTime(startDate);
            Date endDate = dateFormat.parse("2015-12-31");

            ArrayList<Double> freqDataList = new ArrayList<Double>();
            ArrayList<String> aggregatedGridIdList = new ArrayList<String>();
            int dataAvailable = 0;
            int totalData = 0;
            for (Date date = startDate; date.before(endDate); calendar.add(Calendar.DATE, 1)) {
                date = dateFormat.parse(dateFormat.format(calendar.getTime()));

		String gridId = key.toString();		
		for(int hour=0;hour<24;hour++)
                {
			String aggregatedGridId = dateFormat.format(date) + "_" + hour + "_" + gridId;
                	if (exisitingValuesMap.containsKey(aggregatedGridId)) {
                    		freqDataList.add(exisitingValuesMap.get(aggregatedGridId));
                   		dataAvailable++;
                	} else {
         		        freqDataList.add(0.0);
                	}
                	aggregatedGridIdList.add(aggregatedGridId);
                	totalData++;
            	}
	    }
	    System.out.println("Calculated for grid " + key.toString() + " " + dataAvailable + " " + totalData);
            if(dataAvailable < 0.5*totalData)
		return;

	    double[] freqData = ArrayUtils.toPrimitive(freqDataList.toArray(new Double[0]));

            SeasonalTrendLoess.Builder builder = new SeasonalTrendLoess.Builder();
            SeasonalTrendLoess smoother = builder.setPeriodLength(24). // Data has a period of 12
                    setSeasonalWidth(53). // Monthly data smoothed over 35 years
                    setNonRobust(). // Not expecting outliers, so no robustness iterations
                    buildSmoother(freqData);

            SeasonalTrendLoess.Decomposition stl = smoother.decompose();
            double[] seasonal = stl.getSeasonal();
            double[] trend = stl.getTrend();
            double[] residual = stl.getResidual();

            for (int i = 0; i < aggregatedGridIdList.size(); ++i) {
                String output = aggregatedGridIdList.get(i) + "," + longitude + "," + latitude + "," + freqData[i] + "," +seasonal[i] + "," + trend[i] + "," + residual[i];
                context.write(key, new Text(output));
            }
        } catch (Exception e) {
        }
    }
}

