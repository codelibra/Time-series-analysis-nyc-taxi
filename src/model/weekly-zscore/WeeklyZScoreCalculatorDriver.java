import org.apache.hadoop.fs.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.util.*;
import org.apache.hadoop.filecache.*;

public class WeeklyZScoreCalculatorDriver extends Configured implements Tool {

   public static void main(String[] args) throws Exception {
         int res = ToolRunner.run(new Configuration(), new WeeklyZScoreCalculatorDriver(), args);
         System.exit(res);
   }


 public int run(String [] args) throws Exception {
   Configuration conf = getConf();
   conf.set("mapreduce.output.textoutputformat.separator", ",");
   Job job = new Job(conf);
   job.setJarByClass(WeeklyZScoreCalculatorDriver.class);
   DistributedCache.addFileToClassPath(new Path("/user/at3577/taxiAnalyse/jars/stl-decomp-4j-1.0.2.jar"), conf);

   FileInputFormat.addInputPath(job, new Path(args[0]));
   FileOutputFormat.setOutputPath(job, new Path(args[1]));

   job.setMapperClass(WeeklyZScoreCalculatorMapper.class);
   job.setReducerClass(WeeklyZScoreCalculatorReducer.class);
   job.setNumReduceTasks(1);

   job.setOutputKeyClass(Text.class);
   job.setOutputValueClass(Text.class);
   System.exit(job.waitForCompletion(true) ? 0 : 1); 
   return 0;
 }

}

