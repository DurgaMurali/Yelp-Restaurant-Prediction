

import java.io.IOException;
import java.util.StringTokenizer;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import java.util.Iterator;

public class ExtractRatings {

  public static class ReviewMapper extends Mapper<Object, Text, Text, Text>
  {
    private static final int USER_WISE_MAP = 1;
    private static final int ITEM_WISE_MAP = 2;


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
      Configuration conf = context.getConfiguration();
      int MapKey = Integer.parseInt(conf.get("MapKey"));

      StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
      Text reviewKey = new Text();
      Text reviewValue = new Text();

      while(itr.hasMoreTokens())
      {
        if(MapKey == USER_WISE_MAP)
        {
          reviewKey.set(itr.nextToken());
          reviewValue.set(itr.nextToken().toString() + ":" + itr.nextToken().toString());
        }
        else if(MapKey == ITEM_WISE_MAP)
        {
          String user = itr.nextToken().toString();
          reviewKey.set(itr.nextToken());
          reviewValue.set(user + ":" + itr.nextToken().toString());
        }
      }

      context.write(reviewKey, reviewValue);
    }
  }

  public static class ReviewReducer extends Reducer<Text,Text,Text,Text> 
  {
    java.util.Set<String> userItemSet = new java.util.HashSet<String>();

    MultipleOutputs<Text, Text> outFiles;

    protected void setup(Context context) throws IOException, InterruptedException 
    {
        outFiles = new MultipleOutputs<Text, Text>(context);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException 
    {
        outFiles.close();
    }

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException 
    {
      Text userItemText = new Text(":");
      Text ratingText = new Text(":");

      userItemSet.clear();
      
      for (Text val : values)
      {
        String[] valList = val.toString().split(":");
        if(!userItemSet.contains(valList[0]))
        {
          userItemSet.add(valList[0]);
          userItemText.set(userItemText.toString() + valList[0] + ", " );
          ratingText.set(ratingText.toString() + valList[1] + ", " );
        }
      }
      outFiles.write("UserItem", key, userItemText);
      outFiles.write("Rating", key, ratingText);
    }
  }

  public static void main(String[] args) throws Exception
  {
    Configuration conf = new Configuration();
    conf.set("MapKey", args[0]);
    Job job = Job.getInstance(conf, "Extract Reviews");
    job.setJarByClass(ExtractRatings.class);
    job.setMapperClass(ReviewMapper.class);
    job.setReducerClass(ReviewReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    MultipleOutputs.addNamedOutput(job, "UserItem", TextOutputFormat.class, Text.class, Text.class);
    MultipleOutputs.addNamedOutput(job, "Rating", TextOutputFormat.class, Text.class, Text.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}