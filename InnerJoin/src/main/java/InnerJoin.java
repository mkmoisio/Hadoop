import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

public class InnerJoin {


    public static  class StudentMapper extends Mapper<Object, Text, Text, Text> {

        private BloomFilter filter = new BloomFilter();
        private Configuration conf;

        @Override
        public void setup(Context context) throws IOException, InterruptedException{
            conf = context.getConfiguration();
          //  URI[] uris = Job.getInstance(conf).getCacheFiles();
            //Path path = new Path(uris[0].getPath());

            DataInputStream dataInputStream = new DataInputStream(
                    new FileInputStream(context.getConfiguration().get("filter")));


            filter.readFields(dataInputStream);
            dataInputStream.close();

        }


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] row = value.toString().split(",");

            String studentId = row[0];
            String name = row[1];
            String yearOfBirthString = row[2];

            if (Integer.parseInt(yearOfBirthString) > 1989) {
         //       System.out.println("id: " + studentId + ", name: " + name + ", scores: " + scoresRow);
             //   if (filter.membershipTest(new Key(studentId.getBytes()))) {
                    context.write(new Text(studentId), new Text(name + "," + yearOfBirthString));
            //    }
            }
        }
    }

    public static class ScoreMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split(",");

            String studentId = row[0];
            String score1String = row[1];
            String score2String = row[2];
            String score3String = row[3];


            if (Integer.parseInt(score1String) > 80
                    && Integer.parseInt(score2String) > 80
                    && Integer.parseInt(score3String) > 80)
            {

                context.write(new Text(studentId), new Text("s"+score1String + "," + score2String + "," + score3String));
            }

        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, NullWritable> {


        public void reduce(Text studentId, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            String id = studentId.toString();
            String student = null;
            String score = null;
            for (Text text : values) {
                if  (text.toString().charAt(0) == 's'){
                    score = text.toString().substring(1);
                } else {
                    student = text.toString();
                }
            }

            if (student != null && score != null) {
                context.write(new Text(id + "," + student + "," + score), NullWritable.get());
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Join");
        Path bloomFilterPath = new Path(args[2]);

        job.addCacheFile(bloomFilterPath.toUri());
        job.getConfiguration().set("filter", bloomFilterPath.getName());


        job.setJarByClass(InnerJoin.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(StudentMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //    FileInputFormat.addInputPath(job, new Path(args[0]));


        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, StudentMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ScoreMapper.class);
        //  job.setInputFormatClass(TextInputFormat.class);
        //  job.setMapperClass(StudentMapper.class);
        //  job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setCombinerClass(JoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        long start = System.currentTimeMillis();
        int exit = job.waitForCompletion(true) ? 0 : 1;
        System.out.println(System.currentTimeMillis() - start);
        System.exit(exit);
    }

}
