import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.IOException;

public class BloomFilterConstruct {

    public static class BloomMapper extends Mapper<Object, Text, NullWritable, BloomFilter> {


        private BloomFilter filter = new BloomFilter(100000, 3, 1);

        @Override
        public void map(Object key, Text value, Context context) {
            String[] splits = value.toString().split(",");

            if (Integer.parseInt(splits[1]) > 80 && Integer.parseInt(splits[2]) > 80 && Integer.parseInt(splits[3]) > 80) {
                filter.add(new Key(splits[0].getBytes()));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), filter);
        }
    }

    public static class BloomReducer extends Reducer<NullWritable, BloomFilter, NullWritable, NullWritable> {

        private BloomFilter filter = new BloomFilter(100000, 3, 1);

        @Override
        public void reduce(NullWritable key, Iterable<BloomFilter> values, Context context) throws IOException, InterruptedException{
            for (BloomFilter inputFilter : values) {
                filter.or(inputFilter);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {


            filter.write(FileSystem.get(context.getConfiguration()).create(new Path("/tmp/filter.bloom")));
        }


    }

    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BloomFilter");

        job.setJarByClass(BloomFilterConstruct.class);
        job.setOutputKeyClass(BloomFilter.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(BloomMapper.class);
        job.setReducerClass(BloomReducer.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(BloomFilter.class);

        long start = System.currentTimeMillis();
        int exit = job.waitForCompletion(true) ? 0 : 1;
        System.out.println(System.currentTimeMillis() - start);
        System.exit(exit);
    }
}