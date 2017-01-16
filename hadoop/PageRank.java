import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * Created by charles on 17-1-10.
 */
public class PageRank {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text page = new Text();
        private Text link = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString().replace('"', ' '));

            page.set(itr.nextToken());
            link.set(itr.nextToken());
            context.write(page, link);
        }
    }

    public static class LinksReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String result = "";
            Set<String> res_set = new HashSet();

            for (Text val : values) {
                res_set.add(val.toString());
            }
            for (String str : res_set) {
                if (result.length() > 0) {
                    result += ",";
                }
                result += str;
            }
            context.write(new Text(key.toString() + ",1.0"), new Text(result));
        }
    }


    public static class PrMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().trim().split("\t");
            String[] page_rank = line[0].split(",");
            Text page = new Text(page_rank[0]);
            float rank = 1.0f;
            if (page_rank.length == 2) {
                rank = Float.parseFloat(page_rank[1]);
            }

            if (line.length == 1) {
                context.write(page, new Text(page_rank[0] + ";" + page_rank[1]));
            } else {
                String[] links = line[1].split(",");
                float new_rank = rank / links.length;

                for (String link : links) {
                    context.write(new Text(link), new Text(page_rank[0] + ";" + String.valueOf(new_rank)));
                }
                context.write(page, new Text(page_rank[0] + ";0"));
                context.write(page, new Text(line[1]));
            }
        }
    }


    public static class PrCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String links = "";
            Set<String> res_set = new HashSet<String>();

            for (Text val : values) {
                res_set.add(val.toString());
            }

            boolean first = true;
            for (String link : res_set) {
                if (first) {
                    links += link;
                    first = false;
                } else {
                    links += "," + link;
                }
            }
            context.write(new Text(key.toString().split(",")[0]), new Text(links));
        }
    }


    public static class PrReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float rank = 0.0f;
            String links = "";
            String[] res;
            Set<String> res_set = new HashSet<String>();

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                for (String part : parts) {
                    res = part.toString().split(";");
                    if (res.length == 2) {
                        rank += Float.parseFloat(res[1]);
                    } else {
                        res_set.add(part);
                    }
                }
            }

            boolean first = true;
            for (String link : res_set) {
                if (first) {
                    links += link;
                    first = false;
                } else {
                    links += "," + link;
                }
            }
            context.write(new Text(key.toString().split(",")[0] + "," + String.valueOf(0.85*rank + 0.15)), new Text(links));
        }
    }

    public static void main(String[] args) throws Exception {
        long begin = System.currentTimeMillis();
        Configuration conf_pre = new Configuration();
        Job job_pre = Job.getInstance(conf_pre, "Pre-PageRank");
        job_pre.setJarByClass(PageRank.class);
        job_pre.setMapperClass(TokenizerMapper.class);
        job_pre.setCombinerClass(LinksReducer.class);
        job_pre.setReducerClass(LinksReducer.class);
        job_pre.setOutputKeyClass(Text.class);
        job_pre.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job_pre, new Path(args[0]));
        FileOutputFormat.setOutputPath(job_pre, new Path(args[1]));
        job_pre.waitForCompletion(true);
        long end = System.currentTimeMillis() - begin;
        System.out.println("预处理耗时：" + end + "毫秒");

        String[] paths = args[1].split("/");
        String new_path = "";
        int len = paths.length;
        for (int i=0; i<len-1; i++) {
            new_path += paths[i] + "/";
        }
        new_path += paths[len-1].substring(0, paths[len-1].length()-1);

        for (int i=0; i<10; i++) {
            long _begin = System.currentTimeMillis();
            String[] otherArgs = {new_path + String.valueOf(i) + "/part-r-00000",
                    new_path + String.valueOf(i+1)};
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "PageRank"+ String.valueOf(i+1));
            job.setJarByClass(PageRank.class);
            job.setMapperClass(PrMapper.class);
            job.setCombinerClass(PrCombiner.class);
            job.setReducerClass(PrReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
            job.waitForCompletion(true);
            long _end = System.currentTimeMillis() - _begin;
            System.out.println("第" + String.valueOf(i+1) + "轮耗时：" + _end + "毫秒");
        }
        System.exit(0);
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
