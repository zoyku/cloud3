import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;

public class Hw3 {

    public static class TotalMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final static Text word = new Text("Total Salary:");
        private DoubleWritable salary = new DoubleWritable();
        private boolean isHeader = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return;
            }

            String[] fields = value.toString().split(",");
            double sal = Double.parseDouble(fields[6]);
            salary.set(sal);
            context.write(word, salary);
        }
    }

    public static class TotalReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class JobTitleMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text jobTitle = new Text();
        private DoubleWritable salary = new DoubleWritable();
        private boolean isHeader = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return;
            }

            String[] fields = value.toString().split(",");
            jobTitle.set(fields[3]);
            double sal = Double.parseDouble(fields[6]);
            salary.set(sal);
            context.write(jobTitle, salary);
        }
    }

    public static class TitleExperienceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text jobExp = new Text();
        private DoubleWritable salary = new DoubleWritable();
        private boolean isHeader = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return;
            }

            String[] fields = value.toString().split(",");
            String jobTitle = fields[3];
            String experience = fields[1];
            jobExp.set(jobTitle + "_" + experience);
            double sal = Double.parseDouble(fields[6]);
            salary.set(sal);
            context.write(jobExp, salary);
        }
    }

    public static class EmployeeResidenceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text residence = new Text();
        private DoubleWritable salary = new DoubleWritable();
        private boolean isHeader = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return;
            }

            String[] fields = value.toString().split(",");
            String empResidence = fields[7];
            double sal = Double.parseDouble(fields[6]);

            if (empResidence.equals("US")) {
                residence.set("US");
            } else {
                residence.set("Non-US");
            }
            salary.set(sal);
            context.write(residence, salary);
        }
    }

    public static class AverageYearMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text yearPartition = new Text();
        private DoubleWritable salary = new DoubleWritable();
        private boolean isHeader = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return;
            }
            
            String[] fields = value.toString().split(",");
            int workYear = Integer.parseInt(fields[0]);
            double sal = Double.parseDouble(fields[6]);

            if (workYear == 2024) {
                yearPartition.set("2024");
            } else if (workYear == 2023) {
                yearPartition.set("2023");
            } else {
                yearPartition.set("Before 2023");
            }
            salary.set(sal);
            context.write(yearPartition, salary);
        }
    }

    public static class AvgSalaryReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            result.set(sum / count);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Hw3");
        job.setJarByClass(Hw3.class);
        if (args[0].equals("total")) {
            job.setMapperClass(TotalMapper.class);
            job.setReducerClass(TotalReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
        }
        else if (args[0].equals("jobtitle")) {
            job.setMapperClass(JobTitleMapper.class);
            job.setReducerClass(AvgSalaryReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
        }
        else if (args[0].equals("titleexperience")) {
            job.setMapperClass(TitleExperienceMapper.class);
            job.setReducerClass(AvgSalaryReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
        }
        else if (args[0].equals("employeeresidence")) {
            job.setMapperClass(EmployeeResidenceMapper.class);
            job.setReducerClass(AvgSalaryReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
        }
        else if (args[0].equals("averageyear")) {
            job.setMapperClass(AverageYearMapper.class);
            job.setReducerClass(AvgSalaryReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
        }
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        /*FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);*/
    }
}