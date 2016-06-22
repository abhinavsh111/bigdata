package com.apple.irpt.df.jobs.kvs.dataWrite.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class KVSReducer extends Reducer<Text, NullWritable, Text, NullWritable>{

    public static final String UNMAPPED_KEY = "Unmappedkey";

    enum MAP_COUNTER {
        ID_COUNTER
    }
    MultipleOutputs<Text, NullWritable> mos;

    protected void setup(Context context) throws IOException,InterruptedException {
        mos = new MultipleOutputs<Text, NullWritable>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<NullWritable> value, Context context) throws IOException,InterruptedException {
        context.getCounter(MAP_COUNTER.ID_COUNTER).increment(1);
        String id = System.currentTimeMillis()/1000+""+context.getTaskAttemptID().getTaskID().getId()+context.getCounter(MAP_COUNTER.ID_COUNTER).getValue();
        mos.write(UNMAPPED_KEY, new Text(id+"\t"+key), NullWritable.get(), context.getConfiguration().get(UNMAPPED_KEY));
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        mos.close();
    }
}
