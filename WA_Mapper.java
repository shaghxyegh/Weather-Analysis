package org.homa;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class WA_Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Log LOG = LogFactory.getLog(WA_Mapper.class);
    private boolean isHeader = true;
    private Text key = new Text();
    private Text value = new Text();

    @Override
    protected void map(LongWritable longWritable, Text text, Context context) throws IOException, InterruptedException {
        String line = text.toString();

        // Skip the header row
        if (isHeader)
        {
            isHeader = false;
            return;
        }

        String[] fields = line.split(",");

        String datetime = fields[1];
        String tempmax = fields[2];
        String tempmin = fields[3];
        String temp = fields[4];
        String humidity = fields[8];
        String precipprob = fields[10];
        String preciptype = fields[11];

        String outValue = String.join(",", tempmax, tempmin, temp, humidity, precipprob, preciptype);

        // Write the output
        key.set(datetime);
        value.set(outValue);
        context.write(key, value);
    }
}
