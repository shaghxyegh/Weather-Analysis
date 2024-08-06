package org.homa;



import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WA_Reducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(new Text(" Date , MaxTemp , MinTemp , AvgTemp , Humidity , PrecipProb , PrecipType , Hot , Anomaly "), new Text());

        double maxTemp = Double.MIN_VALUE;
        double minTemp = Double.MAX_VALUE;
        double sumTemp = 0;
        int count = 0;
        double humidity = 0;
        double precipprob = 0;
        String preciptype = "";
        double[] tempList = new double[32];

        // First pass: collect data and calculate sum for mean
        for (Text value : values) {
            String[] fields = value.toString().split(",");
            double tempmax = Double.parseDouble(fields[0]);
            double tempmin = Double.parseDouble(fields[1]);
            double temp = Double.parseDouble(fields[2]);
            tempList[count] = temp;
            humidity = Double.parseDouble(fields[3]);
            precipprob = Double.parseDouble(fields[4]);
            preciptype = fields[5];

            if (tempmax > maxTemp) {
                maxTemp = tempmax;
            }
            if (tempmin < minTemp) {
                minTemp = tempmin;
            }
            sumTemp += temp;
            count++;
        }

        double avgTemp = sumTemp / count;
        String anomaly = (maxTemp < avgTemp || avgTemp < minTemp) ? "anomaly" : "not anomaly";
        String hot = (maxTemp > 30) ? "hot" : "not hot";
        String result = String.format("%f,%f,%f,%f,%f,%s,%s,%s", maxTemp, minTemp, avgTemp, humidity, precipprob, preciptype, hot, anomaly);

        context.write(key, new Text(result));
    }


}
