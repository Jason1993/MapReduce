import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class WordCountMapper extends Mapper<LongWritable, Text, Text, Text>
{
    private Text documentId;
    private Text word = new Text();

    /*
    This method is called once at the start of the map and prior to the map method
    being called. You’ll use this opportunity to store the input filename for this map.
     */
    @Override
    protected void setup(Context context) {
        String filename =
                ((FileSplit) context.getInputSplit()).getPath().getName();
        documentId = new Text(filename);
    }

    /*
    This map method is called once per input line; map tasks are run in parallel
    over subsets of the input files.
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Your value contains an entire line from your file. You tokenize the line
        // using StringUtils (which is far faster than using String.split).
        for (String token : StringUtils.split(value.toString())) {
            word.set(token);
            // For each word your map outputs the word as the key and the document ID as the value.
            context.write(word, documentId);
        }
    }
}
