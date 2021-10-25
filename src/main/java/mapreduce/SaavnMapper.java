package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SaavnMapper extends Mapper<LongWritable, Text, Text, Text> {

	/**
	 * Example input line:
	 * I8E7jszF,8b5f6437ebfcea428a3db24e09d13ee0,1512143128,15,2017-12-01
	 *
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] st = value.toString().split(",");
		Integer date = 0;
		try {
			if (st.length == 5) {
				date = Integer.parseInt(st[4].split("-")[2]);
				/*
				 * Filters the data by 1. Considering Stream data from 17 Hr to
				 * 22 Hr 2. Date value from 24 to 30
				 */
				if ((Integer.parseInt(st[3]) >= 19 || Integer.parseInt(st[3]) <= 22) && date < 31 && date > 23) {
					context.write(new Text(st[4]), value);
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}

	}

}
