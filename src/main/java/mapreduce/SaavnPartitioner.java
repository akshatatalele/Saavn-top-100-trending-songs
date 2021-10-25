package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SaavnPartitioner extends Partitioner<Text,Text >{

	@Override
	public int getPartition(Text arg0, Text arg1, int numReduceTask) {
		// TODO Auto-generated method stub
		String[] st = arg1.toString().split(",");
		Integer dateValue = Integer.parseInt(st[4].split("-")[2]);
		/*
		 * Based on the date in records, records are partitioned to reduce task
		 */
		if(dateValue == 24){
			return 0;
		}
		else if(dateValue == 25){
			return 1 % numReduceTask;
		}
		else if(dateValue == 26){
			return 2 % numReduceTask;
		}
		else if(dateValue == 27){
			return 3 % numReduceTask;
		}
		else if(dateValue == 28){
			return 4 % numReduceTask;
		}
		else if(dateValue == 29){
			return 5 % numReduceTask;
		}
		else if(dateValue == 30){
			return 6 % numReduceTask;
		}
		
		return 0;
	}

}
