/*
 *
 * 爱用物流分库分表的算法
 * 首先由listId按分区来确认分片id，然后再用listId取余获取具体表的后缀
 *
 *
 */






package io.mycat.route.function;

import io.mycat.config.model.rule.RuleAlgorithm;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.util.LinkedList;

/**
 * number column partion by Mod operator
 * if count is 10 then 0 to 0,21 to 1 (21 % 10 =1)
 * @author wuzhih
 *
 */
public class PartitionByAiyong extends AbstractPartitionAlgorithm implements RuleAlgorithm {



	private String mapFile;
	private PartitionByRangeMod.LongRange[] longRanges;

	private int count;
	private int tablecount;

	@Override
	public void init() {

		initialize();
	}
	public void setMapFile(String mapFile) {
		this.mapFile = mapFile;
	}


	//由分片规则文件获取分片规则
	private void initialize() {
		BufferedReader in = null;
		try {
			InputStream fin = this.getClass().getClassLoader()
					.getResourceAsStream(mapFile);
			if (fin == null) {
				throw new RuntimeException("can't find class resource file "
						+ mapFile);
			}
			in = new BufferedReader(new InputStreamReader(fin));
			LinkedList<PartitionByRangeMod.LongRange> longRangeList = new LinkedList<PartitionByRangeMod.LongRange>();

			for (String line = null; (line = in.readLine()) != null;) {
				line = line.trim();
				if (line.startsWith("#") || line.startsWith("//")) {
					continue;
				}
				int ind = line.indexOf('=');
				if (ind < 0) {
					System.out.println(" warn: bad line int " + mapFile + " :"
							+ line);
					continue;
				}
				String pairs[] = line.substring(0, ind).trim().split("-");
				long longStart = NumberParseUtil.parseLong(pairs[0].trim());
				long longEnd = NumberParseUtil.parseLong(pairs[1].trim());
				int nodeId = Integer.parseInt(line.substring(ind + 1)
						.trim());
				longRangeList
						.add(new PartitionByRangeMod.LongRange(nodeId, longStart, longEnd));

			}
			longRanges = longRangeList.toArray(new PartitionByRangeMod.LongRange[longRangeList.size()]);
		} catch (Exception e) {
			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			} else {
				throw new RuntimeException(e);
			}

		} finally {
			try {
				in.close();
			} catch (Exception e2) {
			}
		}
	}


	public void setCount(int count) {
		this.count = count;
	}
	public void setTablecount(int tablecount) {
		this.tablecount = tablecount;
	}

	@Override
/*
*
* 分表规则
*
* */

	public Integer calculateTables(String columnValue)  {
//		columnValue = NumberParseUtil.eliminateQoute(columnValue);
		try {


			BigInteger bigNum = new BigInteger(columnValue).abs();

			return (bigNum.mod(BigInteger.valueOf(10))).intValue();
		} catch (NumberFormatException e){
			throw new IllegalArgumentException(new StringBuilder().append("columnValue:").append(columnValue).append(" Please eliminate any quote and non number within it.").toString(),e);
		}

	}
/*
* 分片规则
*
*
* */

	public Integer calculate(String columnValue) {
//		columnValue = NumberParseUtil.eliminateQoute(columnValue);

		Integer listId = Integer.parseInt(columnValue);

		int dataMod = listId % tablecount;
		try {

			Integer rst = null;
			int nodeIndex = 0;
			for (PartitionByRangeMod.LongRange longRang : this.longRanges) {
				if (dataMod <= longRang.valueEnd && dataMod >= longRang.valueStart) {
					return longRang.groupSize - 1;

				}
			}
			//数据超过范围，暂时使用配置的默认节点

			return nodeIndex;
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(new StringBuilder().append("columnValue:").append(columnValue).append(" Please eliminate any quote and non number within it.").toString(), e);
		}


	}

	@Override
	public int getPartitionNum() {
		int nPartition = this.count;
		return nPartition;
	}


	public static void main(String[] args)  {
//		hashTest();
		PartitionByAiyong partitionByMod = new PartitionByAiyong();
		partitionByMod.count=8;
		partitionByMod.calculate("\"6\"");
		partitionByMod.calculate("\'6\'");
	}
}