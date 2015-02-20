package com.magnetic.nano_time_udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

public class NanoTimeBinary extends EvalFunc<DataByteArray> {

	@Override
	public DataByteArray exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		try {
			return new DataByteArray(NanoTimeConverter.toNanoTime(((Integer)input.get(0)).longValue(), false));
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}

}
