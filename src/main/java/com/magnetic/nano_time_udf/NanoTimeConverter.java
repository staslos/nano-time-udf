package com.magnetic.nano_time_udf;

import java.util.Calendar;
import java.util.TimeZone;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import jodd.datetime.JDateTime;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Inspiration from https://github.com/apache/hive/blob/release-0.14.0/ql/src/java/org/apache/hadoop/hive/ql/io/parquet/timestamp/NanoTimeUtils.java
 * and https://github.com/apache/hive/blob/release-0.14.0/ql/src/java/org/apache/hadoop/hive/ql/io/parquet/timestamp/NanoTime.java
 */
public class NanoTimeConverter {
	static final long NANOS_PER_HOUR = TimeUnit.HOURS.toNanos(1);
	static final long NANOS_PER_MINUTE = TimeUnit.MINUTES.toNanos(1);
	static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

	private static final ThreadLocal<Calendar> parquetGMTCalendar = new ThreadLocal<Calendar>();
	private static final ThreadLocal<Calendar> parquetLocalCalendar = new ThreadLocal<Calendar>();

	private static Calendar getGMTCalendar() {
		// Calendar.getInstance calculates the current-time needlessly, so cache
		// an instance.
		if (parquetGMTCalendar.get() == null) {
			parquetGMTCalendar.set(Calendar.getInstance(TimeZone
					.getTimeZone("GMT")));
		}
		return parquetGMTCalendar.get();
	}

	private static Calendar getLocalCalendar() {
		if (parquetLocalCalendar.get() == null) {
			parquetLocalCalendar.set(Calendar.getInstance());
		}
		return parquetLocalCalendar.get();
	}

	private static Calendar getCalendar(boolean skipConversion) {
		return skipConversion ? getLocalCalendar() : getGMTCalendar();
	}

	public static byte[] toNanoTime(long timestamp, boolean skipConversion) {

		Calendar calendar = getCalendar(skipConversion);
		calendar.setTime(new Date(timestamp));
		JDateTime jDateTime = new JDateTime(calendar.get(Calendar.YEAR),
				calendar.get(Calendar.MONTH) + 1, // java calendar index starting at 1.
				calendar.get(Calendar.DAY_OF_MONTH));
		int days = jDateTime.getJulianDayNumber();
		long hour = calendar.get(Calendar.HOUR_OF_DAY);
		long minute = calendar.get(Calendar.MINUTE);
		long second = calendar.get(Calendar.SECOND);
		long nanos = 0;
		long nanosOfDay = nanos + NANOS_PER_SECOND * second + NANOS_PER_MINUTE
				* minute + NANOS_PER_HOUR * hour;

		ByteBuffer buf = ByteBuffer.allocate(12);
	    buf.order(ByteOrder.LITTLE_ENDIAN);
	    buf.putLong(nanosOfDay);
	    buf.putInt(days);
	    buf.flip();
	    
		return buf.array();
	}
}
