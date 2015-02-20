An Apache Pig UDF to transform milliseconds to a Parquet INTERVAL data type (NanoTime).

An example pig script that convert Avro to Parquet:

	-- parameters required:
	-- IN  = Avro input glob to be read from HDFS
	-- OUT = Target Parquet file

	SET job.name 'avro_to_parquet';

	REGISTER /srv/magnetic_hadoop-new/lib/external/avro-1.7.4.jar
	REGISTER /srv/magnetic_hadoop-new/lib/external/json-simple-1.1.jar
	REGISTER /srv/magnetic_hadoop-new/lib/external/jackson-core-asl-1.8.8.jar
	REGISTER /srv/magnetic_hadoop-new/lib/external/jackson-mapper-asl-1.8.8.jar
	REGISTER /srv/magnetic_hadoop-new/lib/external/magnetic_avrostorage-0.0.3-SNAPSHOT.jar
	REGISTER /srv/stanislav/magnetic_hadoop.git/lib/external/nano-time-udf-0.0.1-SNAPSHOT.jar

	DEFINE toNanoTimeBinary com.magnetic.nano_time_udf.NanoTimeBinary;

	in = LOAD '$IN'
    	USING com.magnetic.pig.storage.avro.UnorderedSparseSchemaAvroStorage();

	in_nanotime = FOREACH in GENERATE *, toNanoTimeBinary(log_timestamp) as log_nanotime;

	RMF $OUT;

	out = STORE in_nanotime INTO '$OUT'
    	USING parquet.pig.ParquetStorer();
