## mysql-cdc connector源码解析
> 首先会有一个对应的TableFactory，然后在工厂类里面构造相应的source， 
> 最后将消费下来的数据转成flink认识的RowData格式，发送到下游.

    在flink-connector-mysql-cdc module中，找到其对应的工厂类：MySQLTableSourceFactory，
    进入createDynamicTableSource(Context context)方法，在这个方法里，
    使用从ddl中的属性里获取的host、dbname等信息构造了一个MySQLTableSource类。

## MySQLTableSource
    在MySQLTableSource#getScanRuntimeProvider方法里，
    首先构造了一个用于序列化的对象RowDataDebeziumDeserializeSchema，
    这个对象主要是用于将Debezium获取的SourceRecord格式的数据转化为flink认识的RowData对象。 
    RowDataDebeziumDeserializeSchem#deserialize方法，这里的操作主要就是先判断下进来的
    数据类型（insert 、update、delete），然后针对不同的类型（short、int等）分别进行转换，
    最后flink用于获取数据库变更日志的Source函数是DebeziumSourceFunction，且最终返回的类型是RowData。
    也就是说flink底层是采用了Debezium工具从mysql、postgres等数据库中获取的变更数据

## DebeziumSourceFunction
    DebeziumSourceFunction类继承了RichSourceFunction，并且实现了CheckpointedFunction接口，
    也就是说这个类是flink的一个SourceFunction，会从源端（run方法）获取数据，发送给下游。
    此外这个类还实现了CheckpointedFunction接口，也就是会通过checkpoint的机制来保证exactly once语义。

    接下来我们进入run方法，看看是如何获取数据库的变更数据的。具体见类中注释com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction

    在函数的开始，设置了很多的properties，比如include.schema.changes 设置为false，也就是不包含表的DDL操作，
    表结构的变更是不捕获的。我们这里只关注数据的增删改。

    接下来构造了一个DebeziumChangeConsumer对象，这个类实现了DebeziumEngine.ChangeConsumer接口，
    主要就是将获取到的一批数据进行一条条的加工处理。

    接下来定一个DebeziumEngine对象，这个对象是真正用来干活的，它的底层使用了kafka的connect-api来进行获取数据，
    得到的是一个org.apache.kafka.connect.source.SourceRecord对象。 
    通过notifying方法将得到的数据交给上面定义的DebeziumChangeConsumer来来覆盖缺省实现以进行复杂的操作。

    接下来通过一个线程池ExecutorService来异步的启动这个engine。

    最后，做了一个循环判断，当程序被打断，或者有错误的时候，打断engine，并且抛出异常。
    
    总结一下，就是在Flink的source函数里，使用Debezium 引擎获取对应的数据库变更数据（SourceRecord），
    经过一系列的反序列化操作，最终转成了flink中的RowData对象，发送给下游。

----------------------------------------------------------------------------------------------------------------------------
## changelog format
    从mysql-cdc获取数据库的变更数据，或者写了一个group by的查询的时候，这种结果数据都是不断变化的，
    我们如何将这些变化的数据发到只支持append mode的kafka队列呢?于是flink提供了一种changelog  format，
    简单的理解为，*flink对进来的RowData数据进行了一层包装，然后加了一个数据的操作类型*，
    包括以下几种 INSERT,DELETE,  UPDATE_BEFORE,UPDATE_AFTER。这样当下游获取到这个数据的时候，
    就可以根据数据的类型来判断下如何对数据进行操作了。
  ```
  原始数据格式
  {"day":"2020-06-18","gmv":100}
  经过changelog格式的加工之后
  {"data":{"day":"2020-06-18","gmv":100},"op":"+I"}
  
  changelog format对原生的格式进行了包装，添加了一个op字段，表示数据的操作类型，目前有以下几种：
    +I：插入操作。
    -U ：更新之前的数据内容：
    +U ：更新之后的数据内容。
    -D ：删除操作。
```


----------------------------------------------------------------------------------------------------------------------------
## flink format 主要是序列化和反序列化
    changelog-json 使用了flink-json包进行json的处理
```
    反序列化用的是ChangelogJsonDeserializationSchema类，在其构造方法里
    主要是构造了一个json的序列化器jsonDeserializer用于对数据进行处理。
    public ChangelogJsonDeserializationSchema(
			RowType rowType,
			TypeInformation<RowData> resultTypeInfo,
			boolean ignoreParseErrors,
			TimestampFormat timestampFormatOption) {
		this.resultTypeInfo = resultTypeInfo;
		this.ignoreParseErrors = ignoreParseErrors;
		this.jsonDeserializer = new JsonRowDataDeserializationSchema(
			createJsonRowType(fromLogicalToDataType(rowType)),
			// the result type is never used, so it's fine to pass in Debezium's result type
			resultTypeInfo,
			false, // ignoreParseErrors already contains the functionality of failOnMissingField
			ignoreParseErrors,
			timestampFormatOption);
	}
	
	
	
	其中createJsonRowType方法指定了changelog的format是一种Row类型的格式
	private static RowType createJsonRowType(DataType databaseSchema) {
		DataType payload = DataTypes.ROW(
			DataTypes.FIELD("data", databaseSchema),
			DataTypes.FIELD("op", DataTypes.STRING()));
		return (RowType) payload.getLogicalType();
	}
	指定了这个row格式有两个字段，一个是data，表示数据的内容，一个是op，表示操作的类型
	
	最核心的ChangelogJsonDeserializationSchema#deserialize(byte[] bytes, Collector<RowData> out>)
	
	@Override
	public void deserialize(byte[] bytes, Collector<RowData> out) throws IOException {
		try {
			GenericRowData row = (GenericRowData) jsonDeserializer.deserialize(bytes);
			GenericRowData data = (GenericRowData) row.getField(0);
			String op = row.getString(1).toString();
			RowKind rowKind = parseRowKind(op);
			data.setRowKind(rowKind);
			out.collect(data);
		} catch (Throwable t) {
			// a big try catch to protect the processing.
			if (!ignoreParseErrors) {
				throw new IOException(format(
					"Corrupt Debezium JSON message '%s'.", new String(bytes)), t);
			}
		}
	}
	使用jsonDeserializer对数据进行处理，然后对第二个字段op进行判断，添加对应的RowKind。
```


```
序列化 ChangelogJsonSerializationSchema#serialize
	@Override
	public byte[] serialize(RowData rowData) {
		reuse.setField(0, rowData);
		reuse.setField(1, stringifyRowKind(rowData.getRowKind()));
		return jsonSerializer.serialize(reuse);
	}
```