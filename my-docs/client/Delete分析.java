首先Delete只是针对单行的操作

又可进一步细分:
1. 是针对某个列族
2. 还是针对某个列

同时还可附带一个timestamp时间戳，
所以，还可多出两种组合:
1. 删除等于指定时间戳的数据
2. 删除小于等于指定时间戳的数据

Delete类有6个核心方法:

1. deleteFamily(byte[] family) //删除family这个列族所有版本的数据
2. deleteFamily(byte[] family, long timestamp) //删除family这个列族版本<=timestamp的数据

3. deleteColumns(byte[] family, byte[] qualifier) //删除family.qualifier这个列所有版本的数据
4. deleteColumns(byte[] family, byte[] qualifier, long timestamp) //删除family.qualifier这个列版本<=timestamp的数据

5. deleteColumn(byte[] family, byte[] qualifier) //删除family.qualifier这个列最新版本的数据
6. deleteColumn(byte[] family, byte[] qualifier, long timestamp) //删除family.qualifier这个列与timestamp相等的数据

5、6是单数，所以是删除单个列版本，timestamp要么是最新的，要么精确等于timestamp
1到4是删除多个版本，要么是所有版本，要么是<=timestamp的。


另外，如果上面6个方法都没调用，
只用了下面两个构造函数
public Delete(byte[] row) //删除row这行中的所有列族的所有版本
public Delete(byte[] row, long timestamp, RowLock rowLock) //删除row这行中<=timestamp的列

这两个构造函数删除的数据范围更大，
然后到deleteFamily、再到deleteColumns、最后到deleteColumn

值得注意的代码
==============================================
			//当重复执行这段代码时，虽然时间戳为1，2的版本重复put，但是因为最新版本已到5了，所以是无效的，
			//只有第一次执行这段代码时打印的结果才是5个，第二开始以后都是三个。
			put = new Put(toB("20008"));
			put.add(cf, toB("q1"), 1, toB("v1"));
			put.add(cf, toB("q1"), 2, toB("v1"));
			put.add(cf, toB("q1"), 3, toB("v1"));
			put.add(cf, toB("q1"), 4, toB("v1"));
			put.add(cf, toB("q1"), 5, toB("v1"));
			t.put(put);
			get = new Get(toB("20008"));
			get.setMaxVersions(5);
			// 20008/cf:q1/5/Put/vlen=2/ts=0的格式是: rowKey/列族名:列名/时间戳/KV类型/列值长度/memstore时间戳
			//keyvalues={20008/cf:q1/5/Put/vlen=2/ts=0, 20008/cf:q1/4/Put/vlen=2/ts=0, 20008/cf:q1/3/Put/vlen=2/ts=0, 20008/cf:q1/2/Put/vlen=2/ts=0, 20008/cf:q1/1/Put/vlen=2/ts=0}
			p(t.get(get));
			
			d = new Delete(toB("20008"), 2, null);
			t.delete(d);
			//keyvalues={20008/cf:q1/5/Put/vlen=2/ts=0, 20008/cf:q1/4/Put/vlen=2/ts=0, 20008/cf:q1/3/Put/vlen=2/ts=0}
			p(t.get(get));


如果在Delete构造函数指定了时间戳，但是deleteFamily没有指定，这个时间戳是无效的，deleteFamily保持自己的语义
其他deleteColumns、deleteColumn都类似，Delete构造函数指定的时间戳只有在不调用其他deleteXXX方法时才有效。
==============================================
			put = new Put(toB("20017"));
			put.add(cf, toB("q1"), 1, toB("v1"));
			put.add(cf2, toB("q2"), 1, toB("v2"));
			put.add(cf, toB("q1"), 2, toB("v1"));
			put.add(cf2, toB("q2"), 2, toB("v2"));
			put.add(cf, toB("q1"), 3, toB("v1"));
			put.add(cf2, toB("q2"), 3, toB("v2"));
			t.put(put);
			get = new Get(toB("20017"));
			get.setMaxVersions(5);
			p(t.get(get));
			
			//如果在Delete构造函数指定了时间戳，但是deleteFamily没有指定，这个时间戳是无效的，deleteFamily保持自己的语义
			//其他deleteColumns、deleteColumn都类似，Delete构造函数指定的时间戳只有在不调用其他deleteXXX方法时才有效。
			d = new Delete(toB("20017"), 2, null);
			//删除cf2的所有版本
			d.deleteFamily(cf2);
			t.delete(d);
			p(t.get(get)); //keyvalues={20017/cf:q1/3/Put/vlen=2/ts=0, 20017/cf:q1/2/Put/vlen=2/ts=0, 20017/cf:q1/1/Put/vlen=2/ts=0}


















