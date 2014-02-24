package my.test.filter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ClientScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import my.test.TestBase;

public class FilterTest extends TestBase {
    public FilterTest() throws Exception {
        super();
    }

    public static final String TABLE_NAME = "ScannerTest";
    public static final String FAMILY1 = "cf1";
    public static final String FAMILY2 = "cf2";

    /**
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args) throws Exception {
        new FilterTest().run();

    }

    public void run() throws Exception {
        ResultScanner rs;// = t.getScanner(toB(FAMILY1));

        Scan scan = new Scan();
        //scan.setMaxResultSize(100);

        Configuration conf = HBaseConfiguration.create();
        //conf.set(ScannerCallable.LOG_SCANNER_ACTIVITY, "true");
        //conf.set(ScannerCallable.LOG_SCANNER_LATENCY_CUTOFF, "300");
        //conf.setLong("hbase.regionserver.lease.period", 10000);
        //scan.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_ENABLE, toB("1"));

        scan.setCaching(100);
        scan.setCaching(1050);

        scan.setStartRow(toB("1000"));
        scan.setStopRow(toB("1005"));

        FilterList filterList = null;
        Filter f = null;
        f = new RowFilter(CompareOp.EQUAL, new BinaryComparator(toB("1004")));
        f = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(toB(FAMILY1)));
        f = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(toB("q")));
        f = new ValueFilter(CompareOp.EQUAL, new BinaryComparator(toB("1000")));

        filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        filterList.addFilter(f);
        filterList.addFilter(new ValueFilter(CompareOp.EQUAL, new BinaryComparator(toB("1002"))));

        f = filterList;

        f = new DependentColumnFilter(toB(FAMILY1), toB("q"), false, CompareOp.EQUAL, new BinaryComparator(toB("1002")));

        f = new ColumnPaginationFilter(3, 2);

        scan.setFilter(f);
        rs = new ClientScanner(conf, scan, toB(TABLE_NAME));

        int count = 0;
        for (Result r : rs) {
            p(r);
            count++;
        }
        p("count=" + count);
        rs.close();
    }
}
