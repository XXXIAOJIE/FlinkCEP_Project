package example;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Unit test for simple App.
 */
public class AppTest
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

//
//    /**
//     * @return the suite of tests being tested
//     */
//    public static Test suite()
//    {
//        return new TestSuite( AppTest.class );
//    }
//
//    /**
//     * Rigourous Test :-)
//     */
    public void testApp() throws Exception {
        DataStream<String> result = App.Taxi();
        String outputPath = "reult/flink_taxi_result.csv";
        result.writeAsCsv(outputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        //assertTrue( true );
    }
}

