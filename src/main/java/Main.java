import com.dbs.utils.SqlLiteHelper;
import com.dbs.utils.hbase.*;
import com.dbs.utils.hdfs.StoreCSVFileInHDFS;
import com.dbs.utils.mapreduce.MapReduceProfanityDriver;
import com.dbs.utils.mapreduce.MapReduceSongDataDriver;

public class Main {

    public static void main(String[] args) throws Exception {

        StoreCSVFileInHDFS.run();
        DeleteSongDataTable.run();
        CreateSongDataTable.run();
        DeleteSongDataSummaryTable.run();
        CreateSongDataSummaryTable.run();
        DeleteProfanitySummaryTable.run();
        CreateProfanitySummaryTable.run();
        AddDataToSongDataTable.run();
        MapReduceSongDataDriver.run();
        MapReduceProfanityDriver.run();
        SqlLiteHelper.dropSummaryTable();
        SqlLiteHelper.dropProfantiySummaryTable();
        SqlLiteHelper.createSummaryTable();
        SqlLiteHelper.createProfanityTable();
        CopySummaryDataToSqlLiteDb.run();
        CopyProfanityDataToSqlLiteDb.run();
    }
}
