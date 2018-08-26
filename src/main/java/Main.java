import com.dbs.utils.ProfanityChecker;
import com.dbs.utils.SqlLiteHelper;
import com.dbs.utils.hbase.*;
import com.dbs.utils.hdfs.StoreCSVFileInHDFS;
import com.dbs.utils.mapreduce.MapReduceProfanityDriver;
import com.dbs.utils.mapreduce.MapReduceSongDataJob;

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
        SqlLiteHelper.dropTables();
        SqlLiteHelper.createTables();
        MapReduceSongDataJob.run();
        MapReduceProfanityDriver.run();
        CopySummaryDataToSqlLiteDb.run();
        CopyProfanityDataToSqlLiteDb.run();
    }
}
