import com.dbs.utils.SqlLiteHelper;
import com.dbs.utils.hbase.*;
import com.dbs.utils.hdfs.StoreCSVFileInHDFS;
import com.dbs.utils.mapreduce.MapReduceSongDataJob;

public class Main {

    public static void main(String[] args) throws Exception {

        StoreCSVFileInHDFS.run();
        DeleteSongDataTable.run();
        CreateSongDataTable.run();
        DeleteSongDataSummaryTable.run();
        CreateSongDataSummaryTable.run();
        AddDataToSongDataTable.run();
        SqlLiteHelper.dropTable();
        SqlLiteHelper.createNewDatabase();
        MapReduceSongDataJob.run();
        CopyDataToSqlLiteDb.run();
    }
}
