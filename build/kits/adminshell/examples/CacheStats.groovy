
/**
 * This script will do the following:
 * - clear the cache statistics
 * - issue the QUERY
 * - call the get the current statistics
 *
 * Changes to make before executing:
 *     - set VDB_NAME
 *     - (optional) set VDB_VERSION
 *     - change QUERY to the SQL to perform
 *
 */

public class CacheStats {
        
    public final VDB_NAME = "vdb_name"
    public final int VDB_VERSION = 1
    public final SERVER = "localhost"

    public final JDBC_USER = "user"
    public final JDBC_PWD = "user"

    public final QUERY = "select * from tablename"
    
    public final int LOOP_COUNTER = 10
    
    public static int totalEntriesStart
    public static int totalEntriesEnd
    public static int totalEntriesReset
    
    public static int requestCountStart
    public static int requestCountEnd
    public static int requestCountReset
    
    public static double hitRatioStart
    public static double hitRatioEnd
    public static double hitRatioReset    
    

    public void execute() {
        if ( VDB_NAME.equals( "vdb_name") ) {
          println( "**** Must change VDB_NAME property to the VDB you want to access")
          return
        }
        
        if ( QUERY.equals( "select * from tablename") ) {
          println( "**** Must change QUERY property to the SQL query to execute")
          return
        }        

        connectAsAdmin( )

        performTest()

        disconnect()
        
    } 
    
    void performTest () {    
        
        /* First - clear the QUERY_SERVICE_RESULT_SET_CACHE cache */         
        try {
            clearCache("QUERY_SERVICE_RESULT_SET_CACHE")
            println "\nCleared Cache"
        }
        catch ( Exception e ) {
            println( "clearCache.eachRow exception: " + e.getMessage() )
        }    
        
        /* Retrieve the cached values */ 
        CacheStatistics startCache
        try { 
            startCache = getCacheStats("QUERY_SERVICE_RESULT_SET_CACHE")
        }
        catch ( Exception e ) {
            println( "Exception: " + e.printStackTrace() )
        }
        
        println "\nget Start Stats"
        requestCountStart = startCache.getRequestCount()
        hitRatioStart = startCache.getHitRatio() 
        totalEntriesStart = startCache.getTotalEntries() 
 
            for ( i in 0..LOOP_COUNTER) {
            doQuery()                
        }
        
        CacheStatistics endCache
        try {
            endCache = getCacheStats("QUERY_SERVICE_RESULT_SET_CACHE")
        } 
        catch ( Exception e ) {
            println( "getCacheStats.eachRow exception: " + e.getMessage() )
        }
        
         println "\nget End Stats"
        requestCountEnd = endCache.getRequestCount()
        hitRatioEnd = endCache.getHitRatio() 
        totalEntriesEnd = endCache.getTotalEntries() 
        
        /* Test for: getCacheStats() and clearCache() - clear the cache */ 
        try {
            clearCache("QUERY_SERVICE_RESULT_SET_CACHE")
        }
        catch ( Exception e ) {
            println( "clearCache.eachRow exception: " + e.getMessage() )
        }
        
        CacheStatistics resetCache
        try {
            resetCache = getCacheStats("QUERY_SERVICE_RESULT_SET_CACHE")
            println "RESET: " + getCacheStats("QUERY_SERVICE_RESULT_SET_CACHE")
        } 
        catch ( Exception e ) {
            println( "returnedData.eachRow exception: " + e.getMessage() )
        }    
        
        requestCountReset = resetCache.getRequestCount()
        hitRatioReset = resetCache.getHitRatio() 
        totalEntriesReset = resetCache.getTotalEntries() 
        
        try { 
            println getCacheStats("QUERY_SERVICE_RESULT_SET_CACHE")
        }
        catch ( Exception e ) {
            println( "Exception: " + e.getMessage() )
        }    

        println "\n*** RESULT TOTALS ***"

        println "\n*** totalEntries"
        println( "Start: " + totalEntriesStart )
        println( "End: " + totalEntriesEnd )
        println( "Reset: " + totalEntriesReset )
    
        println "\n*** requestCount"
        println( "Start: " + requestCountStart )
        println( "End: " + requestCountEnd )
        println( "Reset: " + requestCountReset )

        println "\n*** hitRatio"
        println( "Start: " + hitRatioStart )
        println( "End: " + hitRatioEnd )
        println( "Reset: " + hitRatioReset )       
   
        
    } /* method */
    
 

    
} /* class */

test=new CacheStats()
test.execute();
