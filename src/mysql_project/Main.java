package mysql_project;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Ricemilk
 */
public class Main {

    static Connection conn = null;
    static String host = "localhost";
    static String port = "3306";
    static String db = "rice_brightkite";	//Your database name here: "rice_schema" or "rice_brightkite"
    static String user = "root";
    static String pass = "";

    public class Option {
    	
    }
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(getConnectionString(host, port, db, user, pass));

            // Do something with the Connection
            runNonQuery("SET foreign_key_checks = 0");  // Ignore foreign key check

            
            // Calculate co-occurrence for gowalla top500
            String tableName = "gowalla_top500",	// output tableNmae of "runCoOccurrence"
                	checkinTableName = "gowalla_checkin_top500",	// input tableName of "runCoOccurrence": check-in table
                	friendshipTableName = "gowalla_friends_top500";	// input tableName of "runAggregatedCoOccurrences": friendship table
            
	        runCoOccurrence(tableName, checkinTableName);
	        runDiversity(tableName);
	        runTemporal(tableName);
	        runLocationPopularity(tableName);
            runAggregatedCoOccurrences(tableName, friendshipTableName);

            /* example to show process list */
            // ShowProcessList();

            /* example to kill process */
            // Kill(13776);

        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            try {
                if (conn != null) {
                    if (!conn.isClosed()) {
//                        conn.rollback();
                    }
                    conn.close();
                }
            } catch (SQLException e) {
                Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, e);
            }
        } catch (ClassNotFoundException ex) {
            try {
                if (conn != null) {
                    if (!conn.isClosed()) {
//                        conn.rollback();
                    }
                    conn.close();
                }
            } catch (SQLException e) {
                Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, e);
            }
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException ex) {
            System.err.println("Unable to close connection");
        }
    }
    private static void writeLog(String str, String fileName) {
    	PrintWriter writer;
        String home  = System.getProperty("user.home") + "/" ;
		try {
			writer = new PrintWriter(new FileOutputStream(new File( home + "/Desktop/gowalla/result/evaluation/" + fileName), true)); 
            writer.println(str);
            writer.close(); 
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    
    }
    
    private static void runDiversity(String tableName) {
    	String query = "", txt = "";
    	ResultSet rs;

    	try {
    		// Drop all table
    		query = String.format("DROP TABLE IF EXISTS %s_temp1;", tableName);
  			runNonQuery(query);
  			query = String.format("DROP TABLE IF EXISTS %s_temp2;", tableName);
  			runNonQuery(query);
  			query = String.format("DROP TABLE IF EXISTS %s_temp3;", tableName);
  			runNonQuery(query);
  			query = String.format("DROP TABLE IF EXISTS %s_temp4;", tableName);
  			runNonQuery(query);
  			query = String.format("DROP TABLE IF EXISTS %s_temp5;", tableName);
  			runNonQuery(query);
  			query = String.format("DROP TABLE IF EXISTS %s_diversity;", tableName);
  			runNonQuery(query);
  			
  			
    		// Create table temp1: Calculate co_occurrence frequency at a certain venue of user_1 and user_2
    		query = String.format("create table %s_temp1 as( select user_1, user_2, venue_id, sum(frequency) as co_occur_freq from %s group by user_1, user_2, venue_id );", tableName, tableName);
			runNonQuery(query);


    		// Add index temp1
			query = String.format("ALTER TABLE %s_temp1 ADD INDEX (co_occur_freq) ;", tableName);
  			runNonQuery(query);


    		// Create table temp2: Calculate total co_occurrence frequency of user_1 and user_2
			query = String.format("create table %s_temp2 as( select user_1, user_2, venue_id, co_occur_freq, sum(co_occur_freq) as total_freq from %s_temp1 group by user_1, user_2 );", tableName, tableName);
			runNonQuery(query);


  			// Add index temp2
			query = String.format("ALTER TABLE %s_temp2 ADD INDEX (total_freq) ;", tableName);
  			runNonQuery(query);


			// Create table temp 3: Calculate "px" in Formula 1. (where px = distribution)
			txt = "CREATE TABLE IF NOT EXISTS `%s_temp3` ( "+
          		  "`user_1` int(11) NOT NULL, "+
          		  "`user_2` int(11) NOT NULL, "+
          		  "`venue_id` varchar(50) NOT NULL, "+	//"`venue_id` int(11) NOT NULL, "+
          		  "`co_occur_freq` int(11) NOT NULL, "+
          		  "`total_freq` int(11) NOT NULL, "+
          		  "`distribution` double NOT NULL"+
          		") ENGINE=InnoDB DEFAULT CHARSET=latin1; ";
			query = String.format(txt, tableName);
			runNonQuery(query);
			
			txt = "insert into %s_temp3( " + 
					"select temp1.user_1, temp1.user_2, temp1.venue_id, temp1.co_occur_freq, temp2.total_freq, (temp1.co_occur_freq / temp2.total_freq) as distribution " + 
				    "from %s_temp1 as temp1 join %s_temp2 as temp2 on temp1.user_1 = temp2.user_1 and temp1.user_2 = temp2.user_2 "+
				");";
			query = String.format(txt, tableName, tableName, tableName);
			runNonQuery(query);

			// Add index to temp3 table
			query = String.format("ALTER TABLE %s_temp3 ADD INDEX (distribution) ;", tableName);
  			runNonQuery(query);
  			
  			// Create table temp4: Calculate "ln_distribution" used in Shannon entropy
			txt = "CREATE TABLE IF NOT EXISTS `%s_temp4` ( "+
          		  "`user_1` int(11) NOT NULL, "+
          		  "`user_2` int(11) NOT NULL, "+
          		"`venue_id` varchar(50) NOT NULL, "+	//"`venue_id` int(11) NOT NULL, "+
          		  "`co_occur_freq` int(11) NOT NULL, "+
          		  "`total_freq` int(11) NOT NULL, "+
          		  "`distribution` double NOT NULL, "+
          		  "`ln_distribution` double NOT NULL"+
          		") ENGINE=InnoDB DEFAULT CHARSET=latin1; ";
			query = String.format(txt, tableName);
			runNonQuery(query);
			
			// Insert data to table temp4
			query = String.format("insert into %s_temp4( select *, ln(distribution) from %s_temp3 );", tableName, tableName);
			runNonQuery(query);
			
			// Add index to table temp4
			query = String.format("ALTER TABLE %s_temp4 ADD INDEX (distribution) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_temp4 ADD INDEX (ln_distribution) ;", tableName);
  			runNonQuery(query);
			

			// Create table temp 5: Calculate "inverse Simpson index" (Formula 1.)  & "Shannon entropy"
  			txt = "CREATE TABLE IF NOT EXISTS `%s_temp5` ( "+
            		  "`user_1` int(11) NOT NULL, "+
            		  "`user_2` int(11) NOT NULL, "+
            		  "`venue_id` varchar(50) NOT NULL, "+	//"`venue_id` int(11) NOT NULL, "+
            		  "`co_occur_freq` int(11) NOT NULL, "+
            		  "`total_freq` int(11) NOT NULL, "+
            		  "`distribution` double NOT NULL, "+
            		  "`ln_distribution` double NOT NULL, "+
            		  "`shannon_entropy` double NOT NULL, "+
            		  "`inverse_simpson_index` double NOT NULL"+
            		") ENGINE=InnoDB DEFAULT CHARSET=latin1; ";
  			query = String.format(txt, tableName);
  			runNonQuery(query);
  			
			txt = "insert into %s_temp5( "+
				"select user_1, user_2, venue_id, co_occur_freq, total_freq, distribution, ln_distribution, -(sum(distribution * ln_distribution)), (1/(sum(power(distribution,2)))) "+
			    "from %s_temp4 "+
			    "group by user_1, user_2);";
			query = String.format(txt, tableName, tableName);
			runNonQuery(query);
			
			// Add index to table temp5
			query = String.format("ALTER TABLE %s_temp5 ADD INDEX (total_freq) ;", tableName);
			runNonQuery(query);
			query = String.format("ALTER TABLE %s_temp5 ADD INDEX (shannon_entropy) ;", tableName);
			runNonQuery(query);
			query = String.format("ALTER TABLE %s_temp5 ADD INDEX (inverse_simpson_index) ;", tableName);
			runNonQuery(query);

			// Update shannon_entropy
			query = String.format("update %s_temp5 set shannon_entropy = 0.001 where shannon_entropy = 0;", tableName);
			runNonQuery(query);
			
			// Calculate "max.Num_of_co_occur_venue" in Formula 2.
			txt = "select max(venue_id_counter) from ( "+
				"select user_1, user_2, count(distinct(venue_id)) venue_id_counter "+ 
				"from %s_temp1 "+
				"group by user_1, user_2 "+
				"order by count(distinct(venue_id)) desc "+
			") anyname;";
            
			query = String.format(txt, tableName);
            rs = runQuery(query);
            int diversity = 0;
            while(rs.next()) {
            	diversity = Integer.valueOf(rs.getObject(1).toString());
            }
            
            //	Calculate "max.frequency" in Formula 2.
            query = String.format("select max(total_freq) from %s_temp5;", tableName);
            rs = runQuery(query);
            int max_total_freq = 0;
            while(rs.next()) {
            	max_total_freq = Integer.valueOf(rs.getObject(1).toString());
            }
            
            // Create table %s_diversity
            txt = "CREATE TABLE IF NOT EXISTS `%s_diversity` ( "+
          		  "`user_1` int(11) NOT NULL, "+
          		  "`user_2` int(11) NOT NULL, "+
          		  "`total_freq` int(11) NOT NULL, "+
          		  "`shannon_entropy` double NOT NULL, "+
          		  "`inverse_simpson_index` double NOT NULL, "+
				  "`freq_shannon` double NOT NULL, "+
				  "`freq_inverse_simpson` double NOT NULL"+
          		") ENGINE=InnoDB DEFAULT CHARSET=latin1; ";
			query = String.format(txt, tableName);
			runNonQuery(query);
			
			// Calculate the "normalized_frequency * normalized_diversity" in Formula 2.
            txt = "insert into %s_diversity( "+
        		"select user_1, user_2, total_freq, shannon_entropy, inverse_simpson_index, (( total_freq / %d ) * ( shannon_entropy / %d )), "+
        			"(( total_freq / %d ) * ( inverse_simpson_index / %d)) "+
        	    "from %s_temp5); ";
			query = String.format(txt, tableName, max_total_freq, diversity, max_total_freq, diversity, tableName);
			runNonQuery(query);

			// Add index to table %s_diversity
			query = String.format("ALTER TABLE %s_diversity ADD INDEX (user_1) ;", tableName);
			runNonQuery(query);
			query = String.format("ALTER TABLE %s_diversity ADD INDEX (user_2) ;", tableName);
			runNonQuery(query);
			query = String.format("ALTER TABLE %s_diversity ADD INDEX (total_freq) ;", tableName);
			runNonQuery(query);
			query = String.format("ALTER TABLE %s_diversity ADD INDEX (freq_shannon) ;", tableName);
			runNonQuery(query);
			query = String.format("ALTER TABLE %s_diversity ADD INDEX (freq_inverse_simpson) ;", tableName);
			runNonQuery(query);
			
		} catch (SQLException e) {
			log("Error query is " + query);
			log("Error while running runDiversity " + e.getMessage());
		}
    }
    
    private static void runTemporal(String tableName) {
    	String query = "", txt = "";
    	ResultSet rs;

    	try {
    		// Drop all table
    		query = String.format("DROP TABLE IF EXISTS %s_temporal_temp1;", tableName);
  			runNonQuery(query);
  			query = String.format("DROP TABLE IF EXISTS %s_temporal;", tableName);
  			runNonQuery(query);
  			
    		// Create table temporal_temp1
  			// Calculate friendship duration (duration_hour) in Formula 6. and Formula 7.
  			// Calculate frequency (density) in Formula 6.
  			// Calculate standard_deviation in Formula 6.
  			
  			// Note: Here, "density" means "frequency" which is different from the density in paper.
  			txt = "CREATE TABLE IF NOT EXISTS `%s_temporal_temp1` ( "+
            		  "`user_1` int(11) NOT NULL, "+
            		  "`user_2` int(11) NOT NULL, "+
            		  "`venue_id` varchar(50) NOT NULL, "+	//"`venue_id` int(11) NOT NULL, "+
            		  "`timediff` int(11) NOT NULL, "+
            		  "`frequency` int(11) NOT NULL, "+
            		  "`avg_checkin_time` double NOT NULL, "+
            		  "`duration_hour` double NOT NULL, "+
            		  "`density` int(11) NOT NULL, "+
            		  "`std_deviation_hour` double NOT NULL"+
            		") ENGINE=InnoDB DEFAULT CHARSET=latin1; ";
  			query = String.format(txt, tableName);
  			runNonQuery(query);

  			txt = "insert into %s_temporal_temp1( "+
  					"select user_1, user_2, venue_id, timediff, frequency, avg_checkin_time, "+
  							"((max(avg_checkin_time) - min(avg_checkin_time)))/3600 as duration_hour, "+
  				            "count(*) as density, "+
  				            "(stddev(avg_checkin_time))/3600  as std_deviation_hour "+
  				    "from %s "+
  					"group by user_1, user_2 "+
  					"order by duration_hour desc, density desc, std_deviation_hour desc); ";
  			
    		query = String.format(txt, tableName, tableName);
			runNonQuery(query);

    		// Add index to %s_temporal_temp1
			query = String.format("ALTER TABLE %s_temporal_temp1 ADD INDEX (duration_hour) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_temporal_temp1 ADD INDEX (density) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_temporal_temp1 ADD INDEX (std_deviation_hour) ;", tableName);
  			runNonQuery(query);

  			
  			
  			// Calculate max(friendship duration) among all users in Formula 8.
			txt = "select max(duration_hour) from %s_temporal_temp1;";
			query = String.format(txt, tableName);

            rs = runQuery(query);
            double maxDurationHour = 0;
            while(rs.next()) {
            	maxDurationHour = Double.valueOf(rs.getObject(1).toString());
            }
  			
  			
    		// Create table temporal table
  			txt = "CREATE TABLE IF NOT EXISTS `%s_temporal` ( "+
            		  "`user_1` int(11) NOT NULL, "+
            		  "`user_2` int(11) NOT NULL, "+
            		  "`avg_checkin_time` double NOT NULL, "+
            		  "`duration_hour` double NOT NULL, "+
            		  "`density` int(11) NOT NULL, "+
            		  "`std_deviation_hour` double NOT NULL, "+
            		  "`stability` double NOT NULL, "+
            		  "`norm_duration` double NOT NULL"+
            		") ENGINE=InnoDB DEFAULT CHARSET=latin1; ";
  			query = String.format(txt, tableName);
  			runNonQuery(query);

  			
            // Insert into temporal: Calculate "stability" and "duration" in Formula 6. and Formula 8. respectively. 
  			txt = "insert into %s_temporal( "+
	    		"select user_1, user_2, avg_checkin_time, duration_hour, density, std_deviation_hour, "+ 
	    			"exp(-((duration_hour/density) + std_deviation_hour)), "+
	    	        "duration_hour / %.4f "+
	    	    "from %s_temporal_temp1); ";
  			query = String.format(txt, tableName, maxDurationHour, tableName);
  			runNonQuery(query);
  			
  			// Update: if a user pair only has one co-occurrence (density=1), we set stability = 0 
  			query = String.format("update %s_temporal set stability = 0  where density = 1;", tableName);
  			runNonQuery(query);

    		// Add index to %s_temporal
			query = String.format("ALTER TABLE %s_temporal ADD INDEX (user_1) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_temporal ADD INDEX (user_2) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_temporal ADD INDEX (stability) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_temporal ADD INDEX (norm_duration) ;", tableName);
  			runNonQuery(query);
			
  			
		} catch (SQLException e) {
			log("Error query is " + query);
			log("Error while running runTemporal " + e.getMessage());
		}
    }
    
    private static void runLocationPopularity(String tableName) {
    	String query = "", txt = "";
    	ResultSet rs;

    	try {
    		// Drop all table
    		query = String.format("DROP TABLE IF EXISTS %s_location_popularity;", tableName);
  			runNonQuery(query);
  			query = String.format("DROP TABLE IF EXISTS %s_aggrgate_location_popularity_temp1;", tableName);
  			runNonQuery(query);
  			query = String.format("DROP TABLE IF EXISTS %s_aggrgate_location_popularity_temp2;", tableName);
  			runNonQuery(query);
  			query = String.format("DROP TABLE IF EXISTS %s_aggrgate_location_popularity_temp3;", tableName);
  			runNonQuery(query);
  			query = String.format("DROP TABLE IF EXISTS %s_aggrgate_location_popularity_temp4;", tableName);
  			runNonQuery(query);
  			query = String.format("DROP TABLE IF EXISTS %s_aggrgate_location_popularity;", tableName);
  			runNonQuery(query);
  			
    		// Create table temporal_temp1
  			txt = "CREATE TABLE IF NOT EXISTS `%s_location_popularity` ( "+
  					"`venue_id` varchar(50) NOT NULL, "+	//"`venue_id` int(11) NOT NULL, "+
            		  "`visit_count` int(11) NOT NULL, "+
            		  "`inverse_popularity` double NOT NULL"+
            		") ENGINE=InnoDB DEFAULT CHARSET=latin1; ";
  			query = String.format(txt, tableName);
  			runNonQuery(query);

  			txt = "insert into %s_location_popularity ( "+
  					"SELECT venue_id, count(*) as venue_count, 1/count(*) as inverse_location_popularity "+
  					"FROM %s "+
  					"group by venue_id )";
    		query = String.format(txt, tableName, tableName);
			runNonQuery(query);

			
    		// Add index to %s_location_popularity
			query = String.format("ALTER TABLE %s_location_popularity ADD INDEX (venue_id) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_location_popularity ADD INDEX (inverse_popularity) ;", tableName);
  			runNonQuery(query);

  			
  			txt = "CREATE TABLE IF NOT EXISTS `%s_aggrgate_location_popularity_temp1` ( "+
  				  "`user_1` int(11) NOT NULL, "+
          		  "`user_2` int(11) NOT NULL, "+
          		"`venue_id` varchar(50) NOT NULL, "+	//"`venue_id` int(11) NOT NULL, "+
          		  "`co_occur_freq` int(11) NOT NULL, "+
          		  "`inverse_location_popularity` double NOT NULL, "+
          		  "`weighted_popularity` double NOT NULL"+
          		") ENGINE=InnoDB DEFAULT CHARSET=latin1; ";
			query = String.format(txt, tableName);
			runNonQuery(query);
  			
			// Calculate "visiting ratio" (weighted_popularity) in Formula 4.
  			txt = "insert into %s_aggrgate_location_popularity_temp1( "+
  					"select  co_occur.user_1, co_occur.user_2, co_occur.venue_id, co_occur_freq, "+
  							"loc_pop.inverse_popularity, "+
  				            "loc_pop.inverse_popularity * co_occur_freq "+ 
  				    "from %s_temp1 as co_occur "+
  						"join %s_location_popularity loc_pop "+
  							"on co_occur.venue_id = loc_pop.venue_id ); ";
  			query = String.format(txt, tableName, tableName, tableName);
			runNonQuery(query);

    		// Add index to %s_aggrgate_location_popularity_temp1
			query = String.format("ALTER TABLE %s_aggrgate_location_popularity_temp1 ADD INDEX (user_1) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_aggrgate_location_popularity_temp1 ADD INDEX (user_2) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_aggrgate_location_popularity_temp1 ADD INDEX (venue_id) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_aggrgate_location_popularity_temp1 ADD INDEX (weighted_popularity) ;", tableName);
  			runNonQuery(query);
 			
  			// Calculate location popularity in Formula 5. (by the formula of Shannon entropy)  
  			txt = "CREATE TABLE IF NOT EXISTS `%s_aggrgate_location_popularity_temp2` ( "+
  				  "`user_1` int(11) NOT NULL, "+
          		  "`user_2` int(11) NOT NULL, "+
          		"`venue_id` varchar(50) NOT NULL, "+	//"`venue_id` int(11) NOT NULL, "+
          		  "`co_occur_freq` int(11) NOT NULL, "+
        		  "`weighted_popularity` double NOT NULL, "+
          		  "`location_entropy` double NOT NULL"+
          		") ENGINE=InnoDB DEFAULT CHARSET=latin1; ";
			query = String.format(txt, tableName);
			runNonQuery(query);
			
			txt = "insert into %s_aggrgate_location_popularity_temp2 ( "+
					"select user_1, user_2, venue_id, co_occur_freq, weighted_popularity, -(sum(weighted_popularity * log(weighted_popularity))) "+
					"from %s_aggrgate_location_popularity_temp1 "+
					"group by venue_id);";
			
			query = String.format(txt, tableName, tableName);
			runNonQuery(query);

			query = String.format("ALTER TABLE %s_aggrgate_location_popularity_temp2 ADD INDEX (user_1) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_aggrgate_location_popularity_temp2 ADD INDEX (user_2) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_aggrgate_location_popularity_temp2 ADD INDEX (venue_id) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_aggrgate_location_popularity_temp2 ADD INDEX (co_occur_freq) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_aggrgate_location_popularity_temp2 ADD INDEX (location_entropy) ;", tableName);
  			runNonQuery(query);
  			
  			// Aggregate location entropy with co-occurrence table to calculate "weighted_location_entropy"
  			// "weighted_location_entropy" = co_occur_frquency at the venue * location_entropy of the venue
  			txt = "CREATE TABLE IF NOT EXISTS `%s_aggrgate_location_popularity_temp3` ( "+
  				  "`user_1` int(11) NOT NULL, "+
          		  "`user_2` int(11) NOT NULL, "+
          		"`venue_id` varchar(50) NOT NULL, "+	//"`venue_id` int(11) NOT NULL, "+
          		  "`co_occur_freq` int(11) NOT NULL, "+
          		  "`location_entropy` double NOT NULL, "+
          		  "`weighted_location_entropy` double NOT NULL"+
          		") ENGINE=InnoDB DEFAULT CHARSET=latin1; ";
			query = String.format(txt, tableName);
			runNonQuery(query);
			
			txt = "insert into %s_aggrgate_location_popularity_temp3 ( "+
					"select user.user_1, user.user_2, user.venue_id, user.co_occur_freq, location.location_entropy, "+ //sum(weighted_popularity) "+
					"user.co_occur_freq * location.location_entropy "+
					"from %s_aggrgate_location_popularity_temp2 location "+	//"from %s_aggrgate_location_popularity_temp1 "+
					"join %s_aggrgate_location_popularity_temp1 user "+
					"on user.venue_id = location.venue_id); ";
				
			query = String.format(txt, tableName, tableName, tableName);
			runNonQuery(query);

			query = String.format("ALTER TABLE %s_aggrgate_location_popularity_temp3 ADD INDEX (user_1) ;", tableName);
			runNonQuery(query);
			query = String.format("ALTER TABLE %s_aggrgate_location_popularity_temp3 ADD INDEX (user_2) ;", tableName);
			runNonQuery(query);
			query = String.format("ALTER TABLE %s_aggrgate_location_popularity_temp3 ADD INDEX (venue_id) ;", tableName);
			runNonQuery(query);
			query = String.format("ALTER TABLE %s_aggrgate_location_popularity_temp3 ADD INDEX (weighted_location_entropy) ;", tableName);
			runNonQuery(query);

			// Sum the "weighted_location_entropy" of all venues of a user pair
  			txt = "CREATE TABLE IF NOT EXISTS `%s_aggrgate_location_popularity_temp4` ( "+
    				  "`user_1` int(11) NOT NULL, "+
            		  "`user_2` int(11) NOT NULL, "+
            		  "`sum_weighted_location_entropy` double NOT NULL"+
            		") ENGINE=InnoDB DEFAULT CHARSET=latin1; ";
  			query = String.format(txt, tableName);
  			runNonQuery(query);
  			
  			
  			txt = "insert into %s_aggrgate_location_popularity_temp4 ( "+
  					"select user_1, user_2, sum(weighted_location_entropy) "+ //sum(weighted_popularity) "+
  					"from %s_aggrgate_location_popularity_temp3 "+	//"from %s_aggrgate_location_popularity_temp1 "+
  					"group by user_1, user_2);";
  			
  			query = String.format(txt, tableName, tableName);
  			runNonQuery(query);

  			query = String.format("ALTER TABLE %s_aggrgate_location_popularity_temp4 ADD INDEX (user_1) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_aggrgate_location_popularity_temp4 ADD INDEX (user_2) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_aggrgate_location_popularity_temp4 ADD INDEX (sum_weighted_location_entropy) ;", tableName);
  			runNonQuery(query);
  			
  			// Normalize "sum_weighted_location_entropy" with their co-occurrence frequency
  			txt = "CREATE TABLE IF NOT EXISTS `%s_aggrgate_location_popularity` ( "+
  				  "`user_1` int(11) NOT NULL, "+
          		  "`user_2` int(11) NOT NULL, "+
          		  "`sum_weighted_location_entropy` double NOT NULL"+
          		") ENGINE=InnoDB DEFAULT CHARSET=latin1; ";
			query = String.format(txt, tableName);
			runNonQuery(query);
			
  			txt = "insert into %s_aggrgate_location_popularity ( "+
  					"select co_occur.user_1, co_occur.user_2, exp(- (co_occur.sum_weighted_location_entropy/user_total_freq.total_freq)) "+
  					"from %s_aggrgate_location_popularity_temp4 as co_occur "+
  						"join (select user_1, user_2, total_freq from %s_temp2) as user_total_freq "+
  							"on co_occur.user_1 = user_total_freq.user_1 and co_occur.user_2 = user_total_freq.user_2) ";
  			
  			query = String.format(txt, tableName, tableName, tableName);
			runNonQuery(query);
  			
			query = String.format("ALTER TABLE %s_aggrgate_location_popularity ADD INDEX (user_1) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_aggrgate_location_popularity ADD INDEX (user_2) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_aggrgate_location_popularity ADD INDEX (sum_weighted_location_entropy) ;", tableName);
  			runNonQuery(query);
  			
		} catch (SQLException e) {
			log("Error query is " + query);
			log("Error while running runLocationPopularity " + e.getMessage());
		}
    }
    
    
    // Aggregate all features in diversity, popularity and temporal
    private static void runAggregatedCoOccurrences(String tableName, String friendshipTable) {
    	String query = "", txt = "";
    	ResultSet rs;

    	try {
    		// Drop all table
    		query = String.format("DROP TABLE IF EXISTS %s_co_occurrences_aggregated_temp1;", tableName);
  			runNonQuery(query);
  			query = String.format("DROP TABLE IF EXISTS %s_co_occurrences_aggregated;", tableName);
  			runNonQuery(query);
    		query = String.format("DROP TABLE IF EXISTS %s_co_occurrences_with_friendship;", tableName);
  			runNonQuery(query);
  			query = String.format("DROP TABLE IF EXISTS %s_co_occurrences_with_friendship_cleaned;", tableName);
  			runNonQuery(query);
  			
    		// Create table temporal_temp1
  			txt = "CREATE TABLE IF NOT EXISTS `%s_co_occurrences_aggregated_temp1` ( "+
            		  "`user_1` int(11) NOT NULL, "+
            		  "`user_2` int(11) NOT NULL, "+
            		  "`location_popularity` double  NOT NULL, "+
            		  "`total_freq` int(11) NOT NULL, "+
            		  "`freq_shannon` double  NOT NULL, "+
            		  "`freq_inverse_simpson` double NOT NULL"+
            		") ENGINE=InnoDB DEFAULT CHARSET=latin1; ";
  			query = String.format(txt, tableName);
  			runNonQuery(query);

  			txt = "insert into %s_co_occurrences_aggregated_temp1( "+
  					"select location.user_1, location.user_2, location.sum_weighted_location_entropy, "+
  					"diversity.total_freq, diversity.freq_shannon, diversity.freq_inverse_simpson "+
  			"from %s_aggrgate_location_popularity as location "+
  				"join %s_diversity as diversity "+
  					"on location.user_1 = diversity.user_1 and location.user_2 = diversity.user_2) ";

    		query = String.format(txt, tableName, tableName, tableName);
			runNonQuery(query);

			
			query = String.format("ALTER TABLE %s_co_occurrences_aggregated_temp1 ADD INDEX (user_1) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_co_occurrences_aggregated_temp1 ADD INDEX (user_2) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_co_occurrences_aggregated_temp1 ADD INDEX (location_popularity) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_co_occurrences_aggregated_temp1 ADD INDEX (total_freq) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_co_occurrences_aggregated_temp1 ADD INDEX (freq_shannon) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_co_occurrences_aggregated_temp1 ADD INDEX (freq_inverse_simpson) ;", tableName);
  			runNonQuery(query);
			
  			
  			txt = "CREATE TABLE IF NOT EXISTS `%s_co_occurrences_aggregated` ( "+
          		  "`user_1` int(11) NOT NULL, "+
          		  "`user_2` int(11) NOT NULL, "+
          		  "`weighted_location_popularity` double  NOT NULL, "+
          		  "`total_freq` int(11) NOT NULL, "+
          		  "`freq_shannon` double  NOT NULL, "+
          		  "`freq_inverse_simpson` double  NOT NULL, "+
          		  "`stability` double  NOT NULL, "+
          		  "`norm_duration` double  NOT NULL, "+
          		  "`temporal` double  NOT NULL, "+
          		  "`co_occurrence_inverse_simpson` double  NOT NULL, "+
          		  "`co_occurrence_shannon` double NOT NULL"+
          		") ENGINE=InnoDB DEFAULT CHARSET=latin1; ";
			query = String.format(txt, tableName);
			runNonQuery(query);
  			
  			
			txt = "insert into %s_co_occurrences_aggregated( "+
  					"select co_occur.user_1, co_occur.user_2, co_occur.location_popularity, "+
  							"co_occur.total_freq, co_occur.freq_shannon, co_occur.freq_inverse_simpson, "+
  					        "temporal.stability, temporal.norm_duration, "+
  					        "(temporal.stability + temporal.norm_duration) / 2, "+
  					        "(co_occur.freq_inverse_simpson + co_occur.location_popularity + (temporal.stability+temporal.norm_duration)/2)/3, "+
  							"(co_occur.freq_shannon + co_occur.location_popularity + (temporal.stability+temporal.norm_duration)/2)/3 "+
  					"from %s_co_occurrences_aggregated_temp1 co_occur "+
  					        "join %s_temporal as temporal "+
  								"on co_occur.user_1 = temporal.user_1 and co_occur.user_2 = temporal.user_2 ) ";

			query = String.format(txt, tableName, tableName, tableName);
			runNonQuery(query);
			
			// add index to s_co_occurrences_aggregated
			query = String.format("ALTER TABLE %s_co_occurrences_aggregated ADD INDEX (user_1) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_co_occurrences_aggregated ADD INDEX (user_2) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_co_occurrences_aggregated ADD INDEX (co_occurrence_inverse_simpson) ;", tableName);
  			runNonQuery(query);
  			query = String.format("ALTER TABLE %s_co_occurrences_aggregated ADD INDEX (co_occurrence_shannon) ;", tableName);
  			runNonQuery(query);
			

  			
    		// Create table temporal_temp1
  			txt = "CREATE TABLE IF NOT EXISTS `%s_co_occurrences_with_friendship` ( "+
  					"`user_1` int(11) NOT NULL, "+
	  		  		  "`user_2` int(11) NOT NULL, "+
	  		  		  "`weighted_location_popularity` double  NOT NULL, "+
	  		  		  "`total_freq` int(11) NOT NULL, "+
	  		  		  "`freq_shannon` double  NOT NULL, "+
	  		  		  "`freq_inverse_simpson` double  NOT NULL, "+
	  		  		  "`stability` double  NOT NULL, "+
	  		  		  "`norm_duration` double  NOT NULL, "+
	  		  		  "`temporal` double  NOT NULL, "+
	  		  		  "`co_occurrence_inverse_simpson` double  NOT NULL, "+
	  		  		  "`co_occurrence_shannon` double NOT NULL, "+
	  		  		  "`friendship` int(11) NOT NULL"+
	  		  		 ") ENGINE=InnoDB DEFAULT CHARSET=latin1; ";
  			query = String.format(txt, tableName);
  			runNonQuery(query);

  			
  			txt = "insert into %s_co_occurrences_with_friendship ( "+		
  					"select * , "+
  				    "(SELECT EXISTS( "+
  				    		"SELECT *  "+
  				    		"FROM %s "+ 										
  				    		"WHERE %s.user_1 = %s_co_occurrences_aggregated.user_1  "+  
  				    		"and %s.user_2 = %s_co_occurrences_aggregated.user_2)) as friendship "+ 
  				    "from %s_co_occurrences_aggregated) "; 
    		query = String.format(txt, tableName, friendshipTable, friendshipTable, tableName, friendshipTable, tableName, tableName);
			runNonQuery(query);
			
			// we remove those user pairs who only have "co-occurrence frequency = 1"
			txt = "CREATE TABLE IF NOT EXISTS `%s_co_occurrences_with_friendship_cleaned` like %s_co_occurrences_with_friendship";
			query = String.format(txt, tableName, tableName);
  			runNonQuery(query);
			
  			txt = "insert into %s_co_occurrences_with_friendship_cleaned ( "+		
  					"select * "+ 
  				    "from %s_co_occurrences_with_friendship "+
  					"where total_freq != 1 ) "; 
    		query = String.format(txt, tableName, tableName);
			runNonQuery(query);
  			
			
		} catch (SQLException e) {
			log("Error query is " + query);
			log("Error while running runLocationPopularity " + e.getMessage());
		}
    }

    private static void runCoOccurrence(String tableName, String checkInTable) {
    	String query = "", txt = "";
    	ResultSet rs;

    	try {
    		// Drop all tables
    		query = String.format("DROP TABLE IF EXISTS %s;", tableName);
  			runNonQuery(query);

    		// Create table %s: Calculate co-occurrence of all check-in data
  			// "timediff" = time difference between "timestamp_u1" and "timestamp_u2"
  			// "avg_checkin_time" = average of "timestamp_u1" and "timestamp_u2". We use "avg_checkin_time" as the timestamp of a co-occurrence event
  			txt = "CREATE TABLE IF NOT EXISTS `%s` ( "+
  					"`user_1` int(11) NOT NULL, "+
	  		  		  "`user_2` int(11) NOT NULL, "+
	  		  		  "`venue_id` varchar(50)  NOT NULL, "+	//"`venue_id` int(11)  NOT NULL, "+		//brightkite using "varchar(50)" for "venue_id"
	  		  		  "`timediff` int(11) NOT NULL, "+
	  		  		  "`frequency` int(11)  NOT NULL, "+
	  		  		  "`timestamp_u1` int(11)  NOT NULL, "+
	  		  		  "`timestamp_u2` int(11)  NOT NULL, "+
	  		  		  "`avg_checkin_time` double  NOT NULL"+
	  		  		 ") ENGINE=InnoDB DEFAULT CHARSET=latin1; ";
  			query = String.format(txt, tableName);
  			runNonQuery(query);

  			// Co-occurrence: same "venue_id", timestamp difference < 1 hour
  			txt = "insert into %s( "+
  					"select target.user_id, similar.user_id, target.venue_id,  "+
  					"ABS(target.timestamp - similar.timestamp),  "+
  					"1,  "+
  					"target.timestamp,  "+
  					"similar.timestamp, "+
  					"(target.timestamp + similar.timestamp)/2.0 "+
  				"from  "+
  					"(select user_id, venue_id, timestamp from %s) target "+
  			    "join "+
  					"(select user_id, venue_id, timestamp from %s) similar "+
  				"on target.venue_id = similar.venue_id and target.user_id != similar.user_id "+
  				//"on geoDistance(target.lat, target.lon, similiar.lat, similiar.lon) < 0.03 and target.user_id != similar.user_id "+
  				"where ABS(target.timestamp - similar.timestamp) < 3600); "; 
  						
    		query = String.format(txt, tableName,  checkInTable,  checkInTable);
			runNonQuery(query);

		} catch (SQLException e) {
			log("Error query is " + query);
			log("Error while running runEvaluation " + e.getMessage());
		}
    }


    private static void log(String str){
    	System.out.println(str);
    }
    
    private static void printFromQuery(String sql) throws SQLException {
        ResultSet rs = runQuery(sql);
        ResultSetMetaData metadata = rs.getMetaData();
        int col = metadata.getColumnCount();
        int row = 0;
        System.out.println("-------------------------------");
        for(int i=1; i<col; i++)
        {
            System.out.print(metadata.getColumnName(i)+"\t");
        }
        System.out.println("-------------------------------");
        while(rs.next())
        {
            for(int i=1; i<col; i++)
            {
                System.out.print(rs.getObject(i)+"\t");
            }
            System.out.println("");
            row++;
        }
        System.out.println("-------------------------------");
        System.out.println("Number of rows: "+row);        
    }

    private static void ShowProcessList() throws SQLException {
        ResultSet runQuery = runQuery("show processlist");
        while (runQuery.next()) {
            int pid = runQuery.getInt(1);
            String info = runQuery.getString("Info");
            System.out.println(pid + ":" + info);
        }
    }

    private static void Kill(int pid) throws SQLException {
        runNonQuery("kill " + pid);
    }


    private static String getConnectionString(String host, String port, String db, String user, String pass) {
        return String.format("jdbc:mysql://%s:%s/%s?user=%s&password=%s", host, port, db, user, pass);
    }

    private static void runNonQuery(String sql) throws SQLException {
    	DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    	log("[runNonQuery] Start at "+ dateFormat.format(new Date()) + " for query "+ sql);
        conn.setAutoCommit(false);
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        Statement stmt = conn.createStatement();
        System.out.println(sql);
        stmt.executeUpdate(sql);
        conn.commit();
        log("[runNonQuery] Finish at "+ dateFormat.format(new Date()));
    }

    private static ResultSet runQuery(String sql) throws SQLException {
    	DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    	log("[runQuery] Start at "+ dateFormat.format(new Date()) + " for query "+ sql);
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(sql);
        log("[runQuery] Finish at "+ dateFormat.format(new Date()));
        return rs;
    }
    
    

    /*
     *  Old code, for reference
     *  
    private static void combineCooccurrencesTime(int number) throws SQLException {
        int threshold = 3600;
        System.out.println("Start combining co-occurrences time into database : " + number);
        long time = System.nanoTime();
        String sql = String.format("insert into co_occur_time select * from co_occur_time_%d", number);
        runNonQuery(sql);
        time = System.nanoTime() - time;
        System.err.println("Total elapsed time: " + time / 1000000000 + " seconds" + " [" + new Date().toString() + "]");
    }

    private static void extractCooccurrences(int number) throws SQLException {
        int threshold = 3600;
        System.out.println("Start extracting co-occurrences into database : " + number);
        long time = System.nanoTime();
        String sql = String.format("insert into co_occurrences_temp select c.user_1, c.user_2, c.venue_id, %d as timediff, count(c.venue_id) as frequency from co_located_%d c inner join id_user_%d u on c.user_1 = u.id or c.user_2 = u.id group by c.user_1, c.user_2, c.venue_id", threshold, number, number);
        runNonQuery(sql);
        time = System.nanoTime() - time;
        System.err.println("Total elapsed time: " + time / 1000000000 + " seconds" + " [" + new Date().toString() + "]");
    }

    private static void coOccurrences(int number) throws SQLException {
        // Co-location
        int threshold = 3600;
        String select_distinct_user = String.format("select id from id_user_%d", number);
        System.out.println("Start insert co-occurrences into database : " + number);
        long time = System.nanoTime();
        String sql = String.format("insert into co_located_%d select target.user_id, similar.user_id, target.venue_id, ABS(target.unixtimestamps - similar.unixtimestamps) as timediff, 1 as frequency from (select target.user_id, target.venue_id, target.unixtimestamps from checkins target where target.user_id IN (%s)) target inner join (select similar.user_id, similar.venue_id, similar.unixtimestamps from checkins similar) similar on target.venue_id = similar.venue_id and target.user_id != similar.user_id where ABS(target.unixtimestamps - similar.unixtimestamps) < %d;", number, select_distinct_user, threshold);
        runNonQuery(sql);
        time = System.nanoTime() - time;
        System.err.println("Total elapsed time: " + time / 1000000000 + " seconds" + " [" + new Date().toString() + "]");
    }

    private static void coOccurrencesTimestamp(int number) throws SQLException {
        // Co-location
        int threshold = 3600;
        String select_distinct_user = String.format("select id from id_user_%d", number);
        System.out.println("Start insert co_occur_time_ into database : " + number);
        long time = System.nanoTime();
        String sql = String.format("insert into co_occur_time_%d select target.user_id, similar.user_id, target.venue_id, ABS(target.unixtimestamps - similar.unixtimestamps) as timediff, 1 as frequency, target.unixtimestamps as timestamp_u1, similar.unixtimestamps as timestamp_u2 from (select target.user_id, target.venue_id, target.unixtimestamps from checkins target where target.user_id IN (%s)) target inner join (select similar.user_id, similar.venue_id, similar.unixtimestamps from checkins similar) similar on target.venue_id = similar.venue_id and target.user_id != similar.user_id where ABS(target.unixtimestamps - similar.unixtimestamps) < %d;", number, select_distinct_user, threshold);
        runNonQuery(sql);
        time = System.nanoTime() - time;
        System.err.println("Total elapsed time: " + time / 1000000000 + " seconds" + " [" + new Date().toString() + "]");
    }

    private static void coOccurrencesGowalla(int number) throws SQLException {
        // Co-location
        int threshold = 3600;
        System.out.println("Start insert gowalla_co_occur into database : " + number);
        long time = System.nanoTime();
        for (int i = number; i < 100; i++) {
            String sql = String.format("insert into gowalla_co_occur_temp select target.user_id, similar.user_id, target.venue_id, ABS(target.timestamp - similar.timestamp) as timediff, 1 as frequency, target.timestamp as timestamp_u1, similar.timestamp as timestamp_u2, (target.timestamp + similar.timestamp)/2 as avg_checkin_time, 0 as duration, 0 as density, 0 as std_deviation from (select target.user_id, target.venue_id, target.timestamp from gowalla_checkin_%d target where target.user_id) target inner join (select similar.user_id, similar.venue_id, similar.timestamp from gowalla_checkin_%d similar where similar.user_id) similar on target.venue_id = similar.venue_id and target.user_id != similar.user_id where ABS(target.timestamp - similar.timestamp) < %d;", number, i, threshold);
//            runNonQuery(sql);
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            Statement stmt = conn.createStatement();
            System.out.println(sql);
            stmt.executeUpdate(sql);
            System.out.println("Finished iteration: " + i + " [" + new Date() + "]");
        }
        conn.commit();
        time = System.nanoTime() - time;
        System.err.println("Total elapsed time: " + time / 1000000000 + " seconds" + " [" + new Date().toString() + "]");
    }

    private static void insertFromFile(String filename, String table, char delim) throws SQLException {
        conn.setAutoCommit(false);
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        Statement stmt = conn.createStatement();
        String sql = String.format("LOAD DATA LOCAL INFILE \'%s\' IGNORE INTO TABLE %s FIELDS TERMINATED BY \'%c\';", filename, table, delim);
        System.out.println(sql);
        stmt.executeUpdate(sql);
        conn.commit();
    }
    
    private static void runNonQueryAuto(String sql) throws SQLException {
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        Statement stmt = conn.createStatement();
        System.out.println(sql);
        stmt.executeUpdate(sql);
        conn.commit();
    }

	*/
}