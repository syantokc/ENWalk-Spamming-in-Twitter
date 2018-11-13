import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;

public class ParseDataToGraph {
	private String hostname;
	private String port;
	private String database;
	private String user;
	private String password;
	private String baseUrl;

	private ArrayList<String> nodes;
	private HashMap<String, Integer> nodes_in_int;
	private HashMap <Integer, TreeSet<Integer>> neighbors;
	private ArrayList<Double> trust_scores;
	private ArrayList<Double> account_windows;
	private ArrayList<String> friends;
	private ArrayList<String> followers;
	private HashMap<String, Integer> statuses;
	private String server;

	public ParseDataToGraph(String _hostname, String _port, String _database, String _user, String _password, String server){
    	hostname = _hostname;
        port = _port;
        database = _database;
        user = _user;
        password = _password;
        baseUrl = "jdbc:mysql://" + hostname + ":" + port+ "/"+database;
        this.server = server;

        nodes = new ArrayList<String>();
        nodes_in_int = new HashMap <String, Integer>();
        neighbors = new HashMap <Integer, TreeSet<Integer>>();
        trust_scores = new ArrayList<Double>();
        account_windows = new ArrayList<Double>();
        friends = new ArrayList<String>();
        followers = new ArrayList<String>();
        statuses = new HashMap<String, Integer>();

        set_tweet_data();
        //System.out.println("Vertices count "+nodes.size());
        set_edges();
        //System.out.println("Edge count "+edge_count);
	}

	public void add_edges(Integer i, String f){
		if (f.length() <= 0)
			return;

		String[] ids = f.split(",");
		for (int j=0; j<ids.length; j++){
			if(nodes_in_int.containsKey(ids[j])){
				Integer a = nodes_in_int.get(ids[j]);
				if (!neighbors.get(i).contains(a))
				neighbors.get(i).add(a);
				if (!neighbors.get(a).contains(i))
				neighbors.get(a).add(i);
			}
		}
	}

	public void set_edges(){
		for(int i=0; i < nodes.size(); i++){
			nodes_in_int.put(nodes.get(i), i);
			neighbors.put(i, new TreeSet<Integer>());
		}

		for(int i=0; i < friends.size(); i++){
			add_edges(i,friends.get(i));
			add_edges(i,followers.get(i));
		}
	}

	public HashMap <Integer, TreeSet<Integer>> get_edges(){
		return neighbors;
	}

	public ArrayList<String> get_nodes(){
		return nodes;
	}

	public ArrayList<String> get_friends(){
		return friends;
	}

	public ArrayList<String> get_followers(){
		return followers;
	}

	public ArrayList<Double> get_trust_scores(){
		return trust_scores;
	}

	public ArrayList<Double> get_account_windows(){
		return account_windows;
	}

	public HashMap<String, Integer> get_statuses(){
		return statuses;
	}

	public void set_tweet_data() {
        Statement statement = null;
        ResultSet resultSet = null;
        Connection connection = getConnection();

        if(connection != null) {
            try {
               statement = connection.createStatement();
            } catch (SQLException e) {
               System.out.println("Unable to create statement");
            }


            if(statement != null) {
                try {
                	String query = "select * from ff_final_"+server;
                	//System.out.println(query);
                	resultSet = statement.executeQuery(query);

                } catch (SQLException e) {
                    System.out.println("Unable to execute statement-sdds");
                    e.printStackTrace();
                }
            }

            if(resultSet != null) {
            	//System.out.println("Finished sql. now creating arraylist");
                try {
                    while(resultSet.next()) {
						nodes.add(resultSet.getString(1));
						trust_scores.add(Double.parseDouble(resultSet.getString(2)));
						friends.add(resultSet.getString(3));
						followers.add(resultSet.getString(4));
						account_windows.add(Double.parseDouble(resultSet.getString(5)));
						statuses.put(resultSet.getString(1), Integer.parseInt(resultSet.getString(6)));
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            try {
                resultSet.close();
                statement.close();
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

	public Connection getConnection(){

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(baseUrl, user, password);
            //System.out.println("Database connected!");
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot connect the database!", e);
        }
        return connection;
    }

	public static void main (String [] args){
		ParseDataToGraph a = new ParseDataToGraph("127.0.0.1", "3307", "twitter_celebrities", "santosh", "default_p","arjun2");
		ArrayList<String> nodes = a.get_nodes();
		ArrayList<String> friends = a.get_friends();
		ArrayList<String> followers = a.get_followers();
		HashMap<String, Integer> statuses = a.get_statuses();

		File file = new File("./output/graph-analysis.txt");
		try {
			file.createNewFile();
			FileWriter writer = new FileWriter(file);

			for (int i=0; i < friends.size(); i++){
				String[] ids = friends.get(i).split(",");
				int spam_count = 0;
				int node_count= 0;
				for(String id:ids){
					if(nodes.contains(id)){
						node_count++;
						spam_count += statuses.get(id);
					}
				}

				String[] follower_ids = followers.get(i).split(",");
				int follower_spam_count = 0;
				int follower_node_count = 0;
				for(String id:follower_ids){
					if(nodes.contains(id)){
						follower_node_count++;
						follower_spam_count += statuses.get(id);
					}
				}
				writer.append(statuses.get(nodes.get(i))+"\t"+nodes.get(i)+"\t"+ids.length+"\t"+spam_count+"\t"+node_count+"\t"+follower_ids.length+"\t"+follower_spam_count+"\t"+follower_node_count+"\n");
			}

			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
