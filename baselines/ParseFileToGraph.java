import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;

public class ParseFileToGraph {
	private ArrayList<String> nodes;
	private HashMap<String, Integer> nodes_in_int;
	private HashMap <Integer, TreeSet<Integer>> neighbors;
	private ArrayList<Double> trust_scores;
	private ArrayList<Double> account_windows;
	private ArrayList<String> friends;
	private ArrayList<String> followers;
	private HashMap<String, Integer> statuses;
	private String server;

	public ParseFileToGraph(String server){
        nodes = new ArrayList<String>();
        nodes_in_int = new HashMap <String, Integer>();
        neighbors = new HashMap <Integer, TreeSet<Integer>>();
        trust_scores = new ArrayList<Double>();
        account_windows = new ArrayList<Double>();
        friends = new ArrayList<String>();
        followers = new ArrayList<String>();
        statuses = new HashMap<String, Integer>();
        this.server = server;

        set_tweet_data();
        //System.out.println("Vertices count "+nodes.size());
        //set_edges();
        //System.out.println("Edge count "+edge_count);
	}

	//need to change the edge function
	//get directly from the file
	public void add_edges(Integer i, String f){
		if (f.length() <= 0)
			return;

		String[] ids = f.split(", ");
		for (int j=0; j<ids.length; j++){
			if(nodes_in_int.containsKey(ids[j])){
				Integer a = nodes_in_int.get(ids[j]);
				if (!neighbors.get(i).contains(a))
				neighbors.get(i).add(a);
				if (!neighbors.get(a).contains(i))
				neighbors.get(a).add(i);
			}
			else{
				System.out.println("Does not contain the node: "+ids[j]);
			}
		}
	}

	public void set_edges(){
		for(int i=0; i < nodes.size(); i++){
			nodes_in_int.put(nodes.get(i), i);
			neighbors.put(i, new TreeSet<Integer>());
		}

		for(int i=0; i < followers.size(); i++){
//			add_edges(i,friends.get(i));
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
		try {
			FileReader fileReader = new FileReader("./fff/fff-"+server+".txt");
	  	    BufferedReader bufferedReader = new BufferedReader(fileReader);

	  	    String line = null;
	  	    int null_cnt = 0;
	  	    while((line = bufferedReader.readLine()) != null) {
	  	    	if (!line.contains("\t"))
	  	    		continue;
	  	    	String [] x = line.split("\t");
	  	    	nodes.add(x[0]);
	  	    	trust_scores.add(Double.parseDouble(x[1]));
	  	    	account_windows.add(Double.parseDouble(x[2]));
	  	    	statuses.put(x[0],Integer.parseInt(x[3]));
	  	    	if(x[4].equals("null")){
	  	    		x[4]="";
	  	    		null_cnt++;
	  	    	}
	  	    	followers.add(x[4]);
//	  	    	friends.add("");
  	      	}
	  	  bufferedReader.close();
	  	  System.out.println(null_cnt+": total nodes:"+nodes.size());

		}
		catch(FileNotFoundException ex) {
			System.out.println("Unable to open file ");
			ex.printStackTrace();
		}
		catch(IOException ex) {
			ex.printStackTrace();
			System.out.println("Error reading file ");
		}
    }

	public static void main(String []args){
		for(int i=0; i < 1; i++){
			ParseFileToGraph a = new ParseFileToGraph("arjun"+i);
			try{
				FileOutputStream fout = new FileOutputStream("./graphs/graph-arjun-"+i+".ser");
				ObjectOutputStream oos = new ObjectOutputStream(fout);
				oos.writeObject(a);
				oos.close();
			}catch(Exception ex){
				ex.printStackTrace();
			}

		}
	}

}
