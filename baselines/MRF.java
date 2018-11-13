import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;
import org.apache.commons.math3.distribution.NormalDistribution;

public class MRF {

	private ArrayList<String> nodes;
	private HashMap <Integer, TreeSet<Integer>> neighbors;
	private ArrayList<Double> trust_scores;
	private ArrayList<Double> account_windows;
	public HashMap<String, Integer> statuses;
	private int mrf_type;

	private double [][] beliefs;
	private double [][] priors;
	private double [][] observed_states;
	private double [][] propagation_matrix;
	private ArrayList<ArrayList<Double[]>> messages;

	public MRF(int _mrf_type){
//		ParseDataToGraph a = new ParseDataToGraph("server url", "port number", "table_name", "username", "password");
		ParseFileToGraph a = new ParseFileToGraph("arjun3");
		neighbors = a.get_edges();
		nodes = a.get_nodes();
		trust_scores = a.get_trust_scores();
		account_windows = a.get_account_windows();
		statuses = a.get_statuses();
		mrf_type = _mrf_type;
		beliefs = new double [nodes.size()][3];
		priors = new double [nodes.size()][3];
		observed_states = new double [nodes.size()][3];
		propagation_matrix = new double [][]{{0.8,0.15,0.05},{0.4,0.5,0.1},{0.025,0.125,0.85}};
		set_priors();
		set_observed_states();
		initialize_messages();
	}

	private void set_priors(){
		if (mrf_type == 1){
			for (int i=0; i<nodes.size(); i++)
				for (int j=0; j<3; j++)
					priors[i][j] = 1.0/3;
		}
		else{
			for (int i=0; i<nodes.size(); i++){
				double p = trust_scores.get(i);
				//*
				priors[i][0] = (2-p)/5;
				priors[i][1] = 1.0/5;
				priors[i][2] = p/5;
				//*/
				/*
				priors[i][0] = 1-p;
				priors[i][1] = 0;
				priors[i][2] = p;
				*/
			}
		}
	}

	private void set_observed_states(){
		NormalDistribution a;
		double b1 = 1.0/3;
		double b2 = 2.0/3;
		double b3 = 1.0;

		ArrayList<Double> induced_w = trust_scores;
		if (mrf_type == 3)
			induced_w = account_windows;

		for (int i=0; i<nodes.size(); i++){
			double p = induced_w.get(i);
			a = new NormalDistribution(1-p,0.25);
			observed_states[i][0] = a.cumulativeProbability(b1);
			observed_states[i][1] = a.cumulativeProbability(b2)-observed_states[i][0];
			observed_states[i][2] = a.cumulativeProbability(b3)-observed_states[i][1]-observed_states[i][0];
		}
	}

	private void initialize_messages(){
		messages = new ArrayList<ArrayList<Double[]>>();
		for (int i=0; i < nodes.size();i++){
			TreeSet<Integer> neighbor = neighbors.get(i);
			ArrayList<Double[]> msg = new ArrayList<Double[]>();
			for (int j =0; j <neighbor.size(); j++){
				Double [] b = new Double[]{1.0,1.0,1.0};
				msg.add(b);
			}
			messages.add(msg);
		}
	}

	private Double[]  incoming_message_product_only(TreeSet<Integer> neighbor, int incoming){
		Double [] prods = new Double[]{1.0,1.0,1.0};
		ArrayList <Double[]> msg_to_incoming_temp = new ArrayList<Double[]>();
		java.util.Iterator<Integer> il = neighbor.iterator();
		while (il.hasNext()){
			int k = il.next();
			ArrayList<Double[]> msg_of_k = messages.get(k);
			ArrayList<Integer> neighbors_of_k = new ArrayList<Integer>(neighbors.get(k));
			Double [] msg_of_k_to_incoming = msg_of_k.get(neighbors_of_k.indexOf(incoming));

			msg_to_incoming_temp.add(msg_of_k_to_incoming);
			for (int m=0; m < prods.length; m++)
				prods[m] *= msg_of_k_to_incoming[m];
		}
		return prods;
	}

	private ArrayList <Double[]>  incoming_message_product(TreeSet<Integer> neighbor, int incoming){
		Double [] prods = new Double[]{1.0,1.0,1.0};
		ArrayList <Double[]> msg_to_incoming = new ArrayList<Double[]>();
		ArrayList <Double[]> msg_to_incoming_temp = new ArrayList<Double[]>();
		java.util.Iterator<Integer> il = neighbor.iterator();
		while (il.hasNext()){
			int k = il.next();
			ArrayList<Double[]> msg_of_k = messages.get(k);
			ArrayList<Integer> neighbors_of_k = new ArrayList<Integer>(neighbors.get(k));
			Double [] msg_of_k_to_incoming = msg_of_k.get(neighbors_of_k.indexOf(incoming));

			msg_to_incoming_temp.add(msg_of_k_to_incoming);
			for (int m=0; m < prods.length; m++)
				prods[m] *= msg_of_k_to_incoming[m];
		}

		for (int i=0; i < msg_to_incoming_temp.size(); i++){
			Double [] a = new Double[]{1.0,1.0,1.0};
			for (int j=0; j < msg_to_incoming_temp.size(); j++){
				if (i != j){
					for (int m=0; m < prods.length; m++){
						a[m] = msg_to_incoming_temp.get(j)[m];
						if (a[m] == 0){
							System.out.println("Error:exit");
							System.exit(0);
						}
					}
				}
			}
			msg_to_incoming.add(a);
		}
		/*
		for (Double [] a:msg_to_incoming_temp){
			for (int m=0; m < prods.length; m++){
				Double t = a[0];
				a[m] = prods[m]/a[m];
			}
			msg_to_incoming.add(a);
		}
		*/


		return msg_to_incoming;
	}

	private Double calculate_error(ArrayList<Double[]> old_msgs, ArrayList<Double[]> new_msgs){
		Double error = 0.0;
		for (int i=0; i< old_msgs.size(); i++){
			Double[] old_msg = old_msgs.get(i);
			Double[] new_msg = new_msgs.get(i);
			for(int j=0; j< old_msg.length; j++){
				error += Math.abs(old_msg[j]-new_msg[j]);
			}
		}
		return error;
	}

	public double pass_messages(){
		double error = 0.0;
		for (int i=0; i < nodes.size();i++){
			TreeSet<Integer> neighbor = neighbors.get(i);
			ArrayList<Double[]> old_msg = messages.get(i);
			ArrayList<Double[]> new_msg = new ArrayList<Double[]>();

			ArrayList <Double[]> msg_to_incoming = incoming_message_product(neighbor, i);
			for (int j=0; j < neighbor.size(); j++){
				Double [] m = new Double[3];
				for (int k =0; k< m.length; k++){
					m[k] = 0.0;
					for (int l =0; l< m.length; l++){
						//m[k] += propagation_matrix[k][l]*msg_to_incoming.get(j)[k];
						m[k] += propagation_matrix[k][l]*priors[i][k]*msg_to_incoming.get(j)[k];
					}
				}
				new_msg.add(m);
			}
			error += calculate_error(old_msg, new_msg);
			messages.set(i, new_msg);
		}
		return error;
	}

	public void run(){
		double error = pass_messages();
		double threshold = 1.0/10000000000.0;
		while (error/nodes.size() >= threshold){
			//System.out.println("Difference in consequetive pass: "+error/nodes.size());
			error = pass_messages();
		}
		calculate_belief();
		//System.out.println("Final-"+error/nodes.size());
	}

	public void calculate_belief(){
		for (int i=0; i < nodes.size();i++){
			TreeSet<Integer> neighbor = neighbors.get(i);
			Double[] prods = incoming_message_product_only(neighbor, i);
			Double sum = 0.0;
			for (int m=0; m < prods.length; m++){
				prods[m] *= observed_states[i][m];
				sum += prods[m];
			}
			for (int m=0; m < prods.length; m++){
				beliefs[i][m] = prods[m]/sum;
			}
		}
	}

	public void print_output(){
		PrintWriter writer=null;
		try {
			writer = new PrintWriter("./output/mrf-"+mrf_type+".txt", "UTF-8");
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for(int i=0; i < nodes.size(); i++){
			//System.out.println(i+"\t"+nodes.get(i)+"\t"+beliefs[i][0]+"\t"+beliefs[i][1]+"\t"+beliefs[i][2]);
			writer.println(i+"\t"+nodes.get(i)+"\t"+beliefs[i][0]+"\t"+beliefs[i][1]+"\t"+beliefs[i][2]+"\t"+statuses.get(nodes.get(i)));
		}
		writer.close();
		//System.out.println("Total nodes:"+nodes.size());
	}

	public static void main(String[] args){
		MRF a = new MRF(3);
		a.run();
		a.print_output();
	}
}
