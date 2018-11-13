import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.collections15.Transformer;

import edu.uci.ics.jung.algorithms.scoring.PageRankWithPriors;
import edu.uci.ics.jung.graph.DirectedSparseGraph;
import edu.uci.ics.jung.graph.util.Pair;
import ParseFileToGraph;

public class CreateGraph {
	private ArrayList<String> nodes;
	private ArrayList<Double> trust_scores;
	private ArrayList<Double> account_windows;
	private ArrayList<String> followers;
	public HashMap<String, Integer> statuses;

	private DirectedSparseGraph<String, Integer> g;
	private Transformer<String, Double> vertex_prior;
	private Transformer<Integer, Double> edge_weight;
	int type;
	private ArrayList<Double> priors;

	public CreateGraph(int type, String server){
		this.type = type;
		ParseFileToGraph a = new ParseFileToGraph(server);
		nodes = a.get_nodes();
		followers = a.get_followers();
		trust_scores = a.get_trust_scores();
		account_windows = a.get_account_windows();
		statuses = a.get_statuses();

		g = new DirectedSparseGraph<String, Integer>();

		for (int i=0; i < nodes.size(); i++){
			g.addVertex(nodes.get(i));
		}

		int edgeCnt=0;
		int cnt = 0;
		for (int i=0; i < followers.size(); i++){
			if(followers.get(i).length() == 0)
				continue;
			String[] ids = followers.get(i).split(", ");
			cnt += ids.length;
			for (String id:ids){
				g.addEdge(edgeCnt, id, nodes.get(i));
				edgeCnt++;
			}
		}
		// System.out.println("Type: "+type+" Edge count-"+g.getEdgeCount()+"-"+edgeCnt+"-"+cnt+": vertex count-"+g.getVertexCount());
		assign_prior();
		assign_edge_weight();
	}

	private double sum_double(ArrayList<Double> a){
		double total = 0.0;
		for(Double temp:a)
			total += temp;
		return total;
	}

	private void populate_prior(ArrayList<Double> a){
		priors = new ArrayList<Double>();
		double total_prior = sum_double(a);
		for(Double temp:a){
			priors.add(temp/total_prior);
		}
	}

	/*
	 * 0 --> Traditional
	 * 1 --> Activity Window Prior
	 * 2 --> Trust Prior
	 * 3 --> Trust Induced
	 * 4 --> Trust Induced Trust Prior
	 * 5 --> Sockpuppet Induced
	 * 6 --> Sockpuppet Induced Trust Prior
	 * */
	public void assign_prior(){
		if(type == 1){
			populate_prior(account_windows);
		}
		else if (type == 2 || type == 4 || type == 6){
			populate_prior(trust_scores);
		}
		else{
			ArrayList <Double> default_prior = new ArrayList<Double>();
			for(int i =0; i < nodes.size(); i++)
				default_prior.add(1.0);
			populate_prior(default_prior);
		}

		vertex_prior =
		    new Transformer<String, Double>()
		    {
		 @Override
		         public Double transform(String v)
		         {
			 		return priors.get(nodes.indexOf(v));
		         }
		    };
	}

	public Transformer<String, Double> get_prior()
	{
		return vertex_prior;
	}

	public void assign_edge_weight(){
		edge_weight =
		    new Transformer<Integer, Double>()
		    {
		 @Override
		         public Double transform(Integer edge)
		         {
			 		Pair <String > end_points = g.getEndpoints(edge);
			 		String from = end_points.getFirst();
//			 		String to = end_points.getSecond();
			 		double weight = 1.0/(g.outDegree(from));

			 		switch (type){
			 			case 0:
			 				break;
			 			case 1:
			 				break;
			 			case 2:
			 				break;
			 			case 3:
			 				if (trust_scores.get(nodes.indexOf(from)) > 0)
			 					weight *= trust_scores.get(nodes.indexOf(from));
			 				break;
			 			case 4:
			 				if (trust_scores.get(nodes.indexOf(from)) > 0)
			 					weight *= trust_scores.get(nodes.indexOf(from));
			 				break;
			 			case 5:
			 				if (trust_scores.get(nodes.indexOf(from)) > 0)
			 					weight *= trust_scores.get(nodes.indexOf(from));
			 				break;
			 			case 6:
			 				if (trust_scores.get(nodes.indexOf(from)) > 0)
			 					weight *= trust_scores.get(nodes.indexOf(from));
			 				break;
			 			default:
			 				break;
			 		}
			 		return weight;
		         }
		    };
	}

	public Transformer<Integer, Double> get_edge_weight()
	{
		return edge_weight;
	}

	public DirectedSparseGraph<String, Integer> get_graph(){
		return g;
	}

	public static void main(String[] args){
		long start=System.currentTimeMillis();
		int types = 5;

		String server = "arjun1";
		for (int i=0;i <types; i++){
			CreateGraph p = new CreateGraph(i, server);
			System.out.println("\n\nStarting "+server+" loop with i="+i);
			DirectedSparseGraph<String, Integer> g = p.get_graph();
			Transformer<String, Double> prior = p.get_prior();
			Transformer<Integer, Double> edge_weight = p.get_edge_weight();


			long runningTime = System.currentTimeMillis() - start;
			System.out.println(runningTime/1000);

			PageRankWithPriors<String, Integer> ranker = new PageRankWithPriors<String, Integer>(g,edge_weight,prior,0.15);
//			PageRank<String, Integer> ranker = new PageRank<String, Integer>(g,edge_weight,0.15);
			ranker.evaluate();
			runningTime = System.currentTimeMillis() - start;

			File file = new File("./output/"+server+"-pagerank-result-"+i+".txt");
			try {
				file.createNewFile();
				FileWriter writer = new FileWriter(file);

				for (String temp:g.getVertices()){
					writer.append(temp+"\t"+ranker.getVertexScore(temp)+"\t"+ p.statuses.get(temp)+"\n");
				}
				writer.flush();
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println(runningTime/1000);

		}
	}
}
