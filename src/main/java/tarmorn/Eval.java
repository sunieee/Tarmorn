package tarmorn;

import java.io.IOException;
import java.util.ArrayList;

import tarmorn.data.Triple;
import tarmorn.data.TripleSet;
import tarmorn.eval.HitsAtK;
import tarmorn.eval.ResultSet;
import tarmorn.structure.Rule;

public class Eval {
		
	 
	private static String CONFIG_FILE = "config-eval.properties";
	
	
	/**
	 * Read the top k from the ranking file for MRR computation. If less candidates are available, no error is thrown.
	 */
	public static int TOP_K = 100;
	
	
	
	/**
	 * Path to the file that contains the triple set used for learning the rules.
	 */
	public static String PATH_TRAINING = "";
	
	
	
	
	
	/**
	 * Path to the file that contains the triple set used for to test the rules.
	 */
	public static String PATH_TEST = "";
	
	/**
	 * Path to the file that contains the triple set used for validation purpose.
	 */
	public static String PATH_VALID = "";
	
	/**
	 * Path to the output file where the predictions are stored.
	 */
	public static String PATH_PREDICTIONS = "";
	
	
	
	public static void main(String[] args) throws IOException {
		
		
		
		if (args.length == 1) {
			CONFIG_FILE = args[0];
			System.out.println("reading params from file " + CONFIG_FILE);
		}
		
		Rule.applicationMode();
		Settings.load();
		Settings.REWRITE_REFLEXIV = false;
		
		TripleSet trainingSet =  new TripleSet(PATH_TRAINING);
		TripleSet validationSet = new TripleSet(PATH_VALID);
		TripleSet testSet = new TripleSet(PATH_TEST);
		
		
		String[] values = Apply.getMultiProcessing(PATH_PREDICTIONS);

		HitsAtK hitsAtK = new HitsAtK();
		hitsAtK.addFilterTripleSet(trainingSet);
		hitsAtK.addFilterTripleSet(validationSet);
		hitsAtK.addFilterTripleSet(testSet); 
		
		StringBuilder sb = new StringBuilder();
		if (values.length == 1) {
			ResultSet rs = new ResultSet(PATH_PREDICTIONS, true, TOP_K);
			computeScores(rs, testSet, hitsAtK);
			System.out.println(hitsAtK.getHitsAtK(0) + "   " + hitsAtK.getHitsAtK(2) + "   " + hitsAtK.getHitsAtK(9) + "   " + hitsAtK.getApproxMRR());
		}
		else {
			for (String value : values) {
				
				String rsPath = PATH_PREDICTIONS.replaceFirst("\\|.*\\|", "" + value); 
				ResultSet rs = new ResultSet(rsPath, true,  TOP_K);
				computeScores(rs, testSet, hitsAtK);
				System.out.println(hitsAtK.getHitsAtK(0) + "   " + hitsAtK.getHitsAtK(2) + "   " + hitsAtK.getHitsAtK(9) + "   " + hitsAtK.getApproxMRR());
				sb.append(value + "   " + hitsAtK.getHitsAtK(0) + "   " + hitsAtK.getHitsAtK(9) + "   " + hitsAtK.getApproxMRR() + "\n");
			}
			System.out.println("-----");
			System.out.println(sb);
		}

		
	
	}

	
	private static void computeScores(ResultSet rs, TripleSet gold, HitsAtK hitsAtK) {
		for (Triple t : gold.getTriples()) {		
			ArrayList<String> cand1 = rs.getHeadCandidates(t.toString());
			// String c1 = cand1.size() > 0 ? cand1.get(0) : "-";
			hitsAtK.evaluateHead(cand1, t);
			ArrayList<String> cand2 = rs.getTailCandidates(t.toString());
			// String c2 = cand2.size() > 0 ? cand2.get(0) : "-";
			hitsAtK.evaluateTail(cand2, t);
		}
	}
	
	

		
}
