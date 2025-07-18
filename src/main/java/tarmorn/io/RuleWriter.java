package src.main.java.tarmorn.io;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

import src.main.java.tarmorn.structure.Rule;

public class RuleWriter {
	
	public RuleWriter() {
		
	}
	
	public void write(Iterable<Rule> rules, String filepath) throws FileNotFoundException {
		
		int i = 0;
		PrintWriter pw = new PrintWriter(filepath);
		for (Rule rule : rules) {
			pw.println(rule);
			i++;
		}
		pw.flush();
		pw.close();
		System.out.println("* wrote " + i + " rules to " + filepath);
		
	}

}
