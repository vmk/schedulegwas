package nl.vu.psy.gwas;

import java.io.File;
import java.util.ArrayList;

import joptsimple.OptionParser;
import joptsimple.OptionSpec;

public class ScheduleGwasOptionParser extends OptionParser {
	public OptionSpec<String> projectId;
	public OptionSpec<File> sampleFile;
	public OptionSpec<File> covarFile;
	public OptionSpec<String> program;
	@SuppressWarnings("rawtypes")
	public OptionSpec help;
	@SuppressWarnings("rawtypes")
	public OptionSpec retract;
	@SuppressWarnings("rawtypes")
	public OptionSpec report;
	@SuppressWarnings("rawtypes")
	public OptionSpec get;
	@SuppressWarnings("rawtypes")
	public OptionSpec plinkLog;
	
	public ScheduleGwasOptionParser() {
		super();

		// Option declaration
		ArrayList<String> projectIdOption = new ArrayList<String>();
		projectIdOption.add("id");
		projectIdOption.add("projectid");
		
		
		ArrayList<String> sampleFileOption = new ArrayList<String>();
		sampleFileOption.add("s");
		sampleFileOption.add("f");
		sampleFileOption.add("sample");
		sampleFileOption.add("fam");
		
		ArrayList<String> covarFileOption = new ArrayList<String>();
		covarFileOption.add("c");
		covarFileOption.add("covar");
		
		ArrayList<String> programOption = new ArrayList<String>();
		programOption.add("p");
		programOption.add("tool");
		
		ArrayList<String> helpCommand = new ArrayList<String>();
		helpCommand.add("h");
		helpCommand.add("?");
		helpCommand.add("help");
		
		ArrayList<String> plinklogOption = new ArrayList<String>();
		plinklogOption.add("log");
		plinklogOption.add("logistic");
		
		ArrayList<String> retractOption = new ArrayList<String>();
		retractOption.add("retract");
		retractOption.add("delete");
		
		ArrayList<String> reportOption = new ArrayList<String>();
		reportOption.add("report");
		reportOption.add("progress");
		
		ArrayList<String> getOption = new ArrayList<String>();
		getOption.add("retrieve");
		getOption.add("get");
		
		// accept rules:
		projectId = acceptsAll(projectIdOption, "Required. The project name. Used as id to store job information and output files.").withRequiredArg().ofType(String.class).describedAs("string");
		sampleFile = acceptsAll(sampleFileOption, "Required. The .sample or .fam file describing the pedigree and phenotype.").withRequiredArg().ofType(File.class).describedAs("file");
		covarFile = acceptsAll(covarFileOption, "Optional. The covariate file describing any covariates.").withRequiredArg().ofType(File.class).describedAs("file");
		program = acceptsAll(programOption, "Required. The program to use for the analyses. Available options: [ plink, snptest ].").withRequiredArg().ofType(String.class).describedAs("string");
		plinkLog = acceptsAll(plinklogOption, "Optional. Runs plink with the --logistic flag. Default is --linear");
		retract = acceptsAll(retractOption, "Optional. Retracts or deletes all jobs matching the project name.");
		report = acceptsAll(reportOption, "Optional. Displays a report on job progress. Requires a project name.");
		get = acceptsAll(getOption, "Optional. Gets all the curent output files to the current directory. Requires a project name.");
		help = acceptsAll(helpCommand, "Prints usage information.");
	}



}
