package org.jlab.clas.std.services.system;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.PumpStreamHandler;
import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.engine.Engine;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clas.std.services.util.Clas12Types;
import org.jlab.clas.std.services.util.ServiceUtils;
import org.jlab.clas12.tools.property.JPropertyList;

public class DataManager implements Engine {

    private static final String NAME = "DataManager";

    private static final String PROP_INPUT_PATH = "input_path";
    private static final String PROP_OUTPUT_PATH = "output_path";
    private static final String PROP_STAGE_PATH = "stage_path";

    private static final String PROP_ACTION = "action";
    private static final String PROP_FILENAME = "file";

    private static final String PROP_ACTION_STAGE = "stage_input";
    private static final String PROP_ACTION_REMOVE = "remove_input";
    private static final String PROP_ACTION_SAVE = "save_output";

    private static final String PROP_INPUT_FILE = "input_file";
    private static final String PROP_OUTPUT_FILE = "output_file";

    private final String baseDir;

    private volatile Path inputPath;
    private volatile Path outputPath;
    private volatile Path stagePath;
    private volatile String outputPrefix;

    public DataManager() {
        this(System.getenv("CLARA_HOME"));
    }

    public DataManager(String baseDir) {
        this.baseDir = baseDir;
        reset();
    }

    /**
     * Configuration data from an orchestrator should contain the following properties:
     * <ol>
     * <li> {@code input_path}: path to the location of the input-data files.</li>
     * <li> {@code output_path}: destination path of the output-data file.</li>
     * <li> {@code staging_path} (optional): data-file staging location,
     *      that is also used by the orchestrator to configure RW services.</li>
     * </ol>
     * @param input property-list containing the configuration properties
     */
    @Override
    public EngineData configure(EngineData input) {
        EngineData output = new EngineData();
        String mt = input.getMimeType();
        if (mt.equalsIgnoreCase(Clas12Types.PROPERTY_LIST.mimeType())) {
            JPropertyList pl = (JPropertyList) input.getData();
            if (pl.containsProperty(PROP_INPUT_PATH) && pl.containsProperty(PROP_OUTPUT_PATH)) {
                try {
                    updateConfiguration(pl);
                } catch (IllegalArgumentException e) {
                    String msg = String.format("%s config: invalid path: %s%n",
                                               NAME, ClaraUtil.reportException(e));
                    System.err.print(msg);
                    ServiceUtils.setError(output, msg);
                }
            } else {
                String msg = String.format("%s config: Missing properties: %s%n",
                                           NAME, pl.getStringRepresentation(true));
                System.err.print(msg);
                ServiceUtils.setError(output, msg);
            }
        } else {
            String msg = String.format("%s config: Wrong mimetype: %s%n", NAME, mt);
            System.err.print(msg);
            ServiceUtils.setError(output, msg);
        }
        return output;
    }

    private void updateConfiguration(JPropertyList pl) {
        try {
            inputPath = Paths.get(pl.getPropertyValue(PROP_INPUT_PATH));
            outputPath = Paths.get(pl.getPropertyValue(PROP_OUTPUT_PATH));
            if (inputPath.toString().isEmpty()) {
                throw new IllegalArgumentException("Empty input path");
            }
            if (outputPath.toString().isEmpty()) {
                throw new IllegalArgumentException("Empty input path");
            }
            if (pl.containsProperty(PROP_STAGE_PATH)) {
                stagePath = Paths.get(pl.getPropertyValue(PROP_STAGE_PATH));
                if (stagePath.toString().isEmpty()) {
                    throw new IllegalArgumentException("Empyt stage path");
                }
            }

            System.out.printf("%s service: input path set to %s%n", NAME, inputPath);
            System.out.printf("%s service: output path set to %s%n", NAME, outputPath);
            if (pl.containsProperty(PROP_STAGE_PATH)) {
                System.out.printf("%s service: Stage path set to %s%n", NAME, stagePath);
            }
        } catch (Exception e) {
            reset();
            throw e;
        }
    }

    public JPropertyList getConfiguration() {
        JPropertyList config = new JPropertyList();
        config.addTailProperty(PROP_INPUT_PATH, inputPath.toString());
        config.addTailProperty(PROP_OUTPUT_PATH, outputPath.toString());
        config.addTailProperty(PROP_STAGE_PATH, stagePath.toString());
        return config;
    }

    /**
     * Accepts a property list with an action and an input file name.
     *
     * Current version assumes that there is a CLAS12 convention
     * that reconstructed/output file name is constructed as:
     * {@code "out_" + input_file_name}
     * <ul>
     * <li>
     * If the <em>action</em> is {@code stage_input} the input file will be
     * copied to the staging directory. The full paths to the input and output files
     * in the staging directory will be returned, so the orchestrator can use them to
     * configure the reader and writer services.
     * <li>
     * If the <em>action</em> is {@code remove_input} the input file will be
     * removed from the staging directory.
     * <li>
     * If the <em>action</em> is {@code save_output} the output file will be
     * saved to the final location and removed from the staging directory.
     * </ul>
     *
     * The data can also be the string {@code get_config}, in which case a property list
     * with the configured paths will be returned.
     *
     * @param input property-list or string
     * @return paths, file names or error
     */
    @Override
    public EngineData execute(EngineData input) {
        EngineData output = new EngineData();
        String mt = input.getMimeType();
        if (mt.equalsIgnoreCase(Clas12Types.PROPERTY_LIST.mimeType())) {
            JPropertyList request = (JPropertyList) input.getData();
            if (request.containsProperty(PROP_ACTION) && request.containsProperty(PROP_FILENAME)) {
                runAction(request, output);
            } else {
                ServiceUtils.setError(output, "Missing properties: " + request);
            }
        } else if (mt.equalsIgnoreCase(EngineDataType.STRING.mimeType())) {
            String action = (String) input.getData();
            if (action.equals("get_config")) {
                output.setData(Clas12Types.PROPERTY_LIST.mimeType(), getConfiguration());
            } else {
                ServiceUtils.setError(output, "Wrong request: " + action);
            }
        } else {
            ServiceUtils.setError(output, "Wrong mimetype: " + mt);
        }
        return output;
    }

    private void runAction(JPropertyList request, EngineData output) {
        String action = request.getPropertyValue(PROP_ACTION);
        String inputFileName = request.getPropertyValue(PROP_FILENAME);
        FilePaths files = new FilePaths(inputFileName);

        Path resolvedFileName = files.inputFile.getFileName();
        if (resolvedFileName == null || !inputFileName.equals(resolvedFileName.toString())) {
            ServiceUtils.setError(output, "Invalid input file name: " + inputFileName);
            return;
        }

        if (action.equals(PROP_ACTION_STAGE)) {
            stageInputFile(files, output);
        } else if (action.equals(PROP_ACTION_REMOVE)) {
            removeStagedInputFile(files, output);
        } else if (action.equals(PROP_ACTION_SAVE)) {
            saveOutputFile(files, output);
        } else {
            ServiceUtils.setError(output, "Wrong " + PROP_ACTION + " value: " + action);
        }
    }

    private void stageInputFile(FilePaths files, EngineData output) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            CommandLine cmdLine = new CommandLine("cp");
            cmdLine.addArgument(files.inputFile.toString());
            cmdLine.addArgument(files.stagedInputFile.toString());

            DefaultExecutor executor = new DefaultExecutor();
            PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
            executor.setStreamHandler(streamHandler);

            executor.execute(cmdLine);
            System.out.printf("%s service: input file '%s' copied to '%s'%n",
                              NAME, files.inputFile, stagePath);

            JPropertyList fileNames = new JPropertyList();
            fileNames.addTailProperty(PROP_INPUT_FILE, files.stagedInputFile.toString());
            fileNames.addTailProperty(PROP_OUTPUT_FILE, files.stagedOutputFile.toString());
            output.setData(Clas12Types.PROPERTY_LIST.mimeType(), fileNames);

        } catch (ExecuteException e) {
            String msg = "Could not stage input file%n%n%s";
            ServiceUtils.setError(output, msg, outputStream.toString().trim());
        } catch (IOException e) {
            String msg = "Could not stage input file%n%n%s";
            ServiceUtils.setError(output, msg, ClaraUtil.reportException(e));
        }
    }

    private void removeStagedInputFile(FilePaths files, EngineData output) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            CommandLine cmdLine = new CommandLine("rm");
            cmdLine.addArgument(files.stagedInputFile.toString());

            DefaultExecutor executor = new DefaultExecutor();
            PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
            executor.setStreamHandler(streamHandler);

            executor.execute(cmdLine);
            System.out.printf("%s service: staged input file %s removed%n",
                              NAME, files.stagedInputFile);
            output.setData(EngineDataType.STRING.mimeType(), files.inputFile.toString());

        } catch (ExecuteException e) {
            String msg = "Could not remove staged input file%n%n%s";
            ServiceUtils.setError(output, msg, outputStream.toString().trim());
        } catch (Exception e) {
            String msg = "Could not remove staged input file%n%n%s";
            ServiceUtils.setError(output, msg, ClaraUtil.reportException(e));
        }
    }

    private void saveOutputFile(FilePaths files, EngineData output) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            CommandLine cmdLine = new CommandLine("mv");
            cmdLine.addArgument(files.stagedOutputFile.toString());
            cmdLine.addArgument(files.outputFile.toString());

            DefaultExecutor executor = new DefaultExecutor();
            PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
            executor.setStreamHandler(streamHandler);

            executor.execute(cmdLine);
            System.out.printf("%s service: output file '%s' saved to '%s'%n",
                              NAME, files.stagedOutputFile, outputPath);
            output.setData(EngineDataType.STRING.mimeType(), files.outputFile.toString());

        } catch (ExecuteException e) {
            String msg = "Could not save output file%n%n%s";
            ServiceUtils.setError(output, msg, outputStream.toString().trim());
        } catch (Exception e) {
            String msg = "Could not save output file%n%n%s";
            ServiceUtils.setError(output, msg, ClaraUtil.reportException(e));
        }
    }

    private class FilePaths {

        private Path stagedOutputFile;
        private Path outputFile;
        private Path stagedInputFile;
        private Path inputFile;

        FilePaths(String inputFileName) {
            inputFile = inputPath.resolve(inputFileName);
            stagedInputFile = stagePath.resolve(inputFileName);

            String outputFileName = outputPrefix + inputFileName;
            outputFile = outputPath.resolve(outputFileName);
            stagedOutputFile = stagePath.resolve(outputFileName);
        }
    }

    @Override
    public EngineData executeGroup(Set<EngineData> inputs) {
        return null;
    }

    @Override
    public Set<EngineDataType> getInputDataTypes() {
        return ClaraUtil.buildDataTypes(Clas12Types.PROPERTY_LIST, EngineDataType.STRING);
    }

    @Override
    public Set<EngineDataType> getOutputDataTypes() {
        return ClaraUtil.buildDataTypes(Clas12Types.PROPERTY_LIST, EngineDataType.STRING);
    }

    @Override
    public Set<String> getStates() {
        return null;
    }

    @Override
    public String getDescription() {
        return "Copy files from/to local disk.";
    }

    @Override
    public String getVersion() {
        return "0.9";
    }

    @Override
    public String getAuthor() {
        return "Sebastián Mancilla  <smancill@jlab.org>";
    }

    @Override
    public void reset() {
        inputPath = Paths.get(baseDir, "data", "in");
        outputPath = Paths.get(baseDir, "data", "out");
        stagePath = Paths.get("/scratch");
        outputPrefix = "out_";
    }

    @Override
    public void destroy() {
        // nothing
    }
}
