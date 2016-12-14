package org.jlab.clas.std.services.system;

import java.io.ByteArrayOutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
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
import org.jlab.clas.std.services.util.FileUtils;
import org.jlab.clas.std.services.util.ServiceUtils;
import org.json.JSONException;
import org.json.JSONObject;

public class DataManager implements Engine {

    private static final String NAME = "DataManager";

    private static final String CONF_INPUT_PATH = "input_path";
    private static final String CONF_OUTPUT_PATH = "output_path";
    private static final String CONF_STAGE_PATH = "stage_path";

    private static final String REQUEST_TYPE = "type";
    private static final String REQUEST_EXEC = "exec";
    private static final String REQUEST_QUERY = "query";

    private static final String REQUEST_ACTION = "action";
    private static final String REQUEST_FILENAME = "file";

    private static final String REQUEST_EXEC_STAGE = "stage_input";
    private static final String REQUEST_EXEC_REMOVE = "remove_input";
    private static final String REQUEST_EXEC_SAVE = "save_output";
    private static final String REQUEST_EXEC_CLEAR = "clear_stage";

    private static final String REQUEST_QUERY_CONFIG = "get_config";

    private static final String REQUEST_INPUT_FILE = "input_file";
    private static final String REQUEST_OUTPUT_FILE = "output_file";

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
     * Configuration data from an orchestrator should contain the following parameters:
     * <ol>
     * <li> {@code input_path}: path to the location of the input-data files.</li>
     * <li> {@code output_path}: destination path of the output-data file.</li>
     * <li> {@code staging_path} (optional): data-file staging location,
     *      that is also used by the orchestrator to configure RW services.</li>
     * </ol>
     * @param input JSON text containing the configuration parameters
     */
    @Override
    public EngineData configure(EngineData input) {
        EngineData output = new EngineData();
        String mt = input.getMimeType();
        if (mt.equalsIgnoreCase(EngineDataType.JSON.mimeType())) {
            String source = (String) input.getData();
            try {
                JSONObject data = new JSONObject(source);
                updateConfiguration(data);
                returnData(output, getConfiguration());
            } catch (IllegalArgumentException e) {
                String msg = String.format("%s config: invalid path: %s%n",
                                           NAME, ClaraUtil.reportException(e));
                System.err.print(msg);
                ServiceUtils.setError(output, msg);
            } catch (JSONException e) {
                String msg = String.format("%s config: invalid data: %s%n", NAME, source);
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

    private void updateConfiguration(JSONObject data) {
        try {
            inputPath = Paths.get(data.getString(CONF_INPUT_PATH));
            outputPath = Paths.get(data.getString(CONF_OUTPUT_PATH));
            if (inputPath.toString().isEmpty()) {
                throw new IllegalArgumentException("Empty input path");
            }
            if (outputPath.toString().isEmpty()) {
                throw new IllegalArgumentException("Empty input path");
            }
            if (data.has(CONF_STAGE_PATH)) {
                stagePath = Paths.get(data.getString(CONF_STAGE_PATH));
                if (stagePath.toString().isEmpty()) {
                    throw new IllegalArgumentException("Empyt stage path");
                }
            }

            System.out.printf("%s service: input path set to %s%n", NAME, inputPath);
            System.out.printf("%s service: output path set to %s%n", NAME, outputPath);
            if (data.has(CONF_STAGE_PATH)) {
                System.out.printf("%s service: stage path set to %s%n", NAME, stagePath);
            }
        } catch (Exception e) {
            reset();
            throw e;
        }
    }

    public JSONObject getConfiguration() {
        JSONObject config = new JSONObject();
        config.put(CONF_INPUT_PATH, inputPath.toString());
        config.put(CONF_OUTPUT_PATH, outputPath.toString());
        config.put(CONF_STAGE_PATH, stagePath.toString());
        return config;
    }

    /**
     * Accepts a JSON text with an action and an input file name.
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
     * The data can also be the string {@code get_config}, in which case a JSON text
     * with the configured paths will be returned.
     *
     * @param input JSON text
     * @return paths, file names or error
     */
    @Override
    public EngineData execute(EngineData input) {
        EngineData output = new EngineData();
        String mt = input.getMimeType();
        if (mt.equalsIgnoreCase(EngineDataType.JSON.mimeType())) {
            String source = (String) input.getData();
            try {
                JSONObject request = new JSONObject(source);
                String type = request.getString(REQUEST_TYPE);
                switch (type) {
                    case REQUEST_EXEC:
                        runAction(request, output);
                        break;
                    case REQUEST_QUERY:
                        runQuery(request, output);
                        break;
                    default:
                        ServiceUtils.setError(output, "Invalid %s value: %s", REQUEST_TYPE, type);
                }
            } catch (JSONException e) {
                ServiceUtils.setError(output, "Invalid request: " + source);
            }
        } else {
            ServiceUtils.setError(output, "Wrong mimetype: " + mt);
        }
        return output;
    }

    private void runAction(JSONObject request, EngineData output) {
        String action = request.getString(REQUEST_ACTION);
        String inputFileName = request.getString(REQUEST_FILENAME);

        FilePaths files = new FilePaths(inputFileName);
        Path resolvedFileName = files.inputFile.getFileName();
        if (resolvedFileName == null || !inputFileName.equals(resolvedFileName.toString())) {
            ServiceUtils.setError(output, "Invalid input file name: " + inputFileName);
            return;
        }

        switch (action) {
            case REQUEST_EXEC_STAGE:
                stageInputFile(files, output);
                break;
            case REQUEST_EXEC_REMOVE:
                removeStagedInputFile(files, output);
                break;
            case REQUEST_EXEC_SAVE:
                saveOutputFile(files, output);
                break;
            case REQUEST_EXEC_CLEAR:
                clearStageDir(files, output);
                break;
            default:
                ServiceUtils.setError(output, "Invalid %s value: %s", REQUEST_ACTION, action);
        }
    }

    private void runQuery(JSONObject request, EngineData output) {
        String action = request.getString(REQUEST_ACTION);
        switch (action) {
            case REQUEST_QUERY_CONFIG:
                returnData(output, getConfiguration());
                break;
            default:
                ServiceUtils.setError(output, "Invalid %s value: %s", REQUEST_ACTION, action);
        }
    }

    private void stageInputFile(FilePaths files, EngineData output) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            Files.createDirectories(stagePath);

            CommandLine cmdLine = new CommandLine("cp");
            cmdLine.addArgument(files.inputFile.toString());
            cmdLine.addArgument(files.stagedInputFile.toString());

            DefaultExecutor executor = new DefaultExecutor();
            PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
            executor.setStreamHandler(streamHandler);

            executor.execute(cmdLine);
            System.out.printf("%s service: input file '%s' copied to '%s'%n",
                              NAME, files.inputFile, stagePath);
            returnFilePaths(output, files);

        } catch (ExecuteException e) {
            String msg = "Could not stage input file%n%n%s";
            ServiceUtils.setError(output, msg, outputStream.toString().trim());
        } catch (Exception e) {
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
            returnFilePaths(output, files);

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
            Files.createDirectories(outputPath);

            CommandLine cmdLine = new CommandLine("mv");
            cmdLine.addArgument(files.stagedOutputFile.toString());
            cmdLine.addArgument(files.outputFile.toString());

            DefaultExecutor executor = new DefaultExecutor();
            PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
            executor.setStreamHandler(streamHandler);

            executor.execute(cmdLine);
            System.out.printf("%s service: output file '%s' saved to '%s'%n",
                              NAME, files.stagedOutputFile, outputPath);
            returnFilePaths(output, files);

        } catch (ExecuteException e) {
            String msg = "Could not save output file%n%n%s";
            ServiceUtils.setError(output, msg, outputStream.toString().trim());
        } catch (Exception e) {
            String msg = "Could not save output file%n%n%s";
            ServiceUtils.setError(output, msg, ClaraUtil.reportException(e));
        }
    }

    private void clearStageDir(FilePaths files, EngineData output) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            FileUtils.deleteFileTree(stagePath);
            System.out.printf("%s service: removed stage directory '%s'%n", NAME, stagePath);
            returnFilePaths(output, files);
        } catch (UncheckedIOException e) {
            String msg = "Could not remove stage directory%n%n%s";
            ServiceUtils.setError(output, msg, outputStream.toString().trim());
        } catch (Exception e) {
            String msg = "Could not remove stage directory%n%n%s";
            ServiceUtils.setError(output, msg, ClaraUtil.reportException(e));
        }
    }

    private void returnFilePaths(EngineData output, FilePaths files) {
        JSONObject fileNames = new JSONObject();
        fileNames.put(REQUEST_INPUT_FILE, files.stagedInputFile.toString());
        fileNames.put(REQUEST_OUTPUT_FILE, files.stagedOutputFile.toString());
        returnData(output, fileNames);
    }

    private void returnData(EngineData output, JSONObject data) {
        output.setData(EngineDataType.JSON.mimeType(), data.toString());
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
        return ClaraUtil.buildDataTypes(EngineDataType.JSON);
    }

    @Override
    public Set<EngineDataType> getOutputDataTypes() {
        return ClaraUtil.buildDataTypes(EngineDataType.JSON);
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
