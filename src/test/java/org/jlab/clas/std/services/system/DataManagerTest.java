package org.jlab.clas.std.services.system;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Consumer;

import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.engine.EngineStatus;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

public class DataManagerTest {

    private final String testFileName = "collider.txt";
    private final String testFilePath = getClass().getResource("/collider.txt").getPath();

    private DataManager dm;

    @Before
    public void setUp() {
        dm = new DataManager();
    }


    @Test
    public void setDefaultPaths() throws Exception {
        dm = new DataManager("/clara/");

        assertPaths("/clara/data/in", "/clara/data/out", "/scratch");
    }


    @Test
    public void setAllPaths() throws Exception {
        EngineData config = createJsonRequest(data -> {
            data.put("input_path", "/mnt/exp/in");
            data.put("output_path", "/mnt/exp/out");
            data.put("stage_path", "/tmp/files");
        });

        dm.configure(config);

        assertPaths("/mnt/exp/in", "/mnt/exp/out", "/tmp/files");
    }


    @Test
    public void configInputOutputPaths() throws Exception {
        EngineData config = createJsonRequest(data -> {
            data.put("input_path", "/mnt/exp/in");
            data.put("output_path", "/mnt/exp/out");
        });

        dm.configure(config);

        assertPaths("/mnt/exp/in", "/mnt/exp/out", "/scratch");
    }


    private void assertPaths(String input, String output, String stage) {
        JSONObject result = dm.getConfiguration();

        assertThat(result.getString("input_path"), is(input));
        assertThat(result.getString("output_path"), is(output));
        assertThat(result.getString("stage_path"), is(stage));
    }


    @Test
    public void configReturnsErrorOnInvalidInputPath() throws Exception {
        EngineData config = createJsonRequest(data -> {
            data.put("input_path", "");
            data.put("output_path", "/mnt/exp/out");
        });

        assertErrorOnConfig(config, "invalid path");
    }


    @Test
    public void configReturnsErrorOnInvalidOutputPath() throws Exception {
        EngineData config = createJsonRequest(data -> {
            data.put("input_path", "/mnt/exp/out");
            data.put("output_path", "");
        });

        assertErrorOnConfig(config, "invalid path");
    }


    @Test
    public void configReturnsErrorOnInvalidStagePath() throws Exception {
        EngineData config = createJsonRequest(data -> {
            data.put("input_path", "/mnt/exp/in");
            data.put("output_path", "/mnt/exp/out");
            data.put("stage_path", "");
        });

        assertErrorOnConfig(config, "invalid path");
    }


    @Test
    public void configReturnsErrorOnMissingPath() throws Exception {
        EngineData config = createJsonRequest(data -> {
            data.put("input_path", "/mnt/exp/in");
        });

        assertErrorOnConfig(config, "invalid data");
    }


    @Test
    public void configReturnsErrorOnMissingMimeType() throws Exception {
        EngineData config = new EngineData();
        config.setData("text/string", "bad config");

        assertErrorOnConfig(config, "Wrong mimetype: text/string");
    }


    @Test
    public void executeStagesInputFile() throws Exception {
        TestPaths paths = setTestDirectories();

        EngineData request = createJsonRequest(data -> {
            data.put("type", "exec");
            data.put("action", "stage_input");
            data.put("file", paths.inputFile.getFileName().toString());
        });

        EngineData result = dm.execute(request);

        assertThat("Result is not an error", result.getStatus(), is(not(EngineStatus.ERROR)));
        assertThat("Staged input exists", paths.stagedInputFile.toFile().exists(), is(true));
    }


    @Test
    public void executeRemovesStagedInputFile() throws Exception {
        TestPaths paths = setTestDirectories();
        Files.copy(paths.inputFile, paths.stagedInputFile);
        assertThat("Staged input exists", paths.stagedInputFile.toFile().exists(), is(true));

        EngineData request = createJsonRequest(data -> {
            data.put("type", "exec");
            data.put("action", "remove_input");
            data.put("file", paths.inputFile.getFileName().toString());
        });

        EngineData result = dm.execute(request);

        assertThat("Result is not an error", result.getStatus(), is(not(EngineStatus.ERROR)));
        assertThat("Staged input exists", paths.stagedInputFile.toFile().exists(), is(false));
    }


    @Test
    public void executeSavesOutputFile() throws Exception {
        TestPaths paths = setTestDirectories();
        Files.copy(paths.inputFile, paths.stagedOutputFile);

        assertThat("Staged output exists", paths.stagedOutputFile.toFile().exists(), is(true));
        assertThat("Saved output exists", paths.outputFile.toFile().exists(), is(false));

        EngineData request = createJsonRequest(data -> {
            data.put("type", "exec");
            data.put("action", "save_output");
            data.put("file", paths.inputFile.getFileName().toString());
        });

        EngineData result = dm.execute(request);

        assertThat("Result is not an error", result.getStatus(), is(not(EngineStatus.ERROR)));
        assertThat("Staged output exists", paths.stagedOutputFile.toFile().exists(), is(false));
        assertThat("Saved output exists", paths.outputFile.toFile().exists(), is(true));
    }


    private static class TestPaths {
        private Path inputDir;
        private Path outputDir;
        private Path stageDir;

        private Path inputFile;
        private Path outputFile;
        private Path stagedInputFile;
        private Path stagedOutputFile;
    }


    private TestPaths setTestDirectories() throws Exception {
        TestPaths paths = new TestPaths();

        paths.inputDir = Paths.get(testFilePath).getParent();
        paths.inputFile = Paths.get(testFilePath);

        paths.outputDir = Files.createTempDirectory("output");
        paths.outputDir.toFile().deleteOnExit();

        paths.outputFile = paths.outputDir.resolve("out_" + testFileName);
        paths.outputFile.toFile().deleteOnExit();

        paths.stageDir = Files.createTempDirectory("stage");
        paths.stageDir.toFile().deleteOnExit();

        paths.stagedInputFile = paths.stageDir.resolve(testFileName);
        paths.stagedOutputFile = paths.stageDir.resolve("out_" + testFileName);

        paths.stagedInputFile.toFile().deleteOnExit();
        paths.stagedOutputFile.toFile().deleteOnExit();

        EngineData config = createJsonRequest(data -> {
            data.put("input_path", paths.inputDir.toString());
            data.put("output_path", paths.outputDir.toString());
            data.put("stage_path", paths.stageDir.toString());
        });

        dm.configure(config);

        return paths;
    }


    @Test
    public void executeStageInputFailureReturnsError() throws Exception {
        EngineData request = createJsonRequest(data -> {
            data.put("type", "exec");
            data.put("action", "stage_input");
            data.put("file", "file.ev");
        });

        assertErrorOnExecute(request, "Could not stage input");
    }


    @Test
    public void executeRemoveStagedInputFailureReturnsError() throws Exception {
        EngineData request = createJsonRequest(data -> {
            data.put("type", "exec");
            data.put("action", "remove_input");
            data.put("file", "file.ev");
        });

        assertErrorOnExecute(request, "Could not remove staged input");
    }


    @Test
    public void executeSavesOutputFailureReturnsError() throws Exception {
        EngineData request = createJsonRequest(data -> {
            data.put("type", "exec");
            data.put("action", "save_output");
            data.put("file", "file.ev");
        });

        assertErrorOnExecute(request, "Could not save output file");
    }


    @Test
    public void executeReturnsErrorOnMissingProperty() throws Exception {
        EngineData request = createJsonRequest(data -> {
            data.put("command", "bad_action");
            data.put("file", "/mnt/exp/in/file.ev");
        });

        assertErrorOnExecute(request, "Invalid request");
    }


    @Test
    public void executeReturnsErrorOnWrongAction() throws Exception {
        EngineData request = createJsonRequest(data -> {
            data.put("type", "exec");
            data.put("action", "bad_action");
            data.put("file", "file.ev");
        });

        assertErrorOnExecute(request, "Invalid action value: bad_action");
    }


    @Test
    public void executeReturnsErrorOnMissingMimeType() throws Exception {
        EngineData request = new EngineData();
        request.setData("text/number", 42);

        assertErrorOnExecute(request, "Wrong mimetype: text/number");
    }


    @Test
    public void executeReturnsErrorOnInputFileWithFullPath() throws Exception {
        EngineData request = createJsonRequest(data -> {
            data.put("type", "exec");
            data.put("action", "stage_input");
            data.put("file", "/mnt/exp/in/file.ev");
        });

        assertErrorOnExecute(request, "Invalid input file name");
    }


    private EngineData createJsonRequest(Consumer<JSONObject> builder) {
        JSONObject data = new JSONObject();
        builder.accept(data);
        EngineData request = new EngineData();
        request.setData(EngineDataType.JSON.mimeType(), data.toString());
        return request;
    }


    private void assertErrorOnConfig(EngineData config, String msg) {
        EngineData result = dm.configure(config);

        assertThat("Result is an error", result.getStatus(), is(EngineStatus.ERROR));
        assertThat("Description matches", result.getDescription(), containsString(msg));
    }


    private void assertErrorOnExecute(EngineData request, String msg) {
        EngineData result = dm.execute(request);

        assertThat("Result is an error", result.getStatus(), is(EngineStatus.ERROR));
        assertThat("Description matches", result.getDescription(), containsString(msg));
    }
}
