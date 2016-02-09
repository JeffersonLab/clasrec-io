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
import org.jlab.clara.engine.EngineStatus;
import org.jlab.clas.std.services.util.Clas12Types;
import org.jlab.clas12.tools.property.JPropertyList;
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
        EngineData config = createPropertiesRequest(pl -> {
            pl.addTailProperty("input_path", "/mnt/exp/in");
            pl.addTailProperty("output_path", "/mnt/exp/out");
            pl.addTailProperty("stage_path", "/tmp/files");
        });

        dm.configure(config);

        assertPaths("/mnt/exp/in", "/mnt/exp/out", "/tmp/files");
    }


    @Test
    public void configInputOutputPaths() throws Exception {
        EngineData config = createPropertiesRequest(pl -> {
            pl.addTailProperty("input_path", "/mnt/exp/in");
            pl.addTailProperty("output_path", "/mnt/exp/out");
        });

        dm.configure(config);

        assertPaths("/mnt/exp/in", "/mnt/exp/out", "/scratch");
    }


    private void assertPaths(String input, String output, String stage) {
        JPropertyList result = dm.getConfiguration();

        assertThat(result.getProperty("input_path").getValue(), is(input));
        assertThat(result.getProperty("output_path").getValue(), is(output));
        assertThat(result.getProperty("stage_path").getValue(), is(stage));
    }


    @Test
    public void configReturnsErrorOnInvalidInputPath() throws Exception {
        EngineData config = createPropertiesRequest(pl -> {
            pl.addTailProperty("input_path", "");
            pl.addTailProperty("output_path", "/mnt/exp/out");
        });

        assertErrorOnConfig(config, "invalid path");
    }


    @Test
    public void configReturnsErrorOnInvalidOutputPath() throws Exception {
        EngineData config = createPropertiesRequest(pl -> {
            pl.addTailProperty("input_path", "/mnt/exp/out");
            pl.addTailProperty("output_path", "");
        });

        assertErrorOnConfig(config, "invalid path");
    }


    @Test
    public void configReturnsErrorOnInvalidStagePath() throws Exception {
        EngineData config = createPropertiesRequest(pl -> {
            pl.addTailProperty("input_path", "/mnt/exp/in");
            pl.addTailProperty("output_path", "/mnt/exp/out");
            pl.addTailProperty("stage_path", "");
        });

        assertErrorOnConfig(config, "invalid path");
    }


    @Test
    public void configReturnsErrorOnMissingPath() throws Exception {
        EngineData config = createPropertiesRequest(pl -> {
            pl.addTailProperty("input_path", "/mnt/exp/in");
        });

        assertErrorOnConfig(config, "Missing properties");
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

        EngineData request = createPropertiesRequest(pl -> {
            pl.addTailProperty("action", "stage_input");
            pl.addTailProperty("file", paths.inputFile.getFileName().toString());
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

        EngineData request = createPropertiesRequest(pl -> {
            pl.addTailProperty("action", "remove_input");
            pl.addTailProperty("file", paths.inputFile.getFileName().toString());
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

        EngineData request = createPropertiesRequest(pl -> {
            pl.addTailProperty("action", "save_output");
            pl.addTailProperty("file", paths.inputFile.getFileName().toString());
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

        EngineData config = createPropertiesRequest(pl -> {
            pl.addTailProperty("input_path", paths.inputDir.toString());
            pl.addTailProperty("output_path", paths.outputDir.toString());
            pl.addTailProperty("stage_path", paths.stageDir.toString());
        });

        dm.configure(config);

        return paths;
    }


    @Test
    public void executeStageInputFailureReturnsError() throws Exception {
        EngineData request = createPropertiesRequest(pl -> {
            pl.addTailProperty("action", "stage_input");
            pl.addTailProperty("file", "file.ev");
        });

        assertErrorOnExecute(request, "Could not stage input");
    }


    @Test
    public void executeRemoveStagedInputFailureReturnsError() throws Exception {
        EngineData request = createPropertiesRequest(pl -> {
            pl.addTailProperty("action", "remove_input");
            pl.addTailProperty("file", "file.ev");
        });

        assertErrorOnExecute(request, "Could not remove staged input");
    }


    @Test
    public void executeSavesOutputFailureReturnsError() throws Exception {
        EngineData request = createPropertiesRequest(pl -> {
            pl.addTailProperty("action", "save_output");
            pl.addTailProperty("file", "file.ev");
        });

        assertErrorOnExecute(request, "Could not save output file");
    }


    @Test
    public void executeReturnsErrorOnMissingProperty() throws Exception {
        EngineData request = createPropertiesRequest(pl -> {
            pl.addTailProperty("command", "bad_action");
            pl.addTailProperty("file", "/mnt/exp/in/file.ev");
        });

        assertErrorOnExecute(request, "Missing properties");
    }


    @Test
    public void executeReturnsErrorOnWrongAction() throws Exception {
        EngineData request = createPropertiesRequest(pl -> {
            pl.addTailProperty("action", "bad_action");
            pl.addTailProperty("file", "file.ev");
        });

        assertErrorOnExecute(request, "Wrong action value: bad_action");
    }


    @Test
    public void executeReturnsErrorOnMissingMimeType() throws Exception {
        EngineData request = new EngineData();
        request.setData("text/number", 42);

        assertErrorOnExecute(request, "Wrong mimetype: text/number");
    }


    @Test
    public void executeReturnsErrorOnInputFileWithFullPath() throws Exception {
        EngineData request = createPropertiesRequest(pl -> {
            pl.addTailProperty("action", "stage_input");
            pl.addTailProperty("file", "/mnt/exp/in/file.ev");
        });

        assertErrorOnExecute(request, "Invalid input file name");
    }


    private EngineData createPropertiesRequest(Consumer<JPropertyList> builder) {
        JPropertyList pl = new JPropertyList();
        builder.accept(pl);
        EngineData request = new EngineData();
        request.setData(Clas12Types.PROPERTY_LIST.mimeType(), pl);
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
