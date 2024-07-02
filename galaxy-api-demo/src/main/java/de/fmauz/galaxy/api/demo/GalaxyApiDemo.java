package de.fmauz.galaxy.api.demo;

import com.github.jmchilton.blend4j.galaxy.GalaxyInstance;
import com.github.jmchilton.blend4j.galaxy.GalaxyInstanceFactory;
import com.github.jmchilton.blend4j.galaxy.HistoriesClient;
import com.github.jmchilton.blend4j.galaxy.ToolDataClient;
import com.github.jmchilton.blend4j.galaxy.ToolsClient;
import com.github.jmchilton.blend4j.galaxy.beans.Dataset;
import com.github.jmchilton.blend4j.galaxy.beans.History;
import com.github.jmchilton.blend4j.galaxy.beans.OutputDataset;
import com.github.jmchilton.blend4j.galaxy.beans.Tool;
import com.github.jmchilton.blend4j.galaxy.beans.ToolExecution;
import com.github.jmchilton.blend4j.galaxy.beans.ToolInputs;
import com.github.jmchilton.blend4j.galaxy.beans.ToolSection;
import java.io.File;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;

/**
 *
 * @author fmauz
 */
public class GalaxyApiDemo {

    static GalaxyInstance galaxyInstance;
    static String galaxyURL = "https://test.galaxyproject.org/";
    static String apiKey = "cdcfdc5cbbce3273c1786fc8002b80f9";
    static String TOOL_NAME = "Sort";
    static History myHistory;

    public static void main(String[] args) throws Exception {
        galaxyInstance = GalaxyInstanceFactory.get(galaxyURL, apiKey);
        myHistory = getMyHistory();
        ToolDataClient dataClient
                = galaxyInstance.getToolDataClient();

        String datasetId = uploadDataSet(myHistory);
        Dataset dataset
                = waitUntilDatasetReady(datasetId);
        Tool histoGrammTool
                = getHistogrammTool();
        ToolInputs inputs = new ToolInputs(histoGrammTool.getId(), new HashMap<>());
        //inputs.getInputs().put("input1", "30");
        inputs.getInputs().put("input1", String.valueOf(dataset.getHid()));
        //inputs.getInputs().put("numerical_column", "1"); OutputDataset
        OutputDataset output = galaxyInstance.getToolsClient().create(myHistory,
                inputs).getOutputs().get(0);

        waitUntilDatasetReady(output.getId());

        String downloadHttp = galaxyURL + "api/datasets/" + output.getId()
                + "/display?to_ext=csv&key=" + apiKey;

        // String downloadHttp = galaxyURL + "api/datasets/4905136d3f9149da/display?preview=false&raw=false&key=" + apiKey;
        URL url = new URL(downloadHttp);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        InputStream responseStream = con.getInputStream();
        Files.copy(responseStream, Path.of("./response.csv"), StandardCopyOption.REPLACE_EXISTING);
    }

    public static History getMyHistory() {
        HistoriesClient historyClient = galaxyInstance.getHistoriesClient();

        History myHistory = null;
        for (History h : historyClient.getHistories()) {
            if (h.getName().equals("myHistory")) {
                myHistory = h;
            }
        }
        return myHistory;
    }

    public static Tool getHistogrammTool() {
        for (ToolSection t : galaxyInstance.getToolsClient().getTools()) {
            if (t.getName() == null) {
                continue;
            }
            for (Tool tool : t.getElems()) {
                if (tool.getName() == null) {
                    continue;
                }
                if (tool.getName().equals(TOOL_NAME)) {
                    return tool;
                }
            }
        }
        throw new RuntimeException("Tool was not found");
    }

    public static String uploadDataSet(History myHistory) {
        // create Galaxy Library
        File fileToUpload = new File("C:\\Users\\fmauz\\Downloads\\top80_german_cities.csv");
        ToolsClient.FileUploadRequest upload = new ToolsClient.FileUploadRequest(myHistory.getId(), fileToUpload);
        ToolExecution execution = galaxyInstance.getToolsClient().upload(upload);
        return execution.getOutputs().get(0).getId();
    }

    public static Dataset waitUntilDatasetReady(String datasetId) throws InterruptedException {
        Dataset dataset;
        do {
            dataset = galaxyInstance.getHistoriesClient().showDataset(myHistory.getId(), datasetId);
            System.out.println(dataset.getState());
            Thread.sleep(1000);
        } while (!dataset.getState().equals("ok"));
        return dataset;
    }
}
