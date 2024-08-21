package edu.cmu.pdl.macaronsimulator.simulator.commons;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class OSPriceInfoGetter {

    public static String customOSCostInfoFilepath = null;

    /**
     * Gets the object storage cost information from the file objectStorageCostInfo.yml.
     * 
     * @return the object storage cost information
     */
    public static OSPriceInfo getOSPriceInfo() {
        try {
            Yaml yaml = new Yaml(new Constructor(OSPriceInfo.class));
            InputStream inputStream = customOSCostInfoFilepath == null ? getFileInputStream("objectStorageCostInfo.yml")
                    : new FileInputStream(customOSCostInfoFilepath);
            assert inputStream.available() > 0 : "inputStream is not available for objectStorageCostInfo.yml";
            OSPriceInfo osCostInfo = yaml.load(inputStream);
            inputStream.close();
            return osCostInfo;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets the input stream of the file with the given file name.
     * 
     * @param fileName the name of the file
     * @return the input stream of the file
     */
    private static InputStream getFileInputStream(final String fileName) {
        InputStream ioStream = OSPriceInfoGetter.class.getClassLoader().getResourceAsStream(fileName);
        if (ioStream == null)
            throw new IllegalArgumentException(fileName + " is not found");
        return ioStream;
    }
}
