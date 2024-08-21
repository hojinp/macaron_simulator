package edu.cmu.pdl.macaronsimulator.simulator.commons;

import java.io.IOException;
import java.io.InputStream;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class MachineInfoGetter {
    /**
     * Gets the machine information from the file machineInfo.yml.
     * 
     * @param machineType the type of the machine
     * @return the machine information
     */
    public static MachineInfo getMachineInfo(final String machineType) {
        try {
            Yaml yaml = new Yaml(new Constructor(MachineInfos.class));
            InputStream inputStream = getFileInputStream("machineInfo.yml");
            assert inputStream.available() > 0 : "inputStream is not available for machineInfo.yml";
            MachineInfos machineInfos = yaml.load(inputStream);
            inputStream.close();
            for (final MachineInfo machineInfo : machineInfos.getMachines()) {
                if (machineInfo.getName().equals(machineType)) {
                    return machineInfo;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException(machineType + " does not exist in the file machineInfo.yml");
    }

    /**
     * Gets the input stream of the file with the given file name.
     * 
     * @param fileName the name of the file
     * @return the input stream of the file
     */
    private static InputStream getFileInputStream(final String fileName) {
        InputStream ioStream = MachineInfos.class.getClassLoader().getResourceAsStream(fileName);
        if (ioStream == null)
            throw new IllegalArgumentException(fileName + " is not found");
        return ioStream;
    }
}
