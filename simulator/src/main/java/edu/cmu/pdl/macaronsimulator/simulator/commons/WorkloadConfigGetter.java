package edu.cmu.pdl.macaronsimulator.simulator.commons;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class WorkloadConfigGetter {
    /**
     * Gets the workload configuration from the file workloadConfig.yml.
     * 
     * @param filename the name of the file
     * @return the workload configuration
     */
    public static WorkloadConfig getWorkloadConfig(final String filename) {
        try {
            if (!Files.exists(Paths.get(filename)))
                throw new RuntimeException("File " + filename + " does not exist");
            Yaml yaml = new Yaml(new Constructor(WorkloadConfig.class));
            WorkloadConfig workloadConfig;
            workloadConfig = yaml.load(new FileInputStream(filename));
            if (!Files.exists(Paths.get(workloadConfig.getTraceFilename())))
                throw new RuntimeException("File " + workloadConfig.getTraceFilename() + " does not exist");
            return workloadConfig;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
