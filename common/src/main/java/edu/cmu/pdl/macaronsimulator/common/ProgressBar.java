package edu.cmu.pdl.macaronsimulator.common;

import java.util.logging.Logger;

public class ProgressBar {
    public static void printProgressBar(final long total, final long progress) {
        if (total < 20L) {
            return;
        }
        long fivePercentage = total / 20L;
        if (progress % fivePercentage == 0) {
            int count = (int) (progress / fivePercentage);
            if (count >= 0 && count <= 20) {
                Logger.getGlobal()
                        .info("Progress: [" + "*".repeat(count) + "-".repeat(20 - count) + "] (" + 5 * count + "%)");
            }
        }
    }
}
