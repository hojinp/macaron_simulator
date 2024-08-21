package edu.cmu.pdl.macaronsimulator.common;

import java.util.logging.Logger;

public class PrintMacaron {
    public static void printMacaron() {
        Logger logger = Logger.getGlobal();
        logger.info("\n" +
        " __  __    _    ____    _    ____   ___  _   _\n" +
        "|  \\/  |  / \\  / ___|  / \\  |  _ \\ / _ \\| \\ | |\n" +
        "| |\\/| | / _ \\| |     / _ \\ | |_) | | | |  \\| |\n" +
        "| |  | |/ ___ \\ |___ / ___ \\|  _ <| |_| | |\\  |\n" +
        "|_|  |_/_/   \\_\\____/_/   \\_\\_| \\_\\\\___/|_| \\_|\n\n");
    }
}
