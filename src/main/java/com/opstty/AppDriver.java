package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");

            programDriver.addClass("districtlist", DistrictList.class,
                    "A map/reduce program that shows the districts of paris containing trees.");

            programDriver.addClass("specieslist", SpeciesList.class,
                    "A map/reduce program that shows the species of trees in paris.");

            programDriver.addClass("nbtreeskind", NbTreesKind.class,
                    "A map/reduce program that counts the number of trees by kind.");

            programDriver.addClass("maxheightkind", MaxHeightKind.class,
                    "A map/reduce program that shows the higher tree by kind");

            programDriver.addClass("sortheight", SortHeight.class,
                    "A map/reduce program that sorts the height of trees.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
