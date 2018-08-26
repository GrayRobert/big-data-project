package com.dbs.utils;

import com.dbs.Constants;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class ProfanityChecker {

    private List<String> profanity = new ArrayList<>();

    public ProfanityChecker() {

        //Get scanner instance
        Scanner scanner = null;
        try {
            scanner = new Scanner(new File(Constants.PROFANITY_CSV_FILE));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        //Set the delimiter used in file
        scanner.useDelimiter(",");

        //Get all tokens and store them in some data structure
        while (scanner.hasNext())
        {
            profanity.add(scanner.next().toUpperCase().trim());
        }

        //Do not forget to close the scanner
        scanner.close();
    }



    public boolean checkWord(String word) throws IOException {

        boolean isProfanity = false;

        if(this.profanity.contains(word.toUpperCase().trim())) {
            isProfanity = true;
        } else {
            isProfanity = false;
        }
        return isProfanity;
    }
}
