package com.wesang.hermes.maintenance.extract;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class CustomerIdExtraction {
  Scanner scanner = new Scanner(System.in);

  public CustomerIdExtraction() {
    System.out.println("Enter file name >>> ");
    String fileName = scanner.nextLine();

    List<String> customerIds = readFile(fileName);

    if(customerIds.isEmpty()){
      System.out.println("No customer_id data found.");
      return;
    }

    String outputFileName = "customer_ids_output.csv";
    saveCustomerIdsToCSV(outputFileName, customerIds);

    scanner.close();
  }

  private List<String> readFile(String fileName) {
    List<String> customerIds = new ArrayList<>();
    try(BufferedReader reader = Files.newBufferedReader(Paths.get(fileName))) {
      String line;
      boolean headerSkipped = false;

      while((line = reader.readLine()) != null) {
        if(!headerSkipped) {
          headerSkipped = true;
          continue;
        }

        String[] parts = line.split(",");
        if(parts.length >= 2){
          customerIds.add(parts[0]);
        }
      }
    } catch (IOException e) {
      System.out.println("Error occurs during reading the file" + e.getMessage());
    }
    return customerIds;
  }

  private void saveCustomerIdsToCSV(String fileName, List<String> customerIds){
    try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileName), StandardCharsets.UTF_8)) {
      for(String customerId: customerIds){
        writer.write(customerId);
        writer.newLine();
      }
    }catch (IOException e){
      System.err.println("Error writing to the file: " + e.getMessage());
    }
  }

}
