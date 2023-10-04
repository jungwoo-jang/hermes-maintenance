package com.wesang.hermes.maintenance.extract;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class SpecificCustomerExtraction {
  Scanner scanner = new Scanner(System.in);

  public SpecificCustomerExtraction() {
    System.out.println("Enter file name >>> ");
    String fileName = scanner.nextLine();
    System.out.println("Enter customer_id >>> ");
    String customerId = scanner.nextLine();

    List<String> customerIds = readFileAndExtract(fileName, customerId);

    if(customerIds.isEmpty()){
      System.out.println("No customer_id data found.");
      return;
    }
    scanner.close();
  }

  private List<String> readFileAndExtract(String fileName, String customerId) {
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

        if(parts[0].equalsIgnoreCase(customerId)){
          System.out.println(line);
          break;
        }

        if(parts.length >= 2){
          customerIds.add(parts[0]);
        }
      }
    } catch (IOException e) {
      System.out.println("Error occurs during reading the file" + e.getMessage());
    }
    return customerIds;
  }
}
