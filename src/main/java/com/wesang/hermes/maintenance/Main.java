package com.wesang.hermes.maintenance;

import com.wesang.hermes.maintenance.extract.CustomerIdExtraction;
import com.wesang.hermes.maintenance.extract.SpecificCustomerExtraction;

import java.util.Scanner;

public class Main {
  public static void main(String[] args) {
    Scanner sc = new Scanner(System.in);

    System.out.println("Enter number >>>");
    int feature = sc.nextInt();
    switch (feature) {
      case 1 -> new CustomerIdExtraction();
      case 2 -> new SpecificCustomerExtraction();
    }
    sc.close();
  }
}