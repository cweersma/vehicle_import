# Vehicle/software import script

## Prerequisites
The git, php8, and php-composer packages must all be installed.

## Preparation steps

  *Note: this is all done on the command line.*
  
**1. Clone this repository**
   
   It doesn't matter where you clone this to. Just make sure your user has read/write permissions in the directory.
   ```console
   git clone https://github.com/cweersma/vehicle_import.git
   ```
**2. Install Velox Server**
   
   A composer.json file is included. You will only need to run the `composer install` command to install the Velox Server library.
   ```console
   cd vehicle_import
   composer install
   ```

**3. Adjust the configuration as necessary**

   This script uses credentials stored in `inc/connection.php` to connect to the approprate database. This file must be edited to provide the correct details.
   ```console
   nano inc/connection.php
   ```

   *Note: to avoid overwriting this configuration with the default template if the script must be updated from this GitHub repository at any point, run the following line
   to exclude it from the local repository:*
   ```console
   git update-index --skip-worktree inc/connection.php
   ```
---  

   Depending on which information needs to be imported, any combination of 4, 5, and/or 6 can be done, but at least one is required. The CSV files created must
   have a header row; the column names in this header row do not matter (the order of the columns will be followed irrespective of heading), but the import will
   always start from row 2.

---
   
**4. Prepare a hardware/software CSV.**

   This CSV must have two columns. The first column must contain hardware numbers, and the second should contain software numbers. Every row must contain both.

   | hardware | software |
   | -------- | -------- |
   | 123456   | 987654   |
   | 123456   | 321987   |
   | 789123   | 456789   |

**5. Prepare a software/vehicle CSV.**

   This CSV must have software numbers as its first column. Vehicle information can be provided in one of two ways: by VIN or by specification (year/make/model, etc.)

   *Important: all software numbers in this file must either exist in the database already or be included with the hardware/software CSV. Any that do not exist in either
   location will be ignored by the import script.*

   If providing VINs, the CSV should contain only software and VIN: 

   | software | VIN               |
   | -------- | ----------------- |
   | 987654   | 1G8ZH528X2Z310309 |
   | 321987   | 1GCCS1442W8181753 |
   | 456789   | JH4DA3350KS009715 |

   If providing specifications, all the following columns must exist in the following order (though some can be left empty):
   * (required): software number
   * (required): make name
   * (required): model name
   * (required): model year
   * (required): engine displacement (in liters)
   * (optional): engine type (gasoline, diesel, etc.) -- defaults to gasoline if not specified
   * (optional): trim (base, sport, etc.) -- defaults to null
   * (optional): series -- defaults to null
     
   | software | make      | model       | year | displacement | engine type | trim | series          |
   | -------- | --------- | ----------- | ---- | ------------ | ----------- | ---- | --------------- |
   | 987654   | Saturn    | SL1         | 2002 | 1.8          | gasoline    |      |                 |
   | 321987   | Chevrolet | S-10 Pickup | 1998 | 2.2          | gasoline    |      | 1/2 Ton Nominal |
   | 456789   | Acura     | Integra     | 1989 | 1.6          |             | LS   |                 |

**6. Create software/Hollander and/or hardware/Hollander CSVs.**

If either or both of these are provided, they must have two columns. The first column should be the hardware or software
number, and the second should be the Hollander number to be matched with that number. Duplicate values are allowed in
either column, but every row must have both a hardware/software number and a Hollander number.

| software/hardware | Hollander |
|-------------------|-----------|
| 987654            | 590-00001 |
| 987654            | 590-00002 |
| 123456            | 590-00002 |

The first column must have only values of the same type (software or hardware, not a mix of the two). As with the
software/vehicle CSV in step 5, these must already exist in the database or be included with the hardware/software CSV in
step 4, or these will be ignored by the matching.

*Note: the NIS database was not originally built with an inventory_hollander_map table. To be able to do hardware/Hollander
imports, this table must first be created if it doesn't exist. The query to create this table can be found in [this gist](https://gist.github.com/cweersma/4504d7dd8138760b9e0d28164e4c185c).

**7. Upload these files to the server and note their location.**

## Execution

import.php is run from the command line. The flags to be used are as follows:

CSV file flags:

| flag | description                      |
|------|----------------------------------|
| --hs | Hardware/software (from step 4)  |
| --sv | Software/vehicle (from step 5)   |
| --hh | Hardware/Hollander (from step 6) |
| --sh | Software/Hollander (from step 6) |

Any of the above may be omitted, but the script requires at least one of them. The values for each of these
flags must be the path to the file in question. If --sv is used, either --use-vin or --use-spec is also
required so that the script knows what type of vehicle information is in this file.

Option flags:

| flag        | description                                          |
|-------------|------------------------------------------------------|
| --use-vin   | The --sv file contains VINs.                         |
| --use-spec  | The --sv file contains vehicle specs.                |
| --verbose   | The script will output step-by-step status messages. |


Example usage:

```console
 ./import.php --hs /path/to/hardware_software.csv \
     --sv /path/to/software_vehicle.csv \
     --hh /path/to/hardware_hollander.csv \
     --sh /path/to/software_hollander.csv \
     --use-vin --verbose
```

   
