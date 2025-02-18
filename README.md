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
   
---  

   Depending on which information needs to be imported, either or both of 4 and 5 can be done, but at least one is required. The CSV files created must
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

**6. Upload these files to the server and note their location.**

## Execution

import.php is run from the command line, and takes one or both of the --hs and --sv flags, and either of --use-vin or --use-spec. The --hs
flag specifies the location of the hardware/software CSV from step 3; --sv specifies the location the software/vehicle CSV
from step 4. --use-vin indicates that VINs are provided in the software/vehicle CSV, while --use-spec indicates that
specifications are provided instead. It does not matter which order these flags are specified.

```console
 ./import.php --hs /path/to/hardware_software.csv --sv /path/to/software_vehicle.csv --use-vin
```


   
